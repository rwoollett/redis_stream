#include "Producer.h"
#include <workqstream/common/Common.h>
#include <mtlog/mt_log.hpp>
#include <sstream>
#include <iomanip>

#ifdef HAVE_ASIO

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/redis/request.hpp>
#include <boost/redis/response.hpp>

using namespace boost::asio::experimental::awaitable_operators;

namespace WorkQStream
{
  namespace
  {
    const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");
    const char *REDIS_GROUP_CONFIG = std::getenv("REDIS_GROUP_CONFIG");
    const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
    const char *REDIS_HOST = std::getenv("REDIS_HOST");
    const char *REDIS_PORT = std::getenv("REDIS_PORT");
    const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");

    const int CONNECTION_RETRY_AMOUNT = -1;
    const int CONNECTION_RETRY_DELAY = 3;
    const int PRODUCE_TIMEOUT_DELAY = 2;
  }

  std::atomic<std::sig_atomic_t> Producer::MESSAGE_QUEUED_COUNT{0};
  std::atomic<std::sig_atomic_t> Producer::MESSAGE_COUNT{0};
  std::atomic<std::sig_atomic_t> Producer::MESSAGE_SUCCESS_COUNT{0};

  auto verify_certificate(bool, asio::ssl::verify_context &) -> bool
  {
    return true;
  }

  void load_certificates(asio::ssl::context &ctx,
                         const std::string &ca_file,
                         const std::string &cert_file,
                         const std::string &key_file)
  {
    try
    {
      ctx.load_verify_file(ca_file);
      ctx.use_certificate_file(cert_file, asio::ssl::context::pem);
      ctx.use_private_key_file(key_file, asio::ssl::context::pem);
    }
    catch (const std::exception &e)
    {
      mt_logging::logger().log(
          {fmt::format("Producer::load_certificates {}", e.what()),
           mt_logging::LogLevel::Error,
           true});
    }
  }

  Producer::Producer()
      : m_ioc{2}, m_strand(asio::make_strand(m_ioc)), m_conn{}, m_group_config(load_group_config()), m_valid_streams{}
  {
    if (!REDIS_GROUP_CONFIG || !REDIS_HOST || !REDIS_PORT ||
        !REDIS_PASSWORD || !REDIS_USE_SSL || !MTLOG_LOGFILE)
    {
      throw std::runtime_error(
          "Environment variables MTLOG_LOGFILE, REDIS_GROUP_CONFIG, "
          "REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }

    MESSAGE_QUEUED_COUNT.store(0);
    MESSAGE_COUNT.store(0);
    MESSAGE_SUCCESS_COUNT.store(0);

    m_signal_status.store(false);
    m_shutting_down.store(false);
    m_conn_alive.store(false);
    m_reconnect_count.store(0);
    m_state.store(ConnectionState::Idle);

    for (const auto &[groupName, cfg] : m_group_config)
    {
      for (const auto &s : cfg.streams)
      {
        if (s.empty())
          throw std::runtime_error("Stream name cannot be empty");
        if (s.find(' ') != std::string::npos)
          throw std::runtime_error("Stream name cannot contain spaces: " + s);
        m_valid_streams.insert(s);
      }
    }

    mt_logging::logger().log(
        {"Producer created",
         mt_logging::LogLevel::Info,
         true});

    asio::co_spawn(
        m_strand,
        [this]() -> asio::awaitable<void>
        {
          co_return co_await this->co_main();
        },
        asio::detached);

    m_sender_thread = std::thread([this]()
                                  { m_ioc.run(); });
  }

  Producer::~Producer()
  {
    m_shutting_down.store(true);

    if (m_conn)
    {
      asio::post(m_strand, [conn = m_conn]
                 {
        conn->cancel();
        conn->reset_stream(); });
    }

    asio::post(m_strand, [this]
               { m_ioc.stop(); });

    if (m_sender_thread.joinable())
      m_sender_thread.join();

    mt_logging::logger().log(
        {"Redis Producer destroyed",
         mt_logging::LogLevel::Info,
         true});
  }

  void Producer::enqueue_message(
      const std::string &stream,
      const std::vector<std::pair<std::string, std::string>> &fields)
  {
    if (m_signal_status.load())
      return;

    validate_stream_or_throw(stream, m_valid_streams, "(n/a)");

    if (m_state.load() != ConnectionState::Ready)
    {
      mt_logging::logger().log(
          {fmt::format("Producer dropping message while not Ready (stream={})", stream),
           mt_logging::LogLevel::Info,
           true});
      return;
    }

    ProduceMessage msg;
    msg.stream = stream;
    msg.fields.reserve(fields.size());
    for (auto &[f, v] : fields)
      msg.fields.push_back(ProduceField{f, v});

    MESSAGE_QUEUED_COUNT.fetch_add(1, std::memory_order_relaxed);
    MESSAGE_COUNT.fetch_add(1, std::memory_order_relaxed);

    asio::dispatch(
        m_strand,
        [this, msg = std::move(msg)]() mutable
        {
          asio::co_spawn(
              m_strand,
              produce_one(std::move(msg)),
              asio::detached);
        });
  }

  static void push_xadd(redis::request &req,
                        const ProduceMessage &m)
  {
    std::vector<std::string> args;
    args.reserve(2 + m.fields.size() * 2);

    args.push_back(m.stream);
    args.push_back("*");

    for (auto &f : m.fields)
    {
      args.push_back(f.field);
      args.push_back(f.value);
    }

    req.push_range("XADD", args);
  }

  asio::awaitable<void> Producer::produce_one(ProduceMessage msg)
  {
    auto ex = co_await asio::this_coro::executor;

    auto conn = m_conn;
    if (!conn)
      co_return;

    redis::request req;
    push_xadd(req, msg);

    boost::system::error_code exec_ec;
    boost::system::error_code timer_ec;
    redis::response<std::string> resp;

    asio::steady_timer timer{ex};
    timer.expires_after(std::chrono::seconds(PRODUCE_TIMEOUT_DELAY));

    auto result = co_await (
        conn->async_exec(
            req,
            resp,
            asio::redirect_error(asio::use_awaitable, exec_ec)) ||
        timer.async_wait(
            asio::redirect_error(asio::use_awaitable, timer_ec)));

    if (result.index() == 1)
    {
      if (m_state.load() == ConnectionState::Ready)
      {
        set_state(ConnectionState::Broken, "Produce timeout");
        conn->cancel();
        conn->reset_stream();
        m_conn_alive.store(false);
      }
      co_return;
    }

    if (exec_ec)
    {
      if (m_shutting_down.load() &&
          exec_ec == boost::asio::error::operation_aborted)
        co_return;

      if (m_state.load() != ConnectionState::Ready)
        co_return;

      set_state(ConnectionState::Broken,
                fmt::format("Produce failed: {}",
                            make_ops_error(
                                "XADD", msg.stream,
                                "(n/a)", "(n/a)",
                                exec_ec.message(),
                                "Check Redis connectivity and authentication")));
      conn->cancel();
      conn->reset_stream();
      m_conn_alive.store(false);
      co_return;
    }

    MESSAGE_SUCCESS_COUNT.fetch_add(1, std::memory_order_relaxed);
    std::string XID = std::get<0>(resp).value();
    set_state(ConnectionState::Ready,
              fmt::format("Produce OK: {} produced. XID {} for {}",
                          MESSAGE_SUCCESS_COUNT.load(), XID, msg.stream));
  }

  asio::awaitable<void> Producer::co_main()
  {
    auto ex = co_await asio::this_coro::executor;

    redis::config cfg;
    cfg.clientname = "redis_producer";
    cfg.addr.host = REDIS_HOST;
    cfg.addr.port = REDIS_PORT;
    cfg.password = REDIS_PASSWORD;
    if (std::string(REDIS_USE_SSL) == "on")
      cfg.use_ssl = true;
    cfg.health_check_interval = std::chrono::minutes(1);

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif
    sig_set.async_wait(
        [this](const boost::system::error_code &, int)
        {
          set_state(ConnectionState::Shutdown, "Signal received");
          m_signal_status.store(true);
          m_shutting_down.store(true);
          m_conn_alive.store(false);
          if (m_conn)
            m_conn->cancel();
        });

    for (;;)
    {
      if (m_shutting_down.load())
        co_return;

      // fresh connection per attempt
      if (std::string(REDIS_USE_SSL) == "on")
      {
        asio::ssl::context ssl_ctx{asio::ssl::context::tlsv12_client};
        ssl_ctx.set_verify_mode(asio::ssl::verify_peer);
        load_certificates(ssl_ctx,
                          "tls/ca.crt",
                          "tls/redis.crt",
                          "tls/redis.key");
        ssl_ctx.set_verify_callback(verify_certificate);
        m_conn = std::make_shared<redis::connection>(ex, std::move(ssl_ctx));
      }
      else
      {
        m_conn = std::make_shared<redis::connection>(ex);
      }

      set_state(ConnectionState::Connecting, "Starting async_run");
      m_conn_alive.store(true);

      m_conn->async_run(
          cfg,
          redis::logger{redis::logger::level::err},
          asio::consign(asio::detached, [this]
                        {
          if (m_shutting_down.load())
            return;
          set_state(ConnectionState::Broken, "async_run ended");
          m_conn_alive.store(false); }));

      // Startup PING with timeout
      {
        redis::request ping;
        ping.push("PING");

        boost::system::error_code ping_ec;
        boost::system::error_code ping_timer_ec;

        asio::steady_timer ping_timer{ex};
        ping_timer.expires_after(std::chrono::seconds(PRODUCE_TIMEOUT_DELAY));

        auto ping_result = co_await (
            m_conn->async_exec(
                ping,
                boost::redis::ignore,
                asio::redirect_error(asio::use_awaitable, ping_ec)) ||
            ping_timer.async_wait(
                asio::redirect_error(asio::use_awaitable, ping_timer_ec)));

        if (ping_result.index() == 1)
        {
          set_state(ConnectionState::Broken, "Startup PING timeout");
          m_conn_alive.store(false);
          m_conn->cancel();
          m_conn->reset_stream();

          set_state(ConnectionState::Reconnecting,
                    fmt::format("Retrying after failed PING in {} seconds",
                                CONNECTION_RETRY_DELAY));

          co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
              .async_wait(asio::use_awaitable);
          continue;
        }

        if (ping_ec)
        {
          if (m_shutting_down.load() &&
              ping_ec == boost::asio::error::operation_aborted)
            co_return;

          set_state(ConnectionState::Broken,
                    fmt::format("Startup PING failed: {}",
                                make_ops_error(
                                    "PING", "(n/a)",
                                    "(n/a)", "(n/a)",
                                    ping_ec.message(),
                                    "Check Redis connectivity and authentication")));

          m_conn_alive.store(false);
          m_conn->cancel();
          m_conn->reset_stream();

          set_state(ConnectionState::Reconnecting,
                    fmt::format("Retrying after failed PING in {} seconds",
                                CONNECTION_RETRY_DELAY));

          co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
              .async_wait(asio::use_awaitable);
          continue;
        }
      }

      // Ensure consumer groups exist
      for (const auto &[groupName, cfgGroup] : m_group_config)
      {
        for (const auto &stream : cfgGroup.streams)
        {
          mt_logging::logger().log(
              {fmt::format("Ensuring group {} exists on stream {}", groupName, stream),
               mt_logging::LogLevel::Info,
               true});

          redis::request req;
          req.push("XGROUP", "CREATE",
                   stream,
                   groupName,
                   "$",
                   "MKSTREAM");

          redis::response<std::string> resp;
          boost::system::error_code ec;

          co_await m_conn->async_exec(
              req,
              resp,
              asio::redirect_error(asio::use_awaitable, ec));

          if (ec)
          {
            std::string msg = ec.message();
            if (msg.find("BUSYGROUP") == std::string::npos)
            {
              throw std::runtime_error(
                  make_ops_error(
                      "XGROUP CREATE",
                      stream,
                      groupName,
                      "(n/a)",
                      msg,
                      "Verify stream name and Redis ACL permissions"));
            }
            else
            {
              mt_logging::logger().log(
                  {"Group already exists, continuing",
                   mt_logging::LogLevel::Info,
                   true});
            }
          }
        }
      }

      set_state(ConnectionState::Ready, "Redis connection established");

      while (!m_shutting_down.load() && m_conn_alive.load())
      {
        co_await asio::steady_timer(ex, std::chrono::milliseconds(200))
            .async_wait(asio::use_awaitable);
      }

      if (m_shutting_down.load())
        co_return;

      set_state(ConnectionState::Broken, "Connection dropped");

      m_reconnect_count.fetch_add(1, std::memory_order_relaxed);

      set_state(ConnectionState::Reconnecting,
                fmt::format("Reconnect attempt {} in {} seconds",
                            m_reconnect_count.load(), CONNECTION_RETRY_DELAY));

      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
          .async_wait(asio::use_awaitable);

      if (CONNECTION_RETRY_AMOUNT != -1 &&
          m_reconnect_count.load() >= CONNECTION_RETRY_AMOUNT)
        break;
    }

    set_state(ConnectionState::Shutdown, "Exiting co_main");
    m_signal_status.store(true);
  }

  void Producer::set_state(ConnectionState new_state, std::string_view reason)
  {
    ConnectionState old = m_state.exchange(new_state);

    auto to_str = [](ConnectionState s)
    {
      switch (s)
      {
      case ConnectionState::Idle:
        return "Idle";
      case ConnectionState::Connecting:
        return "Connecting";
      case ConnectionState::Authenticating:
        return "Authenticating";
      case ConnectionState::Ready:
        return "Ready";
      case ConnectionState::Broken:
        return "Broken";
      case ConnectionState::Reconnecting:
        return "Reconnecting";
      case ConnectionState::Shutdown:
        return "Shutdown";
      }
      return "Unknown";
    };

    mt_logging::logger().log(
        {fmt::format("State: {} → {} ({})",
                     to_str(old), to_str(new_state), reason),
         new_state == ConnectionState::Broken
             ? mt_logging::LogLevel::Error
             : mt_logging::LogLevel::Info,
         true});
  }


} // namespace WorkQStream

#endif // HAVE_ASIO
