
#include "Recovery.h"
#include <memory>
#include <fstream>
#include <sstream>
#include <string>
#include <iomanip>
#include <mtlog/mt_log.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>

using namespace boost::asio::experimental::awaitable_operators;

namespace WorkQStream
{

  static const char *WORKER_GROUP = std::getenv("WORKER_GROUP");
  static const char *WORKER_RECOVER_PENDING = std::getenv("WORKER_RECOVER_PENDING");
  static const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");
  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 10;
  static const int RECOVER_PENDING_DELAY = 10;
  static const int TRIM_STREAM_DELAY = 60;
  static const int TRIM_STREAM_SIZE = 50000;

#if defined(BOOST_ASIO_HAS_CO_AWAIT)

  Recovery::Recovery(
      const std::string &workerId) : m_ioc{3},
                                     m_signals(m_ioc.get_executor()),
                                     m_worker_id(workerId),
                                     m_group_config(load_group_config()),
                                     m_valid_streams{}
  {
    if (MTLOG_LOGFILE == nullptr ||
        REDIS_HOST == nullptr || REDIS_PORT == nullptr ||
        REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables MTLOG_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }

    m_signal_status.store(false);

    mt_logging::logger().log(
        {"Redis Consumer created",
         mt_logging::LogLevel::Info, true});

    for (const auto &s : get_worker_group(m_group_config).streams)
    {
      if (s.empty())
        throw std::runtime_error("Stream name cannot be empty");
      if (s.find(' ') != std::string::npos)
        throw std::runtime_error("Stream name cannot contain spaces: " + s);
      mt_logging::logger().log(
          {fmt::format("Valid stream {}", s),
           mt_logging::LogLevel::Info, true});
      m_valid_streams.insert(s);
    }

    asio::co_spawn(m_ioc.get_executor(), Recovery::co_main(), asio::detached);
    m_receiver_thread = std::thread([this]()
                                    { m_ioc.run(); });
  }

  Recovery::~Recovery()
  {
    mt_logging::logger().log(
        {"Redis Recovery destroying",
         mt_logging::LogLevel::Debug, true});

    m_signal_status.store(true); // signal stop
    join();

    mt_logging::logger().log(
        {"Redis Recovery destroyed",
         mt_logging::LogLevel::Debug, true});
  }

  void Recovery::request_stop()
  {
    m_signal_status.store(true);
  }

  void Recovery::join()
  {
    if (m_receiver_thread.joinable())
      m_receiver_thread.join();
  }

  void Recovery::setup_signals()
  {
    m_signals.add(SIGINT);
    m_signals.add(SIGTERM);
#if defined(SIGQUIT)
    m_signals.add(SIGQUIT);
#endif
    m_signals.async_wait(
        [&](const boost::system::error_code &, int)
        {
          m_signal_status.store(true);
        });
  }

  std::shared_ptr<redis::connection> Recovery::make_connection(
      const boost::asio::any_io_executor &ex,
      bool use_ssl)
  {
    auto log_callback =
        [this](redis::logger::level lvl, std::string_view msg)
    {
      // Optional: log everything
      mt_logging::logger().log(
          {fmt::format("[Redis log] {}", msg),
           mt_logging::LogLevel::Debug,
           true});
    };

    if (use_ssl)
    {
      asio::ssl::context ctx{asio::ssl::context::tlsv12_client};
      ctx.set_verify_mode(asio::ssl::verify_peer);
      /** Your self-signed CA, Your client certificate, Your private key */
      load_certificates(ctx, "tls/ca.crt", "tls/redis.crt", "tls/redis.key");
      ctx.set_verify_callback(verify_certificate);

      return std::make_shared<redis::connection>(
          ex,
          std::move(ctx),
          redis::logger{redis::logger::level::info, log_callback});
    }
    else
    {
      return std::make_shared<redis::connection>(
          ex,
          redis::logger{redis::logger::level::err, log_callback});
    }
  }

  asio::awaitable<void> Recovery::run_recovery_mode(std::shared_ptr<redis::connection> conn)
  {
    auto ex = co_await asio::this_coro::executor;

    // Let TLS handshake finish
    co_await asio::steady_timer(ex, std::chrono::milliseconds(150)).async_wait(asio::use_awaitable);

    for (auto &stream : m_valid_streams)
    {
      co_await recover_pending_with_conn(stream, conn);
      co_await trim_stream_with_conn(stream, conn);
    }
    co_return;
  }

  auto Recovery::co_main() -> asio::awaitable<void>
  {
    try
    {
      auto ex = co_await asio::this_coro::executor;
      m_signal_status.store(false);

      mt_logging::logger().log(
          {fmt::format("Worker id:  {}", m_worker_id),
           mt_logging::LogLevel::Info,
           true});

      setup_signals();

      bool use_ssl = std::string(REDIS_USE_SSL) == "on";

      redis::config cfg;
      cfg.addr.host = REDIS_HOST;
      cfg.addr.port = REDIS_PORT;
      cfg.password = REDIS_PASSWORD;
      cfg.use_ssl = use_ssl;
      cfg.health_check_interval = std::chrono::seconds(0); // only disables periodic PING

      for (;;)
      {
        if (m_signal_status.load())
          co_return;
        auto conn = make_connection(ex, use_ssl);

        co_await (
            conn->async_run(cfg, asio::use_awaitable) ||
            asio::steady_timer(ex, std::chrono::seconds(5)).async_wait(asio::use_awaitable) ||
            run_recovery_mode(conn));

        mt_logging::logger().log(
            {fmt::format("co_main recover and xtrim."),
             mt_logging::LogLevel::Info,
             true});
        co_await asio::steady_timer(ex, std::chrono::seconds(RECOVER_PENDING_DELAY)).async_wait(asio::use_awaitable);
      }
    }
    catch (const std::exception &e)
    {
      mt_logging::logger().log(
          {fmt::format("co_main error: {}", e.what()),
           mt_logging::LogLevel::Error,
           true});
      m_signal_status.store(true);
    }
    co_return;
  }

  void Recovery::push_dlq_xadd(redis::request &req,
                               const std::string &stream,
                               std::string_view id,
                               const std::unordered_map<std::string, std::string> &fields)
  {
    std::vector<std::string> args;
    args.reserve(4 + fields.size() * 2);

    args.push_back(stream);
    args.push_back("*");

    args.push_back("orig_id");
    args.push_back(std::string(id));

    for (auto &[k, v] : fields)
    {
      args.push_back(k);
      args.push_back(v);
    }

    req.push_range("XADD", args);
  }

  asio::awaitable<void> Recovery::xack(std::string_view stream, std::string_view id,
                                       std::shared_ptr<redis::connection> conn)
  {
    redis::request req;
    req.push("XACK", stream, WORKER_GROUP, id);
    mt_logging::logger().log(
        {fmt::format("XACK'd work item:     [STREAM {}      ID {}]  WORKER GROUP {}", stream, id, WORKER_GROUP),
         mt_logging::LogLevel::Info,
         true});

    boost::system::error_code ec;
    co_await conn->async_exec(req, redis::ignore, asio::redirect_error(asio::use_awaitable, ec));

    if (ec)
    {
      mt_logging::logger().log(
          {fmt::format("XACK failed: {}", ec.message()),
           mt_logging::LogLevel::Error, true});
      co_return;
    }
  }

  asio::awaitable<void> Recovery::send_to_dlq(std::string_view stream, std::string_view id,
                                              const std::unordered_map<std::string, std::string> &fields,
                                              std::shared_ptr<redis::connection> conn)
  {
    redis::request req;
    push_dlq_xadd(req, std::string(stream) + ".DLQ", id, fields);
    mt_logging::logger().log(
        {fmt::format("DLQ'd work item:      [STREAM {}      ID {}]  WORKER GROUP {}", std::string(stream) + ".DLQ", id, WORKER_GROUP),
         mt_logging::LogLevel::Info, true});

    boost::system::error_code ec;
    co_await conn->async_exec(req, redis::ignore, asio::redirect_error(asio::use_awaitable, ec));
    if (ec)
    {
      mt_logging::logger().log(
          {fmt::format("DLQ XADD failed: {}", ec.message()),
           mt_logging::LogLevel::Error, true});
      co_return;
    }

    // Remove from PEL
    co_await xack(stream, id, conn);
  }

  asio::awaitable<void> Recovery::recover_pending_with_conn(
      std::string stream,
      std::shared_ptr<redis::connection> conn)
  {
    auto ex = co_await asio::this_coro::executor;

    if (m_signal_status.load())
      co_return;

    // 1. Get up to 10 pending messages
    mt_logging::logger().log(
        {fmt::format("XPENDING data            [STREAM {}      WORKER GROUP {}]", stream, WORKER_GROUP),
         mt_logging::LogLevel::Info,
         true});

    redis::request req;
    req.push("XPENDING", stream, std::string(WORKER_GROUP), "-", "+", "10");

    redis::generic_response resp;
    boost::system::error_code ec;
    co_await asio::steady_timer(ex, std::chrono::milliseconds(150)).async_wait(asio::use_awaitable);

    co_await conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

    if (ec)
    {
      mt_logging::logger().log(
          {fmt::format("XPENDING fail         [STREAM {}      WORKER GROUP {}] {}", stream, WORKER_GROUP, ec.message()),
           mt_logging::LogLevel::Error,
           true});
      co_return;
    }

    auto pendings = parse_xpending(resp);
    if (pendings.empty())
      co_return;

    for (auto &p : pendings)
    {
      mt_logging::logger().log(
          {fmt::format(" - Pending ID: {} consumer={} idle={} deliveries={}", p.id, p.consumer, p.idle_ms, p.delivery_count),
           mt_logging::LogLevel::Info,
           true});
      // Do DLQ logic
      if (p.delivery_count > 5)
      {
        co_await send_to_dlq(stream, p.id, {/** field are missing can be found with XREADGROUP see later */}, conn);
        // send_to_dlq_now(stream, p.id, {/** field are missing can be found with XREADGROUP see later */});
        continue;
      }
    }
    co_return;
  }

  asio::awaitable<void> Recovery::trim_stream_with_conn(
      std::string stream,
      std::shared_ptr<redis::connection> conn)
  {

    auto ex = co_await asio::this_coro::executor;

    if (m_signal_status.load())
      co_return;

    redis::request req;
    mt_logging::logger().log(
        {fmt::format("XTRIM data            [STREAM {}      WORKER GROUP {}]", stream, WORKER_GROUP),
         mt_logging::LogLevel::Info,
         true});

    req.push("XTRIM", stream, "MAXLEN", "~", std::to_string(TRIM_STREAM_SIZE));

    boost::system::error_code ec;
    co_await asio::steady_timer(ex, std::chrono::milliseconds(150)).async_wait(asio::use_awaitable);
    co_await conn->async_exec(req, redis::ignore, asio::redirect_error(asio::use_awaitable, ec));

    if (ec)
    {
      mt_logging::logger().log(
          {fmt::format("XTRIM failed          [STREAM {}      WORKER GROUP {}] {}", stream, WORKER_GROUP, ec.message()),
           mt_logging::LogLevel::Error,
           true});
    }

    co_return;
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif
