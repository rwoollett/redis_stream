
#include "Producer.h"
#include <workqstream/common/Common.h>
#include <memory>
#include <fstream>
#include <sstream>
#include <string>
#include <tuple>
#include <iomanip>
#include <mtlog/mt_log.hpp>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/redis/response.hpp>
#include <boost/redis/request.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
using namespace boost::asio::experimental::awaitable_operators;

namespace WorkQStream
{

  static const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");
  static const char *REDIS_GROUP_CONFIG = std::getenv("REDIS_GROUP_CONFIG");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 3;
  static const int PRODUCE_TIMEOUT_DELAY = 2; // ensure longer than 1 sec to avoid reconnect ssl rteradown abort

#if defined(BOOST_ASIO_HAS_CO_AWAIT)

  auto verify_certificate(bool, asio::ssl::verify_context &) -> bool
  {
    return true;
  }
  // Helper to load a file into an SSL context
  void load_certificates(asio::ssl::context &ctx,
                         const std::string &ca_file,
                         const std::string &cert_file,
                         const std::string &key_file)
  {
    try
    {
      // Load trusted CA
      ctx.load_verify_file(ca_file);

      // Load client certificate
      ctx.use_certificate_file(cert_file, asio::ssl::context::pem);

      // Load private key
      ctx.use_private_key_file(key_file, asio::ssl::context::pem);
    }
    catch (const std::exception &e)
    {
      mt_logging::logger().log(
          {fmt::format("Producer::load certiciates {}", e.what()),
           mt_logging::LogLevel::Error,
           true});
    }
  }

  Producer::Producer() : m_ioc{2},
                         m_conn{},
                         msg_queue{},
                         m_group_config(load_group_config()),
                         m_valid_streams{}
  {
    if (REDIS_GROUP_CONFIG == nullptr ||
        REDIS_HOST == nullptr || REDIS_PORT == nullptr ||
        REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr ||
        MTLOG_LOGFILE == nullptr)
    {
      throw std::runtime_error("Environment variables MTLOG_LOGFILE, REDIS_GROUP_CONFIG, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }

    MESSAGE_QUEUED_COUNT.store(0);
    MESSAGE_COUNT.store(0);
    MESSAGE_SUCCESS_COUNT.store(0);

    m_is_connected.store(false);
    m_signal_status.store(false);
    m_shutting_down.store(false);
    m_conn_alive.store(false);
    m_state.store(ConnectionState::Idle);

    mt_logging::logger().log(
        {"Producer created",
         mt_logging::LogLevel::Info,
         true});

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
    // asio::co_spawn(m_ioc.get_executor(), Producer::co_main(), asio::detached);
    asio::co_spawn(
        m_ioc.get_executor(),
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

    msg_queue.shutdown();
    msg_queue.push(ProduceMessage{}); // dummy wake

    if (m_worker.joinable())
      m_worker.join();

    if (m_conn)
    {
      // Schedule cancel on the io_context thread
      boost::asio::post(m_ioc, [conn = m_conn]
                        {
                          conn->cancel();
                          conn->reset_stream(); //
                        });
    }

    boost::asio::post(m_ioc, [this]
                      { m_ioc.stop(); });

    if (m_sender_thread.joinable())
      m_sender_thread.join();

    mt_logging::logger().log({"Redis Producer destroyed",
                              mt_logging::LogLevel::Info,
                              true});
  }

  void Producer::enqueue_message(const std::string &channel, const std::vector<std::pair<std::string, std::string>> &fields)
  {
    if (m_signal_status.load())
      return;

    validate_stream_or_throw(channel, m_valid_streams, "(n/a)");

    ProduceMessage msg;
    std::strncpy(msg.channel, channel.c_str(), CHANNEL_LENGTH - 1);
    msg.channel[CHANNEL_LENGTH - 1] = '\0'; // Always null-terminate

    msg.field_count = fields.size();
    int i = 0;
    for (const auto &[in_field, in_value] : fields)
    {
      strncpy(msg.fields[i].field, in_field.c_str(), FIELD_NAME_LENGTH - 1);
      msg.fields[i].field[FIELD_NAME_LENGTH - 1] = '\0';
      strncpy(msg.fields[i].value, in_value.c_str(), FIELD_VALUE_LENGTH - 1);
      msg.fields[i].value[FIELD_VALUE_LENGTH - 1] = '\0';
      i++;
    }

    MESSAGE_QUEUED_COUNT.fetch_add(1, std::memory_order_relaxed);
    msg_queue.push(msg);
  }

  void push_xadd(redis::request &req, const std::string &stream, const ProduceMessage &m)
  {
    std::vector<std::string> args;
    args.reserve(2 + m.field_count * 2);

    args.push_back(stream);
    args.push_back("*");

    for (int i = 0; i < m.field_count; ++i)
    {
      args.push_back(m.fields[i].field);
      args.push_back(m.fields[i].value);
    }

    req.push_range("XADD", args);
  }

  asio::awaitable<void> Producer::produce_one(const ProduceMessage &msg)
  {
    auto ex = co_await asio::this_coro::executor;
    // Take a strong ref to whatever connection is current *now*
    auto conn = m_conn;
    if (!conn)
      co_return;

    redis::request req;
    push_xadd(req, msg.channel, msg);

    boost::system::error_code exec_ec;
    boost::system::error_code timer_ec;
    redis::response<std::string> resp;

    asio::steady_timer timer{ex};
    timer.expires_after(std::chrono::seconds(PRODUCE_TIMEOUT_DELAY)); // produce timeout

    // Wait for EITHER exec OR timer
    auto result = co_await (
        conn->async_exec(
            req,
            resp,
            asio::redirect_error(asio::use_awaitable, exec_ec)) ||
        timer.async_wait(
            asio::redirect_error(asio::use_awaitable, timer_ec)));

    // Timer fired first → async_exec is considered hung
    if (result.index() == 1)
    {
      set_state(ConnectionState::Broken, "Produce timeout");
      conn->cancel();
      conn->reset_stream();
      m_conn_alive.store(false);

      co_return;
    }

    // Exec completed first: check error
    if (exec_ec)
    {
      set_state(ConnectionState::Broken,
                fmt::format("Produce failed: {}", make_ops_error(
                                                      "XADD", msg.channel,
                                                      "(n/a)", "(n/a)",
                                                      exec_ec.message(),
                                                      "Check Redis connectivity and authentication")));
      conn->cancel();
      conn->reset_stream();
      m_conn_alive.store(false);

      co_return;
    }

    // Success path
    MESSAGE_SUCCESS_COUNT.fetch_add(1, std::memory_order_relaxed);
    std::string XID = std::get<0>(resp).value();
    set_state(ConnectionState::Ready, fmt::format("Produce OK: {} produced. XID {} for {}", MESSAGE_SUCCESS_COUNT.load(), XID, msg.channel));
  }

  void Producer::worker_thread_fn(boost::asio::any_io_executor ex)
  {
    for (;;)
    {
      ProduceMessage msg;
      // Blocking wait
      if (!msg_queue.blocking_pop(msg))
        break; // queue shutdown

      if (m_shutting_down.load())
        break;

      // Hand off to Asio thread
      std::cerr << "post to asio\n";
      asio::post(ex, [this, msg, ex]
                 { asio::co_spawn(
                       ex,
                       [this, msg]() -> asio::awaitable<void>
                       {
                         co_return co_await produce_one(msg);
                       },
                       asio::detached); });
    }

    // --- Shutdown cleanup ---
    if (m_shutting_down.load())
    {
      ProduceMessage leftover;
      int dropped = 0;

      while (msg_queue.pop(leftover))
        dropped++;

      mt_logging::logger().log({fmt::format("Producer shutdown: dropped {} pending messages", dropped),
                                mt_logging::LogLevel::Info,
                                true});
    }
  }

  auto Producer::co_main() -> asio::awaitable<void>
  {
    auto ex = co_await asio::this_coro::executor;
    redis::config cfg;
    cfg.clientname = "redis_producer";
    cfg.addr.host = REDIS_HOST;
    cfg.addr.port = REDIS_PORT;
    cfg.password = REDIS_PASSWORD;
    if (std::string(REDIS_USE_SSL) == "on")
    {
      cfg.use_ssl = true;
    }
    // cfg.health_check_interval = std::chrono::seconds(0); // set 0 for tls friendly
    cfg.health_check_interval = std::chrono::minutes(1); // set 0 for tls friendly

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif // defined(SIGQUIT)
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          set_state(ConnectionState::Shutdown, "Signal received");
          m_signal_status.store(true);
          m_shutting_down.store(true);
          if (m_conn)
            m_conn->cancel();
          msg_queue.shutdown();
        });

    // --- Start worker thread ---
    m_worker = std::thread([this, ex]
                           { this->worker_thread_fn(ex); });

    if (std::string(REDIS_USE_SSL) == "on")
    {
      asio::ssl::context ssl_ctx{asio::ssl::context::tlsv12_client};
      ssl_ctx.set_verify_mode(asio::ssl::verify_peer);
      load_certificates(ssl_ctx,
                        "tls/ca.crt",    // Your self-signed CA
                        "tls/redis.crt", // Your client certificate
                        "tls/redis.key"  // Your private key
      );
      ssl_ctx.set_verify_callback(verify_certificate);
      m_conn = std::make_shared<redis::connection>(ex, std::move(ssl_ctx));
    }
    else
    {
      m_conn = std::make_shared<redis::connection>(ex);
    }

    for (;;)
    {
      if (m_shutting_down.load())
      {
        co_return;
      }

      set_state(ConnectionState::Connecting, "Starting async_run");
      m_conn_alive.store(true);

      // m_conn->async_run(cfg, redis::logger{redis::logger::level::err}, asio::consign(asio::detached, m_conn));
      m_conn->async_run(
          cfg,
          redis::logger{redis::logger::level::err},
          asio::consign(asio::detached, [this]
                        {
                set_state(ConnectionState::Broken, "async_run ended");
                m_conn_alive.store(false); }));

      // --- Confirm Redis is actually reachable ---
      {
        redis::request ping;
        ping.push("PING");

        boost::system::error_code ec;
        co_await m_conn->async_exec(
            ping,
            boost::redis::ignore,
            asio::redirect_error(asio::use_awaitable, ec));

        if (ec)
        {
          set_state(ConnectionState::Broken,
                    fmt::format("Startup PING failed: {}", make_ops_error(
                                                               "PING", "(n/a)",
                                                               "(n/a)", "(n/a)",
                                                               ec.message(),
                                                               "Check Redis connectivity and authentication")));

          m_conn_alive.store(false);
          m_conn->cancel();
          m_conn->reset_stream();

          set_state(ConnectionState::Reconnecting, "Retrying after failed PING");

          co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
              .async_wait(asio::use_awaitable);

          continue; // reconnect loop
        }
      }

      // ------------------------------------------------------------
      // Ensure the consumer group exists (idempotent)
      // ------------------------------------------------------------
      for (const auto &[groupName, cfg] : m_group_config)
      {
        for (const auto &stream : cfg.streams)
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
          co_await m_conn->async_exec(req, resp,
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

      // --- Redis is confirmed UP ---
      set_state(ConnectionState::Ready, "Redis connection established");

      // --- Wait until connection dies or shutdown requested ---
      while (!m_shutting_down.load() && m_conn_alive.load())
      {
        co_await asio::steady_timer(ex, std::chrono::milliseconds(200))
            .async_wait(asio::use_awaitable);
      }

      if (m_shutting_down.load())
      {
        co_return;
      }

      set_state(ConnectionState::Broken, "Connection dropped");

      // Delay before reconnecting
      m_reconnect_count.fetch_add(1, std::memory_order_relaxed);

      set_state(ConnectionState::Reconnecting,
                fmt::format("Reconnect attempt {} in {} seconds",
                            m_reconnect_count.load(), CONNECTION_RETRY_DELAY));

      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY)).async_wait(asio::use_awaitable);

      if (CONNECTION_RETRY_AMOUNT == -1)
        continue;
      if (m_reconnect_count.load() >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
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

    mt_logging::logger().log({fmt::format("State: {} → {} ({})",
                                          to_str(old), to_str(new_state), reason),
                              ConnectionState::Broken == new_state ? mt_logging::LogLevel::Error : mt_logging::LogLevel::Info,
                              true});
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif

// asio::awaitable<void> Producer::process_messages()
// {
//   boost::system::error_code ec;
//   redis::request ping_req;
//   ping_req.push("PING");

//   co_await m_conn->async_exec(ping_req, boost::redis::ignore, asio::redirect_error(asio::deferred, ec));
//   if (ec)
//   {
//     m_is_connected.store(false);
//     mt_logging::logger().log(
//         {REDIS_STREAM_PRODUCER_LOGFILE,
//          make_ops_error(
//              "PING", "(n/a)",
//              "(n/a)", "(n/a)",
//              ec.message(),
//              "Check Redis connectivity and authentication"),
//          std::ios::app,
//          true});
//     co_return; // Connection lost, break so we can exit function and try reconnect to redis.
//   }

//   // ------------------------------------------------------------
//   // Ensure the consumer group exists (idempotent)
//   // ------------------------------------------------------------
//   for (const auto &[groupName, cfg] : m_group_config)
//   {
//     for (const auto &stream : cfg.streams)
//     {
//       mt_logging::logger().log(
//           {REDIS_STREAM_PRODUCER_LOGFILE,
//            fmt::format("Ensuring group {} exists on stream {}", groupName, stream),
//            std::ios::app,
//            true});
//       redis::request req;
//       req.push("XGROUP", "CREATE",
//                stream,
//                groupName,
//                "$",
//                "MKSTREAM");

//       redis::response<std::string> resp;

//       boost::system::error_code ec;
//       co_await m_conn->async_exec(req, resp,
//                                   asio::redirect_error(asio::use_awaitable, ec));

//       if (ec)
//       {
//         std::string msg = ec.message();

//         if (msg.find("BUSYGROUP") == std::string::npos)
//         {
//           throw std::runtime_error(
//               make_ops_error(
//                   "XGROUP CREATE",
//                   stream,
//                   groupName,
//                   "(n/a)",
//                   msg,
//                   "Verify stream name and Redis ACL permissions"));
//         }
//         else
//         {
//           mt_logging::logger().log(
//               {REDIS_STREAM_PRODUCER_LOGFILE,
//                "Group already exists, continuing",
//                std::ios::app,
//                true});
//         }
//       }
//     }
//   }

//   m_is_connected.store(true);
//   m_reconnect_count.store(0); // reset
//   for (boost::system::error_code ec;;)
//   {
//     if (m_shutting_down.load())
//       break;

//     // std::vector<ProduceMessage> batch;
//     ProduceMessage m;
//     // while (batch.size() < BATCH_SIZE && msg_queue.blocking_pop(msg))
//     // {
//     //   if (m_shutting_down.load())
//     //     break;
//     //   batch.push_back(msg);
//     //   MESSAGE_COUNT.fetch_add(1, std::memory_order_relaxed);
//     // }
//     if (!msg_queue.blocking_pop(m))
//     {
//       if (m_shutting_down.load())
//         break;
//       continue;
//     }

//     if (m_shutting_down.load())
//       break;

//     // if (batch.empty())
//     //   continue;

//     // for (const auto &m : batch)
//     // {
//     redis::request req;
//     push_xadd(req, m.channel, m);

//     redis::response<std::string> resp;
//     req.get_config().cancel_if_not_connected = true;
//     co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

//     if (ec)
//     {
//       mt_logging::logger().log(
//           {REDIS_STREAM_PRODUCER_LOGFILE,
//            fmt::format(
//                "Perform a full reconnect to redis. Reason for error: {}",
//                make_ops_error(
//                    "XADD", m.channel,
//                    "(n/a)", "(n/a)",
//                    ec.message(),
//                    "Check Redis connectivity and authentication")),
//            std::ios::app,
//            true});

//       // for (const auto &m : batch)
//       // {
//       msg_queue.push(m);
//       MESSAGE_COUNT.fetch_sub(1, std::memory_order_relaxed);
//       //}

//       co_return; // Connection lost, exit function and try reconnect to redis.
//     }

//     MESSAGE_SUCCESS_COUNT.fetch_add(1, std::memory_order_relaxed);

//     std::string XID = std::get<0>(resp).value();
//     D(mt_logging::logger().log(
//         {REDIS_STREAM_PRODUCER_LOGFILE,
//          fmt::format("Messages queued: {}, messages XADD'ed {}", MESSAGE_QUEUED_COUNT.load(), MESSAGE_COUNT.load()),
//          std::ios::app,
//          true});)
//     mt_logging::logger().log(
//         {REDIS_STREAM_PRODUCER_LOGFILE,
//          fmt::format("Redis Produce: {} produced. XID {}", MESSAGE_SUCCESS_COUNT.load(), XID),
//          std::ios::app,
//          true});
//     //}
//   }
//   // Drop all remaining messages on shutdown
//   if (m_shutting_down.load())
//   {
//     ProduceMessage leftover;
//     int dropped = 0;
//     while (msg_queue.pop(leftover))
//     {
//       dropped++;
//     }

//     mt_logging::logger().log({REDIS_STREAM_PRODUCER_LOGFILE,
//                               fmt::format("Producer shutdown: dropped {} pending messages", dropped),
//                               std::ios::app,
//                               true});
//   }
// }
