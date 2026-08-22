
#include "Consumer.h"
#include <memory>
#include <fstream>
#include <sstream>
#include <string>
#include <iomanip>
#include <mtlog/mt_log.hpp>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>

namespace WorkQStream
{

  static const char *WORKER_GROUP = std::getenv("WORKER_GROUP");
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

  void Awakener::broadcast_single(
      std::string stream_name,
      std::string message_id,
      std::unordered_map<std::string, std::string> fields)
  {
    mt_logging::logger().log(
        {fmt::format("Broadcast work item message \n STREAM: {}\n    ID: {}\n", stream_name, message_id, fmt::join(fields, ", ")),
         mt_logging::LogLevel::Info,
         true});
  }

  Consumer::Consumer(
      const std::string &workerId,
      Awakener &awakener) : m_ioc{3},
                            m_signals(m_ioc.get_executor()),
                            m_awakener(awakener),
                            m_conn_read{},
                            m_conn_write{},
                            m_write_strand(asio::make_strand(m_ioc)),
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

    m_is_connected.store(false);
    m_connected.store(false);
    m_connecting.store(false);
    m_signal_status.store(false);
    m_cstoken_message_count.store(0);

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

    asio::co_spawn(m_ioc.get_executor(), Consumer::co_main(), asio::detached);
    m_receiver_thread = std::thread([this]()
                                    { m_ioc.run(); });
  }

  Consumer::~Consumer()
  {
    mt_logging::logger().log(
        {"Redis Consumer destroying",
         mt_logging::LogLevel::Debug, true});
    request_stop();
    join();
    mt_logging::logger().log(
        {"Redis Consumer destroyed",
         mt_logging::LogLevel::Debug, true});
  }

  void Consumer::request_stop()
  {
    m_signal_status.store(true);

    // Wake Redis operations
    if (m_conn_read)
    {
      boost::asio::post(m_ioc, [conn = m_conn_read]
                        { conn->cancel(); });
    }
    if (m_conn_write)
    {
      boost::asio::post(m_write_strand, [conn = m_conn_write]
                        { conn->cancel(); });
    }

    // Wake the awakener
    m_awakener.stop();

    // Stop the io_context on its own thread
    boost::asio::post(m_ioc, [this]
                      { m_ioc.stop(); });
  }

  void Consumer::join()
  {
    if (m_receiver_thread.joinable())
      m_receiver_thread.join();
  }

  asio::awaitable<void> Consumer::ensure_group_exists()
  {
    for (const auto &stream : m_valid_streams)
    {
      mt_logging::logger().log(
          {fmt::format("Ensuring group {} exists on stream {}", WORKER_GROUP, stream),
           mt_logging::LogLevel::Info,
           true});

      redis::request req;
      req.push("XGROUP", "CREATE",
               stream,
               WORKER_GROUP,
               "0",
               "MKSTREAM");

      redis::response<std::string> resp;
      boost::system::error_code ec;

      co_await m_conn_write->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));
      if (ec)
      {
        std::string msg = ec.message();
        if (msg.find("BUSYGROUP") != std::string::npos)
        {
          mt_logging::logger().log(
              {"Group already exists, continuing",
               mt_logging::LogLevel::Warn,
               true});
        }
        else
        {
          throw std::runtime_error(
              make_ops_error(
                  "XGROUP CREATE",
                  stream,
                  WORKER_GROUP,
                  "(n/a)",
                  msg,
                  "Verify stream name and Redis ACL permissions"));
        }
      }
    }
  }

  void Consumer::read_stream(const redis::generic_response &resp)
  {
    auto dispatch_items = parse_dispatch_view(resp);

    for (auto &item : dispatch_items)
    {
      m_cstoken_message_count.fetch_add(1, std::memory_order_relaxed);
      m_awakener.broadcast_single(
          std::string(item.stream),       // service name
          std::string(item.id),           // message ID
          std::move(convert_fields(item)) // all fields
      );
    }
  }

  auto Consumer::receiver() -> asio::awaitable<void>
  {
    redis::request req;
    std::vector<std::string> args;
    args.reserve(6 + m_valid_streams.size() * 2);

    args.push_back("GROUP");
    args.push_back(WORKER_GROUP);
    args.push_back(m_worker_id);
    args.push_back("BLOCK");
    args.push_back("5000");
    args.push_back("STREAMS");

    size_t index = 0;
    for (auto it = m_valid_streams.begin(); it != m_valid_streams.end(); ++it, ++index)
    {
      args.push_back(*it);
    }
    for (size_t i = 0; i < m_valid_streams.size(); ++i)
    {
      args.push_back(">");
    }

    req.push_range("XREADGROUP", args);
    redis::generic_response resp;

    // req.get_config().cancel_if_not_connected = false;
    m_is_connected.store(true);
    m_reconnect_count.store(0); // reset

    for (boost::system::error_code ec;;)
    {
      if (m_signal_status.load())
      {
        co_return;
      }
      co_await m_conn_read->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));
      if (ec)
      {
        if (ec == asio::error::operation_aborted)
        {
          mt_logging::logger().log(
              {fmt::format(
                   "- Consumer::receiver operation_aborted → marking disconnected {} {}", ec.message(), ec.value()),
               mt_logging::LogLevel::Error,
               true});

          m_connected.store(false);
          co_return; // true; // false; // do not reconnect this ec
        }

        mt_logging::logger().log(
            {fmt::format(
                 "Perform a full reconnect to redis. Reason for error: {}",
                 make_ops_error(
                     "XREADGROUP", "(n/a)",
                     WORKER_GROUP, m_worker_id,
                     ec.message(),
                     "Check Redis connectivity and authentication")),
             mt_logging::LogLevel::Error,
             true});

        m_connected.store(false);

        throw std::runtime_error(
            make_ops_error(
                "XREADGROUP", "(n/a)",
                WORKER_GROUP, m_worker_id,
                ec.message(),
                "Check Redis connectivity and authentication"));
        //
      }

      read_stream(resp);

      resp.value().clear(); // Clear the response value to avoid processing old messages again.
    }
  }

  void Consumer::setup_signals()
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
          m_awakener.stop();
          if (m_conn_read)
          {
            m_conn_read->cancel();
          }
          if (m_conn_write)
          {
            m_conn_write->cancel();
          }
        });
  }

  void Consumer::setup_connections(const boost::asio::any_io_executor &ex)
  {

    m_connected.store(false); // start pessimistic

    auto use_ssl = std::string(REDIS_USE_SSL) == "on";
    m_conn_read = make_connection(ex, use_ssl);
    m_conn_write = make_connection(ex, use_ssl);

    redis::config cfg_read;
    cfg_read.addr.host = REDIS_HOST;
    cfg_read.addr.port = REDIS_PORT;
    cfg_read.password = REDIS_PASSWORD;
    cfg_read.use_ssl = use_ssl;
    // minute health check:

    redis::config cfg_write;
    cfg_write.addr.host = REDIS_HOST;
    cfg_write.addr.port = REDIS_PORT;
    cfg_write.password = REDIS_PASSWORD;
    cfg_write.use_ssl = use_ssl;
    // minute health check:
    cfg_read.health_check_interval = std::chrono::minutes(1);  // set 0 for tls friendly
    cfg_write.health_check_interval = std::chrono::minutes(1); // set 0 for tls friendly

    m_conn_read->async_run(
        cfg_read,
        [this, self = m_conn_read](boost::system::error_code ec)
        {
          mt_logging::logger().log(
              {fmt::format("[m_conn_read async_run] ended: {}", ec.message()),
               mt_logging::LogLevel::Debug,
               true});
          m_connected.store(false);
        });
    m_conn_write->async_run(
        cfg_write,
        [this, self = m_conn_write](boost::system::error_code ec)
        {
          mt_logging::logger().log(
              {fmt::format("[m_conn_write async_run] ended: {}", ec.message()),
               mt_logging::LogLevel::Debug,
               true});
          m_connected.store(false);
        });
    m_connecting.store(true);
  }

  auto Consumer::handle_reconnect() -> asio::awaitable<void>
  {
    auto ex = co_await asio::this_coro::executor;

    m_is_connected.store(false);
    if (m_connected.load())
      co_return;

    m_reconnect_count.fetch_add(1, std::memory_order_relaxed);

    mt_logging::logger().log(
        {fmt::format("Consumer receiver exited {} times, reconnecting in {} seconds...",
                     m_reconnect_count.load(),
                     CONNECTION_RETRY_DELAY),
         mt_logging::LogLevel::Info,
         true});

    // Cancel old connections
    if (m_conn_read)
      m_conn_read->cancel();
    if (m_conn_write)
      m_conn_write->cancel();

    // Recreate connections
    setup_connections(ex);

    // Wait until logger marks connection ready
    for (int i = 0; i < 50; i++) // 5 seconds
    {
      if (m_connected.load())
      {
        mt_logging::logger().log(
            {"Redis reconnected", mt_logging::LogLevel::Info, true});
        co_return;
      }

      co_await asio::steady_timer(ex, std::chrono::milliseconds(100)).async_wait(asio::use_awaitable);
    }

    mt_logging::logger().log(
        {"Redis reconnect timeout", mt_logging::LogLevel::Error, true});
    //    co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY)).async_wait(asio::use_awaitable);
  }

  std::shared_ptr<redis::connection> Consumer::make_connection(
      const boost::asio::any_io_executor &ex,
      bool use_ssl)
  {
    auto log_callback =
        [this](redis::logger::level lvl, std::string_view msg)
    {
      // Detect connection refused
      if (msg.find("Connection refused") != std::string::npos)
      {
        m_connected.store(false);
      }

      // Detect TLS handshake failure
      if (msg.find("handshake") != std::string::npos ||
          msg.find("certificate") != std::string::npos)
      {
        m_connected.store(false);
      }

      // Detect any Redis connection error
      if (msg.find("Failed to connect") != std::string::npos)
      {
        m_connected.store(false);
      }

      if (msg.find("Connected") != std::string::npos)
      {
        m_connected.store(true);
      }

      // Optional: log everything
      mt_logging::logger().log(
          {fmt::format("{} [Redis log] {}", m_connected.load(), msg),
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

  auto Consumer::run_consumer() -> asio::awaitable<void>
  {
    auto ex = co_await asio::this_coro::executor;

    for (;;)
    {
      if (m_signal_status.load())
      {
        co_return;
      }

      if (!m_connected.load())
      {
        mt_logging::logger().log(
            {"Redis not connected → reconnecting", mt_logging::LogLevel::Warn, true});
        co_await handle_reconnect();
        continue;
      }

      co_await asio::steady_timer(ex, std::chrono::milliseconds(200)).async_wait(asio::use_awaitable);

      bool error_occurred = false;
      try
      {
        co_await ensure_group_exists();
        co_await receiver(); // XREADGROUP
      }
      catch (const std::exception &e)
      {
        mt_logging::logger().log(
            {fmt::format("Redis consume error: {}", e.what()),
             mt_logging::LogLevel::Debug,
             true});

        // Mark connection down
        m_connected.store(false);
        error_occurred = true;
      }

      if (m_signal_status.load())
      {
        co_return;
      }

      if (error_occurred)
      {
        // reconnect OUTSIDE the catch
        co_await handle_reconnect();
        continue;
      }

      if (CONNECTION_RETRY_AMOUNT == -1)
        continue;
      if (m_reconnect_count.load() >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
    }

    co_return;
  }

  auto Consumer::co_main() -> asio::awaitable<void>
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
      setup_connections(ex);
      co_await run_consumer();
    }
    catch (const std::exception &e)
    {
      mt_logging::logger().log(
          {fmt::format("co_main error: {}", e.what()),
           mt_logging::LogLevel::Error,
           true});
      // m_signal_status.store(true);
      // m_awakener.stop();
    }
  }

  void Consumer::xack_now(std::string stream, std::string id)
  {
    if (m_signal_status.load())
      return;

    asio::dispatch(
        m_write_strand,
        [this, stream = std::move(stream), id = std::move(id)]() mutable
        {
          asio::co_spawn(
              m_write_strand,
              [this, stream = std::move(stream), id = std::move(id)]() mutable -> asio::awaitable<void>
              {
                co_await xack(stream, id);
              },
              asio::detached);
        });
  }

  std::future<boost::system::error_code>
  Consumer::xack_wait_now(std::string stream, std::string id)
  {
    auto p = std::make_shared<std::promise<boost::system::error_code>>();

    asio::dispatch(
        m_write_strand,
        [this, stream = std::move(stream), id = std::move(id), p]() mutable
        {
          asio::co_spawn(
              m_write_strand,
              [this, stream = std::move(stream), id = std::move(id), p]() mutable -> asio::awaitable<void>
              {
                auto ec = co_await xack_wait(stream, id);
                p->set_value(ec);
              },
              asio::detached);
        });

    return p->get_future();
  }

  void Consumer::xpending_oldest_now(std::string stream,
                                     std::string group,
                                     std::function<void(std::string)> callback)
  {
    if (m_signal_status.load())
      return;

    asio::dispatch(
        m_write_strand,
        [this,
         stream = std::move(stream),
         group = std::move(group),
         callback = std::move(callback)]() mutable
        {
          asio::co_spawn(
              m_write_strand,
              [this,
               stream = std::move(stream),
               group = std::move(group),
               callback = std::move(callback)]() mutable -> asio::awaitable<void>
              {
                co_await xpending_oldest(stream, group, callback);
              },
              asio::detached);
        });
  }

  void Consumer::send_to_dlq_now(std::string stream,
                                 std::string id,
                                 std::unordered_map<std::string, std::string> fields)
  {
    if (m_signal_status.load())
      return;

    asio::dispatch(
        m_write_strand,
        [this, stream = std::move(stream), id = std::move(id), fields = std::move(fields)]() mutable
        {
          asio::co_spawn(
              m_write_strand,
              [this, stream = std::move(stream), id = std::move(id), fields = std::move(fields)]() mutable -> asio::awaitable<void>
              {
                co_await send_to_dlq(stream, id, fields);
              },
              asio::detached);
        });
  }

  void Consumer::push_dlq_xadd(redis::request &req,
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

  asio::awaitable<void> Consumer::xack(std::string_view stream, std::string_view id)
  {
    redis::request req;
    req.push("XACK", stream, WORKER_GROUP, id);
    mt_logging::logger().log(
        {fmt::format("XACK'd work item:     [STREAM {}      ID {}]  WORKER GROUP {}", stream, id, WORKER_GROUP),
         mt_logging::LogLevel::Info,
         true});

    boost::system::error_code ec;
    co_await m_conn_write->async_exec(req, redis::ignore, asio::redirect_error(asio::use_awaitable, ec));

    if (ec)
    {
      mt_logging::logger().log(
          {fmt::format("XACK failed: {}", ec.message()),
           mt_logging::LogLevel::Error, true});
      co_return;
    }
  }

  asio::awaitable<boost::system::error_code> Consumer::xack_wait(std::string_view stream, std::string_view id)
  {
    redis::request req;
    req.push("XACK", stream, WORKER_GROUP, id);

    boost::system::error_code ec;
    redis::ignore_t ignore;
    mt_logging::logger().log(
        {fmt::format("XACK wait work item:  [STREAM {}      ID {}]  WORKER GROUP {}", stream, id, WORKER_GROUP),
         mt_logging::LogLevel::Info,
         true});

    co_await m_conn_write->async_exec(
        req,
        ignore,
        asio::redirect_error(asio::use_awaitable, ec));

    co_return ec;
  }

  asio::awaitable<void> Consumer::xpending_oldest(std::string_view stream,
                                                  std::string_view group,
                                                  std::function<void(std::string)> callback)
  {
    redis::request req;
    req.push("XPENDING", stream, group, "-", "+", "1");

    redis::generic_response resp;
    boost::system::error_code ec;

    co_await m_conn_write->async_exec(
        req,
        resp,
        asio::redirect_error(asio::use_awaitable, ec));

    if (ec)
    {
      callback(""); // no oldest xid
      co_return;
    }

    auto pendings = parse_xpending(resp);
    if (pendings.empty())
    {
      callback(""); // no oldest xid
      co_return;
    }

    std::string oldest_xid;
    auto &p = pendings.at(0);
    mt_logging::logger().log(
        {fmt::format(" - Oldest pending ID: {} consumer={} idle={} deliveries={}",
                     p.id, p.consumer, p.idle_ms, p.delivery_count),
         mt_logging::LogLevel::Debug,
         true});
    oldest_xid = p.id;
    callback(oldest_xid);
  }

  asio::awaitable<void> Consumer::send_to_dlq(std::string_view stream, std::string_view id,
                                              const std::unordered_map<std::string, std::string> &fields)
  {
    redis::request req;
    push_dlq_xadd(req, std::string(stream) + ".DLQ", id, fields);
    mt_logging::logger().log(
        {fmt::format("DLQ'd work item:      [STREAM {}      ID {}]  WORKER GROUP {}", std::string(stream) + ".DLQ", id, WORKER_GROUP),
         mt_logging::LogLevel::Info, true});

    boost::system::error_code ec;
    co_await m_conn_write->async_exec(req, redis::ignore, asio::redirect_error(asio::use_awaitable, ec));
    if (ec)
    {
      mt_logging::logger().log(
          {fmt::format("DLQ XADD failed: {}", ec.message()),
           mt_logging::LogLevel::Error, true});
      co_return;
    }

    // Remove from PEL
    co_await xack(stream, id);
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif
