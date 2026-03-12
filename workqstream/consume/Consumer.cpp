
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
#include "ParseRedisResp.h"

namespace WorkQStream
{

  static const char *WORKER_GROUP = std::getenv("WORKER_GROUP");
  static const char *WORKER_RECOVER_PENDING = std::getenv("WORKER_RECOVER_PENDING");
  static const char *REDIS_STREAM_CONSUMER_LOGFILE = std::getenv("REDIS_STREAM_CONSUMER_LOGFILE");
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
        {REDIS_STREAM_CONSUMER_LOGFILE,
         fmt::format("Broadcast work item message \n STREAM: {}\n    ID: {}\n", stream_name, message_id, fmt::join(fields, ", ")),
         std::ios::app,
         true});
  }

  std::unordered_map<std::string, std::string> convert_fields(const DispatchView &item)
  {
    std::unordered_map<std::string, std::string> field_map;
    field_map.reserve(item.fields.size());
    for (auto &[k, v] : item.fields)
      field_map.emplace(std::string(k), std::string(v));
    return field_map;
  }

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
          {REDIS_STREAM_CONSUMER_LOGFILE,
           fmt::format("Consumer::load certiciates {}", e.what()),
           std::ios::app,
           true});
    }
  }

  Consumer::Consumer(
      const std::string &workerId,
      Awakener &awakener) : m_ioc{3},
                            m_awakener(awakener),
                            m_conn_read{},
                            m_conn_write{},
                            m_worker_id(workerId),
                            m_group_config(load_group_config()),
                            m_valid_streams{}
  {
    if (REDIS_STREAM_CONSUMER_LOGFILE == nullptr ||
        REDIS_HOST == nullptr || REDIS_PORT == nullptr ||
        REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr ||
        WORKER_RECOVER_PENDING == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_STREAM_CONSUMER_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, WORKER_RECOVER_PENDING and REDIS_USE_SSL must be set.");
    }

    m_is_connected.store(false);
    m_signal_status.store(false);
    m_cstoken_message_count.store(0);

    mt_logging::logger().log(
        {REDIS_STREAM_CONSUMER_LOGFILE,
         "Redis Consumer created",
         std::ios::app,
         true});

    for (const auto &s : get_worker_group(m_group_config).streams)
    {
      if (s.empty())
        throw std::runtime_error("Stream name cannot be empty");
      if (s.find(' ') != std::string::npos)
        throw std::runtime_error("Stream name cannot contain spaces: " + s);

      m_valid_streams.insert(s);
    }

    asio::co_spawn(m_ioc.get_executor(), Consumer::co_main(), asio::detached);
    m_receiver_thread = std::thread([this]()
                                    { m_ioc.run(); });
  }

  Consumer::~Consumer()
  {
    request_stop();
    join();
    mt_logging::logger().log({REDIS_STREAM_CONSUMER_LOGFILE,
                              "Redis Consumer destroyed",
                              std::ios::app,
                              true});
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
      boost::asio::post(m_ioc, [conn = m_conn_write]
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
          {REDIS_STREAM_CONSUMER_LOGFILE,
           fmt::format("Ensuring group {} exists on stream {}", WORKER_GROUP, stream),
           std::ios::app,
           true});

      redis::request req;
      req.push("XGROUP", "CREATE",
               stream,
               WORKER_GROUP,
               "0",
               "MKSTREAM");
      req.get_config().cancel_if_not_connected = true;

      redis::response<std::string> resp;
      boost::system::error_code ec;
      co_await m_conn_write->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));
      if (ec)
      {
        std::string msg = ec.message();
        if (msg.find("BUSYGROUP") == std::string::npos)
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
        else
        {
          mt_logging::logger().log(
              {REDIS_STREAM_CONSUMER_LOGFILE,
               "Group already exists, continuing",
               std::ios::app,
               true});
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

      mt_logging::logger().log(
          {REDIS_STREAM_CONSUMER_LOGFILE,
           fmt::format(" {} consumed. XID {}", m_cstoken_message_count.load(), item.id),
           std::ios::app,
           true});

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

    req.get_config().cancel_if_not_connected = false;
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
              {REDIS_STREAM_CONSUMER_LOGFILE,
               fmt::format(
                   "- Consumer::receiver operation_aborted {} {}", ec.message(), ec.value()),
               std::ios::app,
               true});

          co_return; // true; // false; // do not reconnect this ec
        }

        mt_logging::logger().log(
            {REDIS_STREAM_CONSUMER_LOGFILE,
             fmt::format(
                 "Perform a full reconnect to redis. Reason for error: {}",
                 make_ops_error(
                     "XREADGROUP", "(n/a)",
                     WORKER_GROUP, m_worker_id,
                     ec.message(),
                     "Check Redis connectivity and authentication")),
             std::ios::app,
             true});

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

  auto Consumer::co_main() -> asio::awaitable<void>
  {
    auto ex = co_await asio::this_coro::executor;
    redis::config cfg;
    cfg.addr.host = REDIS_HOST;
    cfg.addr.port = REDIS_PORT;
    cfg.password = REDIS_PASSWORD;
    if (std::string(REDIS_USE_SSL) == "on")
    {
      cfg.use_ssl = true;
      // disable health check:
      cfg.health_check_interval = std::chrono::seconds(0); // set 0 for tls friendly
    }

    mt_logging::logger().log(
        {REDIS_STREAM_CONSUMER_LOGFILE,
         fmt::format("Worker id:  {}", m_worker_id),
         std::ios::app,
         true});

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif
    sig_set.async_wait(
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

    // bool should_reconnect = false;
    for (;;)
    {
      if (m_signal_status.load())
      {
        co_return;
      }
      if (std::string(REDIS_USE_SSL) == "on")
      {
        asio::ssl::context ssl_ctx_read{asio::ssl::context::tlsv12_client};
        asio::ssl::context ssl_ctx_write{asio::ssl::context::tlsv12_client};
        ssl_ctx_read.set_verify_mode(asio::ssl::verify_peer);
        ssl_ctx_write.set_verify_mode(asio::ssl::verify_peer);
        load_certificates(ssl_ctx_read, "tls/ca.crt", /** Your self-signed CA*/ "tls/redis.crt", /** Your client certificate*/ "tls/redis.key" /** Your private key */);
        load_certificates(ssl_ctx_write, "tls/ca.crt", /** Your self-signed CA*/ "tls/redis.crt", /** Your client certificate*/ "tls/redis.key" /** Your private key */);
        ssl_ctx_read.set_verify_callback(verify_certificate);
        ssl_ctx_write.set_verify_callback(verify_certificate);
        m_conn_read = std::make_shared<redis::connection>(ex, std::move(ssl_ctx_read));
        m_conn_write = std::make_shared<redis::connection>(ex, std::move(ssl_ctx_write));
      }
      else
      {
        m_conn_read = std::make_shared<redis::connection>(ex);
        m_conn_write = std::make_shared<redis::connection>(ex);
      }

      m_conn_read->async_run(
          cfg,
          redis::logger{redis::logger::level::err},
          [self = m_conn_read](boost::system::error_code ec)
          {
            mt_logging::logger().log(
                {REDIS_STREAM_CONSUMER_LOGFILE,
                 fmt::format("[m_conn_read async_run] ended: {}", ec.message()),
                 std::ios::app,
                 true});
          });
      m_conn_write->async_run(
          cfg,
          redis::logger{redis::logger::level::err},
          [self = m_conn_write](boost::system::error_code ec)
          {
            mt_logging::logger().log(
                {REDIS_STREAM_CONSUMER_LOGFILE,
                 fmt::format("[m_conn_write async_run] ended: {}", ec.message()),
                 std::ios::app,
                 true});
          });

      if (std::string(WORKER_RECOVER_PENDING) == "on")
      {
        for (const auto &stream : m_valid_streams)
        {
          asio::co_spawn(
              m_ioc,
              recover_pending(stream),
              asio::detached);
          asio::co_spawn(
              m_ioc,
              trim_stream(stream),
              asio::detached);
        }
      }

      co_await asio::steady_timer(co_await asio::this_coro::executor, std::chrono::milliseconds(500)).async_wait(asio::use_awaitable);

      try
      {
        co_await ensure_group_exists();
        co_await receiver();
      }
      catch (const std::exception &e)
      {
        mt_logging::logger().log(
            {REDIS_STREAM_CONSUMER_LOGFILE,
             fmt::format("Redis consume co_main error: {}", e.what()),
             std::ios::app,
             true});
      }
      if (m_signal_status.load())
      {
        co_return;
      }

      m_is_connected.store(false);
      m_reconnect_count.fetch_add(1, std::memory_order_relaxed);

      mt_logging::logger().log(
          {REDIS_STREAM_CONSUMER_LOGFILE,
           fmt::format("Consumer receiver exited {} times, reconnecting in {} seconds...",
                       m_reconnect_count.load(),
                       CONNECTION_RETRY_DELAY),
           std::ios::app,
           true});

      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY)).async_wait(asio::use_awaitable);

      if (CONNECTION_RETRY_AMOUNT == -1)
        continue;
      if (m_reconnect_count.load() >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
    }
    m_signal_status.store(true);
    m_awakener.stop();
  }

  void Consumer::xack_now(std::string stream, std::string id)
  {
    if (m_signal_status.load())
      return;
    asio::co_spawn(
        m_ioc,
        [this, stream = std::move(stream), id = std::move(id)]() mutable -> asio::awaitable<void>
        {
          co_await xack(stream, id);
        },
        asio::detached);
  }

  void Consumer::send_to_dlq_now(std::string stream,
                                 std::string id,
                                 std::unordered_map<std::string, std::string> fields)
  {
    if (m_signal_status.load())
      return;
    asio::co_spawn(
        m_ioc,
        [this, stream = std::move(stream), id = std::move(id), fields = std::move(fields)]() mutable -> asio::awaitable<void>
        {
          co_await send_to_dlq(stream, id, fields);
        },
        asio::detached);
  }

  void push_dlq_xadd(redis::request &req,
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
        {REDIS_STREAM_CONSUMER_LOGFILE,
         fmt::format("XACK'd work item:     [STREAM {}      ID {}]  WORKER GROUP {}", stream, id, WORKER_GROUP),
         std::ios::app,
         true});

    co_await m_conn_write->async_exec(req, redis::ignore, asio::use_awaitable);
  }

  asio::awaitable<void> Consumer::send_to_dlq(std::string_view stream, std::string_view id,
                                              const std::unordered_map<std::string, std::string> &fields)
  {
    redis::request req;
    push_dlq_xadd(req, std::string(stream) + ".DLQ", id, fields);
    mt_logging::logger().log(
        {REDIS_STREAM_CONSUMER_LOGFILE,
         fmt::format("DLQ'd work item:      [STREAM {}      ID {}]  WORKER GROUP {}", std::string(stream) + ".DLQ", id, WORKER_GROUP),
         std::ios::app,
         true});

    co_await m_conn_write->async_exec(req, redis::ignore, asio::use_awaitable);

    // Remove from PEL
    co_await xack(stream, id);
  }

  asio::awaitable<void> Consumer::recover_pending(std::string stream)
  {
    auto ex = co_await asio::this_coro::executor;

    while (!m_signal_status.load())
    {
      try
      {
        // 1. Get up to 10 pending messages
        mt_logging::logger().log(
            {REDIS_STREAM_CONSUMER_LOGFILE,
             fmt::format("XPENDING data         [STREAM {}      WORKER GROUP {}]", stream, WORKER_GROUP),
             std::ios::app,
             true});

        redis::request req;
        req.push("XPENDING", stream, std::string(WORKER_GROUP), "-", "+", "10");

        redis::generic_response resp;
        co_await m_conn_write->async_exec(req, resp, asio::use_awaitable);
        auto pendings = parse_xpending(resp);

        if (pendings.empty())
        {
          co_await asio::steady_timer(ex, std::chrono::seconds(RECOVER_PENDING_DELAY)).async_wait(asio::use_awaitable);
          continue;
        }

        for (auto &p : pendings)
        {
          mt_logging::logger().log(
              {REDIS_STREAM_CONSUMER_LOGFILE,
               fmt::format(" - Pending ID: {} consumer={} idle={} deliveries={}", p.id, p.consumer, p.idle_ms, p.delivery_count),
               std::ios::app,
               true});
          // Optional: DLQ logic
          if (p.delivery_count > 5)
          {
            co_await send_to_dlq(stream, p.id, {/** field are missing can be found with XREADGROUP see later */});
            continue;
          }

          // 2. Claim the message
          redis::request claim;
          claim.push("XCLAIM", stream, WORKER_GROUP, m_worker_id, "0", p.id);

          redis::generic_response claim_resp;
          co_await m_conn_write->async_exec(claim, claim_resp, asio::use_awaitable);

          // 3. Fetch the message fields
          redis::request read_req;
          read_req.push("XREADGROUP", "GROUP", WORKER_GROUP, m_worker_id, "STREAMS", stream, p.id);

          redis::generic_response read_resp;
          co_await m_conn_write->async_exec(read_req, read_resp, asio::use_awaitable);

          auto items = parse_dispatch_view(read_resp);

          for (auto &item : items)
          {
            mt_logging::logger().log(
                {REDIS_STREAM_CONSUMER_LOGFILE,
                 fmt::format("XCLAIMED message:     [STREAM {}      ID {}]  Fields: {}", item.stream, item.id, fmt::join(item.fields, " = ")),
                 std::ios::app,
                 true});

            // 4. Push into your Awakener queue
            m_awakener.broadcast_single(
                std::string(item.stream),
                std::string(item.id),
                std::move(convert_fields(item)));
          }
        }
      }
      catch (std::exception &e)
      {
        mt_logging::logger().log(
            {REDIS_STREAM_CONSUMER_LOGFILE,
             fmt::format("XPENDING recovery     error: {}", e.what()),
             std::ios::app,
             true});
      }

      if (m_signal_status.load())
        co_return;

      // Sleep before next scan
      boost::system::error_code ec;
      co_await asio::steady_timer(ex, std::chrono::seconds(RECOVER_PENDING_DELAY))
          .async_wait(asio::redirect_error(asio::use_awaitable, ec));

      if (ec == asio::error::operation_aborted || m_signal_status.load())
        co_return;
    }
  }

  asio::awaitable<void> Consumer::trim_stream(std::string stream)
  {
    auto ex = co_await asio::this_coro::executor;

    while (!m_signal_status.load())
    {
      try
      {
        redis::request req;
        mt_logging::logger().log(
            {REDIS_STREAM_CONSUMER_LOGFILE,
             fmt::format("XTRIM data            [STREAM {}      WORKER GROUP {}]", stream, WORKER_GROUP),
             std::ios::app,
             true});

        req.push("XTRIM", stream, "MAXLEN", "~", std::to_string(TRIM_STREAM_SIZE));

        co_await m_conn_write->async_exec(req, redis::ignore, asio::use_awaitable);
      }
      catch (std::exception &e)
      {
        mt_logging::logger().log(
            {REDIS_STREAM_CONSUMER_LOGFILE,
             fmt::format("XTRIM recovery        error: {}", e.what()),
             std::ios::app,
             true});
      }

      if (m_signal_status.load())
        co_return;

      boost::system::error_code ec;
      co_await asio::steady_timer(ex, std::chrono::seconds(TRIM_STREAM_DELAY))
          .async_wait(asio::redirect_error(asio::use_awaitable, ec));

      if (ec == asio::error::operation_aborted || m_signal_status.load())
        co_return;
    }
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif
