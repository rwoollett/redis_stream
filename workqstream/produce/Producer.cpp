
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

namespace WorkQStream
{

  static const char *REDIS_STREAM_PRODUCER_LOGFILE = std::getenv("REDIS_STREAM_PRODUCER_LOGFILE");
  static const char *REDIS_GROUP_CONFIG = std::getenv("REDIS_GROUP_CONFIG");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 10;

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
          {REDIS_STREAM_PRODUCER_LOGFILE,
           fmt::format("Producer::load certiciates {}", e.what()),
           std::ios::app,
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
        REDIS_STREAM_PRODUCER_LOGFILE == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_STREAM_PRODUCER_LOGFILE, REDIS_GROUP_CONFIG, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }

    MESSAGE_QUEUED_COUNT.store(0);
    MESSAGE_COUNT.store(0);
    MESSAGE_SUCCESS_COUNT.store(0);

    m_is_connected.store(false);
    m_signal_status.store(false);
    m_shutting_down.store(false);
    mt_logging::logger().log(
        {REDIS_STREAM_PRODUCER_LOGFILE,
         "Producer created",
         std::ios::app,
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
    asio::co_spawn(m_ioc.get_executor(), Producer::co_main(), asio::detached);

    m_sender_thread = std::thread([this]()
                                  { m_ioc.run(); });
  }
  Producer::~Producer()
  {
    m_shutting_down.store(true);
    if (m_conn)
    {
      // Schedule cancel on the io_context thread
      boost::asio::post(m_ioc, [conn = m_conn]
                        { conn->cancel(); });
    }

    boost::asio::post(m_ioc, [this]
                      { msg_queue.shutdown(); });

    msg_queue.push(ProduceMessage{}); // dummy wake-up on lockfree queue 

    if (m_sender_thread.joinable())
      m_sender_thread.join();

    mt_logging::logger().log({REDIS_STREAM_PRODUCER_LOGFILE,
                              "Redis Producer destroyed",
                              std::ios::app,
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

  asio::awaitable<void> Producer::process_messages()
  {
    boost::system::error_code ec;
    redis::request ping_req;
    ping_req.push("PING");

    co_await m_conn->async_exec(ping_req, boost::redis::ignore, asio::redirect_error(asio::deferred, ec));
    if (ec)
    {
      m_is_connected.store(false);
      mt_logging::logger().log(
          {REDIS_STREAM_PRODUCER_LOGFILE,
           make_ops_error(
               "PING", "(n/a)",
               "(n/a)", "(n/a)",
               ec.message(),
               "Check Redis connectivity and authentication"),
           std::ios::app,
           true});
      co_return; // Connection lost, break so we can exit function and try reconnect to redis.
    }

    // ------------------------------------------------------------
    // Ensure the consumer group exists (idempotent)
    // ------------------------------------------------------------
    for (const auto &[groupName, cfg] : m_group_config)
    {
      for (const auto &stream : cfg.streams)
      {
        mt_logging::logger().log(
            {REDIS_STREAM_PRODUCER_LOGFILE,
             fmt::format("Ensuring group {} exists on stream {}", groupName, stream),
             std::ios::app,
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
                {REDIS_STREAM_PRODUCER_LOGFILE,
                 "Group already exists, continuing",
                 std::ios::app,
                 true});
          }
        }
      }
    }

    m_is_connected.store(true);
    m_reconnect_count.store(0); // reset
    for (boost::system::error_code ec;;)
    {
      if (m_shutting_down.load())
        break;

      std::vector<ProduceMessage> batch;
      ProduceMessage msg;
      while (batch.size() < BATCH_SIZE && msg_queue.blocking_pop(msg) )
      {
        if (m_shutting_down.load())
          break;
        batch.push_back(msg);
        MESSAGE_COUNT.fetch_add(1, std::memory_order_relaxed);
      }

      if (m_shutting_down.load())
        break;

      if (batch.empty())
        continue;

      for (const auto &m : batch)
      {
        redis::request req;
        push_xadd(req, m.channel, m);

        redis::response<std::string> resp;
        req.get_config().cancel_if_not_connected = true;
        co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

        if (ec)
        {
          mt_logging::logger().log(
              {REDIS_STREAM_PRODUCER_LOGFILE,
               fmt::format(
                   "Perform a full reconnect to redis. Reason for error: {}",
                   make_ops_error(
                       "XADD", m.channel,
                       "(n/a)", "(n/a)",
                       ec.message(),
                       "Check Redis connectivity and authentication")),
               std::ios::app,
               true});

          for (const auto &m : batch)
          {
            msg_queue.push(m);
            MESSAGE_COUNT.fetch_sub(1, std::memory_order_relaxed);
          }

          co_return; // Connection lost, exit function and try reconnect to redis.
        }

        MESSAGE_SUCCESS_COUNT.fetch_add(1, std::memory_order_relaxed);

        std::string XID = std::get<0>(resp).value();
        D(mt_logging::logger().log(
            {REDIS_STREAM_PRODUCER_LOGFILE,
             fmt::format("Messages queued: {}, messages XADD'ed {}", MESSAGE_QUEUED_COUNT.load(), MESSAGE_COUNT.load()),
             std::ios::app,
             true});)
        mt_logging::logger().log(
            {REDIS_STREAM_PRODUCER_LOGFILE,
             fmt::format("Redis Produce: {} produced. XID {}", MESSAGE_SUCCESS_COUNT.load(), XID),
             std::ios::app,
             true});
      }
    }
    // Drop all remaining messages on shutdown
    if (m_shutting_down.load())
    {
      ProduceMessage leftover;
      int dropped = 0;
      while (msg_queue.pop(leftover))
      {
        dropped++;
      }

      mt_logging::logger().log({REDIS_STREAM_PRODUCER_LOGFILE,
                                fmt::format("Producer shutdown: dropped {} pending messages", dropped),
                                std::ios::app,
                                true});
    }
  }

  auto Producer::co_main() -> asio::awaitable<void>
  {
    auto ex = co_await asio::this_coro::executor;
    redis::config cfg;
    cfg.addr.host = REDIS_HOST;
    cfg.addr.port = REDIS_PORT;
    cfg.password = REDIS_PASSWORD;
    if (std::string(REDIS_USE_SSL) == "on")
    {
      cfg.use_ssl = true;
      // DONOT disable health check
    }

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif // defined(SIGQUIT)
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          m_signal_status.store(true);
        });

    for (;;)
    {
      if (m_shutting_down.load())
      {
        co_return;
      }
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

      m_conn->async_run(cfg, redis::logger{redis::logger::level::err}, asio::consign(asio::detached, m_conn));

      try
      {
        co_await process_messages();
      }
      catch (const std::exception &e)
      {
        mt_logging::logger().log(
            {REDIS_STREAM_PRODUCER_LOGFILE,
             fmt::format("Redis Produce error: {}", e.what()),
             std::ios::app,
             true});
      }

      if (m_shutting_down.load())
      {
        co_return;
      }

      // Delay before reconnecting
      m_reconnect_count.fetch_add(1, std::memory_order_relaxed);
      mt_logging::logger().log(
          {REDIS_STREAM_PRODUCER_LOGFILE,
           fmt::format("Producer process messages exited {} times, reconnecting in {} seconds...",
                       m_reconnect_count.load(),
                       CONNECTION_RETRY_DELAY),
           std::ios::app,
           true});

      m_conn->cancel();

      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY)).async_wait(asio::use_awaitable);

      if (CONNECTION_RETRY_AMOUNT == -1)
        continue;
      if (m_reconnect_count.load() >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
    }
    m_signal_status.store(true);
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif