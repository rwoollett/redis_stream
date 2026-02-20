
#include "Producer.h"
#include <workqstream/common/Common.h>
#include <memory>
#include <fstream>
#include <sstream>
#include <string>
#include <tuple>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/redis/response.hpp>
#include <boost/redis/request.hpp>

namespace WorkQStream
{

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
    std::cout << "set_verify_callback" << std::endl;
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
      std::cerr << "Producer::load certiciates " << e.what() << std::endl;
    }
  }

  Producer::Producer() : m_ioc{2},
                         m_conn{},
                         msg_queue{},
                         m_signalStatus{0},
                         m_isConnected{0},
                         m_sender_thread{},
                         m_group_config(load_group_config()),
                         m_validStreams{}
  {
    D(std::cerr << "Produce created\n";)
    if (REDIS_GROUP_CONFIG == nullptr ||
        REDIS_HOST == nullptr || REDIS_PORT == nullptr ||
        REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_GROUP_CONFIG, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }

    for (const auto &[groupName, cfg] : m_group_config)
    {
      for (const auto &s : cfg.streams)
      {
        if (s.empty())
          throw std::runtime_error("Stream name cannot be empty");
        if (s.find(' ') != std::string::npos)
          throw std::runtime_error("Stream name cannot contain spaces: " + s);
        m_validStreams.insert(s);
      }
    }
    asio::co_spawn(m_ioc.get_executor(), Producer::co_main(), asio::detached);

    m_sender_thread = std::thread([this]()
                                  { m_ioc.run(); });
  }

  Producer::~Producer()
  {
    ProduceMessage msg;
    int countMsg = 0;
    while (!msg_queue.empty())
    {
      if (m_isConnected == 0)
      {
        // Exited because of no redis connection so empty out msg_queue
        msg_queue.pop(msg);
        countMsg++;
      }
      else
      {
        D(std::cout << "Redis Produceer destructor found msg.\n";)
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
      }
    };
    if (m_isConnected == 0)
    {
      std::cout << "Redis Produceer found not connected to redis: " << countMsg << " messages deleted\n";
    }

    m_ioc.stop();
    if (m_sender_thread.joinable())
      m_sender_thread.join();
    std::cerr << "Redis Producer destroyed\n";
  }

  void Producer::enqueue_message(
      const std::string &channel,
      const std::vector<std::pair<std::string, std::string>> &fields)
  {
    if (m_signalStatus == 1)
      return;

    validate_stream_or_throw(channel, m_validStreams, "(n/a)");

    ProduceMessage msg;
    std::strncpy(msg.channel, channel.c_str(), CHANNEL_LENGTH - 1);
    msg.channel[CHANNEL_LENGTH - 1] = '\0'; // Always null-terminate

    msg.field_count = fields.size();
    int i = 0;
    for (const auto &[in_field, in_value] : fields)
    {
      std::cout << in_field << " " << in_value << " sizes " << in_field.size() << ", " << in_value.size() << std::endl;
      strncpy(msg.fields[i].field, in_field.c_str(), FIELD_NAME_LENGTH - 1);
      msg.fields[i].field[FIELD_NAME_LENGTH - 1] = '\0';
      strncpy(msg.fields[i].value, in_value.c_str(), FIELD_VALUE_LENGTH - 1);
      msg.fields[i].value[FIELD_VALUE_LENGTH - 1] = '\0';
      i++;
    }

    messageQueuedCount++;
    msg_queue.push(msg);
  }

  void push_xadd(redis::request &req,
                 const std::string &stream,
                 const ProduceMessage &m)
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
      m_isConnected = 0;
      std::cout << make_ops_error(
                       "PING", "(n/a)",
                       "(n/a)", "(n/a)",
                       ec.message(),
                       "Check Redis connectivity and authentication")
                << std::endl;
      D(std::cout << "PING unsuccessful\n";)
      co_return; // Connection lost, break so we can exit function and try reconnect to redis.
    }
    else
    {
      D(std::cout << "PING successful\n";)
    }

    // ------------------------------------------------------------
    // Ensure the consumer group exists (idempotent)
    // ------------------------------------------------------------
    for (const auto &[groupName, cfg] : m_group_config)
    {
      for (const auto &stream : cfg.streams)
      {

        std::cout << "Ensuring group '" << groupName
                  << "' exists on stream '" << stream << "'\n";

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
            std::cerr << "XGROUP CREATE failed: " << msg << "\n";
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
            std::cout << "Group already exists, continuing\n";
          }
        }
        else
        {
          std::cout << "Group created successfully\n";
        }
      }
    }

    m_isConnected = 1;
    m_reconnectCount = 0; // reset
    for (boost::system::error_code ec;;)
    {
      std::vector<ProduceMessage> batch;
      ProduceMessage msg;
      while (msg_queue.pop(msg))
      {
        if (batch.size() < BATCH_SIZE)
        {
          batch.push_back(msg);
          messageCount++;
        }
        else
        {
          msg_queue.push(msg);
          break; // exit while
        }
      }

      if (!batch.empty())
      {
        std::cout << "Amount batched " << batch.size() << std::endl;
        for (const auto &m : batch)
        {
          redis::request req;
          push_xadd(req, m.channel, m);

          redis::response<std::string> resp;
          req.get_config().cancel_if_not_connected = true;
          co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

          if (ec)
          {
            std::cout << "Perform a full reconnect to redis. Reason for error: "
                      << make_ops_error(
                             "XADD", m.channel,
                             "(n/a)", "(n/a)",
                             ec.message(),
                             "Check Redis connectivity and authentication")
                      << std::endl;

            for (const auto &m : batch)
            {
              msg_queue.push(m);
              messageCount--;
            }

            co_return; // Connection lost, exit function and try reconnect to redis.
          }

          messageSuccessCount++;

          std::string XID = std::get<0>(resp).value();
          std::cout << "XADD ID: " << XID << std::endl;

          std::cout
              << "Redis Produce: " << " batch size: " << batch.size() << ". "
              << messageQueuedCount << " queued, "
              << messageCount << " sent, "
              << messageSuccessCount << " Produceed. "
              << std::endl;
        }
      }
      else
      {
        co_await asio::steady_timer(co_await asio::this_coro::executor, std::chrono::milliseconds(100)).async_wait(asio::use_awaitable);
      }
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
      std::cout << "Configure ssl\n";
      cfg.use_ssl = true;
      // DONOT disable health check:
      //cfg.health_check_interval = std::chrono::seconds(0); // set 0 for tls friendly
    }

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif // defined(SIGQUIT)
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          m_signalStatus = 1;
        });

    for (;;)
    {
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
        std::cerr << "Redis Produce error: " << e.what() << std::endl;
      }

      // Delay before reconnecting
      m_reconnectCount++;
      std::cout << "Producer process messages exited " << m_reconnectCount << " times, reconnecting in "
                << CONNECTION_RETRY_DELAY << " second..." << std::endl;

      m_conn->cancel();

      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY)).async_wait(asio::use_awaitable);
      std::cout << "Timer done." << std::endl;

      if (CONNECTION_RETRY_AMOUNT == -1)
        continue;
      if (m_reconnectCount >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
    }
    m_signalStatus = 1;
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif