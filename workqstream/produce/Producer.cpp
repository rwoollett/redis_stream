
#include "Producer.h"
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

  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const char *REDIS_CHANNEL = std::getenv("REDIS_CHANNEL");
  static const char *REDIS_SERVICE_GROUP = std::getenv("REDIS_SERVICE_GROUP");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 3;

  std::list<std::string> splitByComma(const char *str)
  {
    std::list<std::string> result;
    if (str == nullptr)
    {
      return result; // Return an empty list if the input is null
    }

    std::stringstream ss(str);
    std::string token;

    // Split the string by commas
    while (std::getline(ss, token, ','))
    {
      result.push_back(token);
    }

    return result;
  }

#if defined(BOOST_ASIO_HAS_CO_AWAIT)

  auto verify_certificate(bool, asio::ssl::verify_context&) -> bool
  {
    std::cout << "set_verify_callback" << std::endl;
    return true;
  }
  // Helper to load a file into an SSL context
  void load_certificates(asio::ssl::context& ctx,
                        const std::string& ca_file,
                        const std::string& cert_file,
                        const std::string& key_file)
  {
    try 
    {
      // Load trusted CA
      ctx.load_verify_file(ca_file);

      // Load client certificate
      ctx.use_certificate_file(cert_file, asio::ssl::context::pem);

      // Load private key
      ctx.use_private_key_file(key_file, asio::ssl::context::pem);
    } catch(const std::exception &e) 
    {
      std::cerr << "Producer::load certiciates " << e.what() << std::endl;
    }
  }

  Producer::Producer() : m_ioc{2},
                       m_conn{},
                       msg_queue{},
                       m_signalStatus{0},
                       m_isConnected{0},
                       m_sender_thread{}
  {
    D(std::cerr << "Produce created\n";)
    if (REDIS_HOST == nullptr || REDIS_PORT == nullptr || REDIS_CHANNEL == nullptr || REDIS_SERVICE_GROUP == nullptr || REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, REDIS_SERVICE_GROUP, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }

    asio::co_spawn(m_ioc.get_executor(), Producer::co_main(), asio::detached);


    m_sender_thread = std::thread([this]()
                                  { m_ioc.run(); });
  }

  Producer::~Producer()
  {
    D(std::cerr << "Redis Producer  destroying\n";)
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
    std::cerr << "Redis Produceer destroyed\n";
  }

  void Producer::enqueue_message(
    const std::string &channel, 
    const std::vector<std::pair<std::string,std::string>> &fields)
  {
    if (m_signalStatus == 1)
      return;

    ProduceMessage msg;
    std::strncpy(msg.channel, channel.c_str(), CHANNEL_LENGTH - 1);
    msg.channel[CHANNEL_LENGTH - 1] = '\0'; // Always null-terminate

    msg.field_count = fields.size();
    int i = 0;
    for (const auto& [in_field, in_value] : fields) {
      std::cout << in_field << " " << in_value << " sizes " << in_field.size() << ", " << in_value.size() << std::endl;
      strncpy(msg.fields[i].field, in_field.c_str(), FIELD_NAME_LENGTH - 1); 
      msg.fields[i].field[FIELD_NAME_LENGTH - 1] = '\0';
      strncpy(msg.fields[i].value, in_value.c_str(), FIELD_VALUE_LENGTH - 1);
      msg.fields[i].value[FIELD_VALUE_LENGTH - 1] = '\0';
      i++;
    }

    cstokenQueuedCount++;
    msg_queue.push(msg);
  }

  void push_xadd(redis::request& req,
               const std::string& stream,
               const ProduceMessage& m)
  {
      std::vector<std::string> args;
      args.reserve(2 + m.field_count * 2);

      args.push_back(stream);
      args.push_back("*");

      for (int i = 0; i < m.field_count; ++i) {
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
    {
      std::list<std::string> channels = splitByComma(REDIS_CHANNEL);
      // Print the result
      for (const auto &channel : channels)
      {
        std::cout << "REDIS channel stream: " << channel << std::endl;
        redis::request req;
        req.push("XGROUP", "CREATE",
                channel,                // stream name
                REDIS_SERVICE_GROUP,    // group name
                "$",                    // start at new messages
                "MKSTREAM");            // create stream if missing

        redis::response<std::string> resp;

        boost::system::error_code ec;
        co_await m_conn->async_exec(req, resp,
            asio::redirect_error(asio::use_awaitable, ec));

        if (ec) {
          std::string msg = ec.message();
          // Ignore BUSYGROUP (group already exists)
          if (msg.find("BUSYGROUP") == std::string::npos) {
              std::cerr << "XGROUP CREATE failed: " << msg << std::endl;
          } else {
              std::cout << "XGROUP already exists, continuing\n";
          }
        } else {
            std::cout << "XGROUP prerender_group created successfully\n";
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
          cstokenMessageCount++;
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
          //req.push("Produce", "csToken_request", "my message");
          //req.push("XADD", "csToken_request", "*", "postid", "1234", "postname", "test"); // THIS IS GOOD

          redis::request req;
          push_xadd(req, m.channel, m);

          redis::response<std::string> resp;
          req.get_config().cancel_if_not_connected = true;
          co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

          if (ec)
          {
            std::cout << "Perform a full reconnect to redis. Reason for error: " << ec.message() << std::endl;
            for (const auto &m : batch)
            {
              msg_queue.push(m);
              cstokenMessageCount--;
            }
            co_return; // Connection lost, exit function and try reconnect to redis.
          }

          cstokenWorkCount++;

          std::string XID = std::get<0>(resp).value();
          std::cout << "XADD ID: " << XID << std::endl;

          std::cout
                << "Redis Produce: " << " batch size: " << batch.size() << ". "
                << cstokenQueuedCount << " queued, "
                << cstokenMessageCount << " sent, "
                << cstokenWorkCount << " Produceed. "
                //<< cstokenSuccessCount << " successful subscribes made. "
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

      } else {
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
      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
          .async_wait(asio::use_awaitable);

      m_conn->cancel();

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