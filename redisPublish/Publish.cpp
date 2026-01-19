
#include "Publish.h"
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

namespace RedisPublish
{

  // class Exception : public std::exception
  // {
  // protected:
  //   /** Error message.
  //    */
  //   std::string msg_;

  // public:
  //   explicit Exception(const char *message) : msg_(message)
  //   {
  //   }

  //   /** Constructor (C++ STL strings).
  //    *  @param message The error message.
  //    */
  //   explicit Exception(const std::string &message) : msg_(message) {}

  //   /** Destructor.
  //    * Virtual to allow for subclassing.
  //    */
  //   virtual ~Exception() throw() {}

  //   /** Returns a pointer to the (constant) error description.
  //    *  @return A pointer to a const char*. The underlying memory
  //    *          is in posession of the Exception object. Callers must
  //    *          not attempt to free the memory.
  //    */
  //   virtual const char *what() const throw()
  //   {
  //     return msg_.c_str();
  //   }
  // };

  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
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
      std::cerr << "Publish::load certiciates " << e.what() << std::endl;
    }
  }

  Publish::Publish() : m_ioc{2},
                       m_conn{},
                       msg_queue{},
                       m_signalStatus{0},
                       m_isConnected{0},
                       m_sender_thread{}
  {
    asio::co_spawn(m_ioc.get_executor(), Publish::co_main(), asio::detached);
    m_sender_thread = std::thread([this]()
                                  { m_ioc.run(); });
  }

  Publish::~Publish()
  {
    D(std::cerr << "Redis Publisher  destroying\n";)
    PublishMessage msg;
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
        D(std::cout << "Redis Publisher destructor found msg.\n";)
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
      }
    };
    if (m_isConnected == 0)
    {
      std::cout << "Redis Publisher found not connected to redis: " << countMsg << " messages deleted\n";
    }

    m_ioc.stop();
    if (m_sender_thread.joinable())
      m_sender_thread.join();
    std::cerr << "Redis Publisher destroyed\n";
  }

  void Publish::enqueue_message(const std::string &channel, const std::string &message)
  {
    if (m_signalStatus == 1)
      return;

    PublishMessage msg;
    std::strncpy(msg.channel, channel.c_str(), CHANNEL_LENGTH - 1);
    msg.channel[CHANNEL_LENGTH - 1] = '\0'; // Always null-terminate
    std::strncpy(msg.message, message.c_str(), MESSAGE_LENGTH - 1);
    msg.message[MESSAGE_LENGTH - 1] = '\0'; // Always null-terminate

    cstokenQueuedCount++;
    msg_queue.push(msg);
  }

  asio::awaitable<void> Publish::process_messages()
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

    m_isConnected = 1;
    m_reconnectCount = 0; // reset
    for (boost::system::error_code ec;;)
    {
      std::vector<PublishMessage> batch;
      PublishMessage msg;
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
        redis::request req;
        for (const auto &m : batch)
        {
          req.push("PUBLISH", m.channel, m.message);
        }
        redis::generic_response resp;
        req.get_config().cancel_if_not_connected = true;
        co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

        if (ec)
        {
          std::cout << "Perform a full reconnect to redis. Batch size: " << batch.size()
                    << ". Reason for error: " << ec.message()
                    << std::endl;
          // Perform a full reconnect to redis
          for (const auto &m : batch)
          {
            msg_queue.push(m);
            cstokenMessageCount--;
          }

          break; // Connection lost, break so we can exit function and try reconnect to redis.
        }
        else
        {
          for (const auto &node : resp.value())
          {
            if (node.data_type == redis::resp3::type::number)
            {
              // Process number
              if (std::atoi(std::string(node.value).c_str()) > 0)
                cstokenSuccessCount++;
            }
            cstokenPublishedCount++;
          }

          D(std::cout
                << "Redis publish: " << " batch size: " << batch.size() << ". "
                << cstokenQueuedCount << " queued, "
                << cstokenMessageCount << " sent, "
                << cstokenPublishedCount << " published. "
                << cstokenSuccessCount << " successful subscribes made. "
                //<< ", payload [" << msg.message << "]"
                << std::endl;)
        }
      }
      else
      {
        co_await asio::steady_timer(co_await asio::this_coro::executor, std::chrono::milliseconds(100)).async_wait(asio::use_awaitable);
      }
    }
  }

  auto Publish::co_main() -> asio::awaitable<void>
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
        std::cerr << "Redis publish error: " << e.what() << std::endl;
      }

      // Delay before reconnecting
      m_reconnectCount++;
      std::cout << "Publish process messages exited " << m_reconnectCount << " times, reconnecting in "
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

} /* namespace RedisSubscribe */
#endif