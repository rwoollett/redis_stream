
#include "Subscribe.h"
#include <memory>
#include <fstream>
#include <sstream>
#include <string>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>

namespace RedisSubscribe
{

  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_CHANNEL = std::getenv("REDIS_CHANNEL");
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
      std::cerr << "Subscribe::load certiciates " << e.what() << std::endl;
    }
  }

  Subscribe::Subscribe() : m_ioc{3},
                           m_conn{},
                           m_signalStatus{0},
                           cstokenSubscribedCount{0},
                           cstokenMessageCount{0},
                           m_isConnected{0}
  {
    D(std::cerr << "Subscribe created\n";)
    if (REDIS_HOST == nullptr || REDIS_PORT == nullptr || REDIS_CHANNEL == nullptr || REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }
  }

  Subscribe::~Subscribe()
  {
    m_ioc.stop();
    if (m_receiver_thread.joinable())
      m_receiver_thread.join();
    D(std::cerr << "Subscriber destroyed\n";)
  }

  void Subscribe::handleError(const std::string &msg)
  {
    std::cerr << "Subscribe::handleError: " << msg << std::endl;
  };

  auto Subscribe::receiver(Awakener &awakener) -> asio::awaitable<void>
  {

    // get
    std::list<std::string> channels = splitByComma(REDIS_CHANNEL);
    // Print the result
    for (const auto &channel : channels)
    {
      std::cout << channel << std::endl;
    }

    redis::request req;
    req.push_range("SUBSCRIBE", channels);

    redis::generic_response resp;
    D(std::cout << "- Subscribe::receiver try connenct" << std::endl;)

    // Reconnect to channels.
    // std::cout << "Configure ssl env is " << REDIS_USE_SSL << "\n";
    // if (std::string(REDIS_USE_SSL) == "on")
    // {
    //   std::cout << "Configure ssl next layer\n";
    //   m_conn->next_layer().set_verify_mode(asio::ssl::verify_peer);
    //   m_conn->next_layer().set_verify_callback(verify_certificate);
    // }
    m_conn->set_receive_response(resp);

    // req.get_config().cancel_if_not_connected = true;
    co_await m_conn->async_exec(req, redis::ignore, asio::deferred);

    awakener.on_subscribe();
    m_isConnected = 1;
    m_reconnectCount = 0; // reset
    // Loop reading Redis pushs messages.
    for (boost::system::error_code ec;;)
    {

      D(std::cout << "- Subscribe::receiver generic response " << ec.message() << std::endl;)
      // First tries to read any buffered pushes.
      m_conn->receive(ec);
      if (ec == redis::error::sync_receive_push_failed)
      {
        ec = {};
        co_await m_conn->async_receive(asio::redirect_error(asio::use_awaitable, ec));
      }

      if (ec)
      {
        std::cout << "- Subscribe::receiver ec " << ec.message() << std::endl;
        break; // Connection lost, break so we can reconnect to channels.
      }

      int amount = resp.value().size();
      int index = 0;
      int refmsg = cstokenMessageCount + cstokenSubscribedCount;
      // The resp.value() is a vector of nodes, each node contains a value and a data_type.
      // The SUBSCRIBE response is a vector of nodes, where the first node is the command name,
      // the second node is the channel name, and the third node is the message payload.
      // There can be multiple msg payloads for the same channel.
      // So a size of 12 means there are 3 messages in the vector, the first message is at index 0,
      // the second message is at index 4, and third at index 8.
      D(std::cout << refmsg << " resp.value() information: amount: " << amount << std::endl;)
      std::list<std::string> messages;
      for (const auto &node : resp.value())
      {

        auto ancestorNode = (index > 1) ? resp.value().at(index - 2) : node;
        auto prevNode = (index > 0) ? resp.value().at(index - 1) : node;
        // std::cout << refmsg << " " << index << " value() at ancestorNode: " << ancestorNode.value << std::endl;
        // std::cout << refmsg << " " << index << " data_type() at ancestorNode: " << ancestorNode.data_type << std::endl
        //           << std::endl;
        // std::cout << refmsg << " " << index << " value() at prevNode: " << prevNode.value << std::endl;
        // std::cout << refmsg << " " << index << " data_type() at prevNode: " << prevNode.data_type << std::endl
        //           << std::endl;
        // std::cout << refmsg << " " << index << " value() at node: " << node.value << std::endl;
        // std::cout << refmsg << " " << index << " data_type() at node: " << node.data_type << std::endl
        //           << std::endl;
        if (node.data_type == boost::redis::resp3::type::simple_error)
        {
          // Handle simple error
          // std::cout << refmsg << " " << index << " resp.value() at node: " << node.value << std::endl;
          // std::cout << refmsg << " " << index << " resp.value() at node: " << node.data_type << std::endl;
          handleError("Subscribe::receiver error: " + std::string(node.value));
          continue; // Skip to the next node
        }
        if (node.data_type == boost::redis::resp3::type::blob_string ||
            node.data_type == boost::redis::resp3::type::simple_string)
        {
          // Handle simple string
          auto msg = node.value;
          // std::cout << refmsg << " " << index << " resp.value() at node: " << node.value << std::endl;
          // std::cout << refmsg << " " << index << " resp.value() at node: " << node.data_type << std::endl;
          if (msg == "subscribe")
          {
            cstokenSubscribedCount++;
            // std::cout << refmsg << " " << index << " resp.value() at node: subscribe" << std::endl;
          }
          else if (msg == "message")
          {
            cstokenMessageCount++;
            // std::cout << refmsg << " " << index << " resp.value() at node: message" << std::endl;
          }
          // else if (std::find(channels.begin(), channels.end(), msg) != channels.end())
          // {
          //   std::cout << refmsg << " " << index << " find channel " << msg << std::endl;
          // }
          else
          {
            if (ancestorNode.value == "message")
            {
              // This is a message payload, we can process it.
              std::stringstream ss;
              ss << prevNode.value << " message: " << msg;
              messages.push_back(msg);
            }
          }
        }
        index++;
      }

      D(std::cout << "\n#******************************************************\n";
        std::cout << cstokenSubscribedCount << " subscribed, "
                  << cstokenMessageCount << " successful messages received. " << std::endl
                  << messages.size() << " messages in this response received. "
                  << amount << " size of resp. " << std::endl;
        std::cout << "******************************************************\n\n";)

      awakener.broadcast_messages(messages);

      resp.value().clear(); // Clear the response value to avoid processing old messages again.
      redis::consume_one(resp);
    }
    // }
  }

  auto Subscribe::co_main(Awakener &awakener) -> asio::awaitable<void>
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
    std::cout << "- Subscribe co_main wait to signal" << std::endl;
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          m_signalStatus = 1;
          awakener.stop();
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

      m_conn->async_run(cfg, redis::logger{redis::logger::level::err},  asio::consign(asio::detached, m_conn));

      try
      {
        co_await receiver(awakener);
      }
      catch (const std::exception &e)
      {
        std::cerr << "Redis subscribe error: " << e.what() << std::endl;
      }

      // Delay before reconnecting
      m_isConnected = 0;
      m_reconnectCount++;
      std::cout << "Receiver exited " << m_reconnectCount << " times, reconnecting in "
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
    awakener.stop();
  }

  auto Subscribe::main_redis(Awakener &awakener) -> int
  {
    try
    {
      // net::io_context ioc;
      asio::co_spawn(m_ioc.get_executor(), Subscribe::co_main(awakener),
                     [](std::exception_ptr p)
                     {
                       std::cout << "co_spawn(m_ioc.get_executor() throw\n";
                       if (p)
                         std::rethrow_exception(p);
                     });
      m_receiver_thread = std::thread([this]()
                                      { m_ioc.run(); });
      return 0;
    }
    catch (std::exception const &e)
    {
      std::cerr << "subscribe (main_redis) " << e.what() << std::endl;
      return 1;
    }
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace RedisSubscribe */
#endif