
#include "Consumer.h"
#include <memory>
#include <fstream>
#include <sstream>
#include <string>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>

namespace WorkQStream
{

  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_SERVICE_GROUP = std::getenv("REDIS_SERVICE_GROUP");
  static const char *REDIS_STREAM = std::getenv("REDIS_STREAM");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 3;

  struct DispatchView {
    std::string_view stream;
    std::string_view id;
    std::vector<std::pair<std::string_view, std::string_view>> fields;
  };

  std::vector<DispatchView>
  parse_dispatch_view(const redis::generic_response& resp)
  {
      std::vector<DispatchView> out;

      std::string_view current_stream;
      DispatchView current_msg;
      std::string_view current_key;
      int index = 0;

      for (auto const& n : resp.value())
      {

          // auto ancestorNode = (index > 1) ? resp.value().at(index - 2) : n;
          // auto prevNode = (index > 0) ? resp.value().at(index - 1) : n;
          // std::cout << "\n----parse_dispatch_view-----------------------------------" << std::endl;
          // std::cout << "Reference " << index << std::endl;
          // if (ancestorNode != n)
          // {
          //   std::cout << "index " << index << " ancestor node index " << index - 2 << std::endl;
          //   std::cout << " value          " << ancestorNode.value << std::endl;
          //   std::cout << " data_type      " << ancestorNode.data_type << std::endl;
          //   std::cout << " aggregate_size " << ancestorNode.aggregate_size << std::endl;
          //   std::cout << " depth          " << ancestorNode.depth << std::endl;
          //   std::cout << std::endl;
          // }
          // if (prevNode != n)
          // {
          //   std::cout << "index " << index << " previous node index " << index - 1 << std::endl;
          //   std::cout << " value          " << prevNode.value << std::endl;
          //   std::cout << " data_type      " << prevNode.data_type << std::endl;
          //   std::cout << " aggregate_size " << prevNode.aggregate_size << std::endl;
          //   std::cout << " depth          " << prevNode.depth << std::endl;
          //   std::cout << std::endl;
          // }
          // std::cout << "index " << index << " current node" << std::endl;
          // std::cout << " value          " << n.value << std::endl;
          // std::cout << " data_type      " << n.data_type << std::endl;
          // std::cout << " aggregate_size " << n.aggregate_size << std::endl;
          // std::cout << " depth          " << n.depth << std::endl;
          // std::cout << std::endl;

          // STREAM NAME
          if (n.depth == 1 &&
              n.data_type == boost::redis::resp3::type::blob_string)
          {
              current_stream = n.value;
              continue;
          }

          // MESSAGE ID
          if (n.depth == 3 &&
              n.data_type == boost::redis::resp3::type::blob_string)
          {
              // If we already have a message pending, push it
              if (!current_msg.id.empty()) {
                  out.push_back(std::move(current_msg));
                  current_msg = DispatchView{};
              }

              current_msg.stream = current_stream;
              current_msg.id = n.value;
              continue;
          }

          // FIELD KEY/VALUE
          if (n.depth == 4 &&
              n.data_type == boost::redis::resp3::type::blob_string)
          {
              if (current_key.empty()) {
                  current_key = n.value;
              } else {
                  current_msg.fields.emplace_back(current_key, n.value);
                  current_key = {};
              }
              continue;
          }
          index++;
      }

      // Push last message
      if (!current_msg.id.empty()) {
          out.push_back(std::move(current_msg));
      }

      return out;
  }

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
      std::cerr << "Consumer::load certiciates " << e.what() << std::endl;
    }
  }

  Consumer::Consumer(const std::string &workerId) : m_ioc{3},
                           m_conn{},
                           m_signalStatus{0},
                           cstokenWorkCount{0},
                           cstokenMessageCount{0},
                           m_isConnected{0},
                           m_worker_id(workerId)
  {
    D(std::cerr << "Consumer created\n";)
    if (REDIS_HOST == nullptr || REDIS_PORT == nullptr || REDIS_SERVICE_GROUP == nullptr || REDIS_STREAM == nullptr || REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_HOST, REDIS_PORT, REDIS_SERVICE_GROUP, REDIS_STREAM, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }
  }

  Consumer::~Consumer()
  {
    m_ioc.stop();
    if (m_receiver_thread.joinable())
      m_receiver_thread.join();
    D(std::cerr << "Consumer destroyed\n";)
  }

  void Consumer::handleError(const std::string &msg)
  {
    std::cerr << "Consumer::handleError: " << msg << std::endl;
  };

  asio::awaitable<void> Consumer::xack(std::string_view stream, std::string_view id)
  {
      redis::request req;
      req.push("XACK", stream, REDIS_SERVICE_GROUP, id);
      std::cout << "- XACK'd work item:      [STREAM " << stream << "  ID " << id << "]  SERVICE GROUP " << REDIS_SERVICE_GROUP << std::endl;
      co_await m_conn->async_exec(req, redis::ignore, asio::use_awaitable);
  }

  void Consumer::xack_now(const std::string& stream, const std::string& id)
  {
      asio::co_spawn(
          m_ioc,
          xack(stream, id),
          asio::detached
      );
  }

  auto Consumer::receiver(Awakener &awakener) -> asio::awaitable<void>
  {
    std::list<std::string> streams = splitByComma(REDIS_STREAM);
    for (const auto &stream : streams)
    {
      std::cout << stream << std::endl;
    }

    redis::request req;
    std::vector<std::string> args;
    args.reserve(6 + streams.size() * 2);

    args.push_back("GROUP");
    args.push_back(REDIS_SERVICE_GROUP);
    args.push_back(m_worker_id);
    args.push_back("BLOCK");
    args.push_back("0");
    args.push_back("STREAMS");

    size_t index = 0;
    for (auto it = streams.begin(); it != streams.end(); ++it, ++index) {
        std::cout << "Index " << index << ": " << *it << '\n';
        args.push_back(*it);
    }
    for (size_t i = 0; i < streams.size(); ++i) {
        args.push_back(">");
    }

    req.push_range("XREADGROUP", args);

    redis::generic_response resp;

    // req.get_config().cancel_if_not_connected = true;
    m_isConnected = 1;
    m_reconnectCount = 0; // reset

    // Loop reading Redis pulling messages.
    for (boost::system::error_code ec;;)
    {

      D(std::cout << "- Consumer::receiver blocking until message response " << ec.message() << std::endl;)
      co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

      if (ec)
      {
        std::cout << "- Consumer::receiver ec " << ec.message() << std::endl;
        break; // Connection lost, break so we can reconnect to channels.
      }
      awakener.on_subscribe();

      auto dispatch_items = parse_dispatch_view(resp);

      for (auto& item : dispatch_items)
      {
          // Debug
          D(std::cout << "- STREAM: " << item.stream << "  ID " << item.id << "  Fields: ";
          for (auto& [k, v] : item.fields)
              std::cout << "  " << k << " = " << v ;
          std::cout << std::endl;)

          // Convert to your queue format
          std::unordered_map<std::string, std::string> field_map; 
          field_map.reserve(item.fields.size());

          for (auto& [k, v] : item.fields)
              field_map.emplace(std::string(k), std::string(v));

          // Pass to your dispatcher
          awakener.broadcast_single(
              std::string(item.stream),   // service name
              std::string(item.id),       // message ID
              std::move(field_map)        // all fields
          );

      }

      // Debug
      // D(std::cout << "\n#******************************************************\n";
      //   std::cout << cstokenWorkCount << " subscribed, "
      //             << cstokenMessageCount << " successful messages received. " << std::endl
      //             << messages.size() << " messages in this response received. "
      //             << std::endl;
      //   std::cout << "******************************************************\n";)

      // if (!messages.empty())
      //     awakener.broadcast_messages(messages);
//      D(std::cout << "******************************************************\n\n";)

      resp.value().clear(); // Clear the response value to avoid processing old messages again.
      //redis::consume_one(resp);
    }
  }

  auto Consumer::co_main(Awakener &awakener) -> asio::awaitable<void>
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
    std::cout << "Worker id: "  << m_worker_id << std::endl;
    
    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif // defined(SIGQUIT)
    std::cout << "- Consumer co_main wait to signal" << std::endl;
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          D(std::cout << "- Consumer is signalled" << std::endl;)
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
        std::cerr << "WorkQStream consumer error: " << e.what() << std::endl;
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

  auto Consumer::main_redis(Awakener &awakener) -> int
  {
    try
    {
      // net::io_context ioc;
      asio::co_spawn(m_ioc.get_executor(), Consumer::co_main(awakener),
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
      std::cerr << "Consumer (main_redis) " << e.what() << std::endl;
      return 1;
    }
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif