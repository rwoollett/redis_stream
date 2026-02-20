
#include "Consumer.h"
#include <memory>
#include <fstream>
#include <sstream>
#include <string>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>
#include "Dispatch.h"

namespace WorkQStream
{

  static const char *WORKER_GROUP = std::getenv("WORKER_GROUP");
  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 3;

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
      std::cerr << "Consumer::load certiciates " << e.what() << std::endl;
    }
  }

  Consumer::Consumer(
      const std::string &workerId,
      Awakener &awakener) : m_ioc{3},
                            m_conn{},
                            m_signalStatus{0},
                            cstokenMessageCount{0},
                            m_isConnected{0},
                            m_worker_id(workerId),
                            m_group_config(load_group_config()),
                            m_validStreams{}
  {
    D(std::cerr << "Consumer created\n";)
    if (REDIS_HOST == nullptr || REDIS_PORT == nullptr ||
        REDIS_PASSWORD == nullptr || REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }
    for (const auto &s : get_worker_group(m_group_config).streams)
    {
      if (s.empty())
        throw std::runtime_error("Stream name cannot be empty");
      if (s.find(' ') != std::string::npos)
        throw std::runtime_error("Stream name cannot contain spaces: " + s);

      m_validStreams.insert(s);
    }

    asio::co_spawn(m_ioc.get_executor(), Consumer::co_main(awakener), asio::detached);
    m_receiver_thread = std::thread([this]()
                                    { m_ioc.run(); });
  }

  Consumer::~Consumer()
  {
    std::cerr << "Consumer destroyed\n";
    m_ioc.stop();
    if (m_receiver_thread.joinable())
      m_receiver_thread.join();
  }

  void Consumer::handleError(const std::string &msg)
  {
    std::cerr << "Consumer::handleError: " << msg << std::endl;
  };

  asio::awaitable<void> Consumer::ensure_group_exists()
  {
    for (const auto &stream : m_validStreams)
    {
      std::cout << "Ensuring group '" << WORKER_GROUP << "' exists on stream '" << stream << "'\n";
      redis::request req;
      req.push("XGROUP", "CREATE",
               stream,
               WORKER_GROUP,
               "0",
               "MKSTREAM");

      redis::response<std::string> resp;
      boost::system::error_code ec;
      co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

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
                  WORKER_GROUP,
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

  void Consumer::read_stream(const redis::generic_response &resp, Awakener &awakener)
  {
    auto dispatch_items = parse_dispatch_view(resp);

    for (auto &item : dispatch_items)
    {
      // Debug
      D(std::cout << "- STREAM: " << item.stream << "  ID " << item.id << "  Fields: ";
        for (auto &[k, v] : item.fields)
            std::cout
        << "  " << k << " = " << v;
        std::cout << std::endl;)

      // Convert to your queue format
      std::unordered_map<std::string, std::string> field_map;
      field_map.reserve(item.fields.size());

      for (auto &[k, v] : item.fields)
        field_map.emplace(std::string(k), std::string(v));

      // Pass to your dispatcher
      awakener.broadcast_single(
          std::string(item.stream), // service name
          std::string(item.id),     // message ID
          std::move(field_map)      // all fields
      );
    }

    // Debug
    cstokenMessageCount += dispatch_items.size();
    std::cout << "\n#******************************************************\n";
    std::cout << cstokenMessageCount << " successful messages received. " << std::endl
              << dispatch_items.size() << " messages in this response received. "
              << std::endl;
    std::cout << "******************************************************\n";
  }

  auto Consumer::receiver(Awakener &awakener) -> asio::awaitable<void>
  {
    std::cout << "Valid streams\n";
    for (const auto &stream : m_validStreams)
    {
      std::cout << stream << std::endl;
    }

    redis::request req;
    std::vector<std::string> args;
    args.reserve(6 + m_validStreams.size() * 2);

    args.push_back("GROUP");
    args.push_back(WORKER_GROUP);
    args.push_back(m_worker_id);
    args.push_back("BLOCK");
    args.push_back("5000");
    args.push_back("STREAMS");

    size_t index = 0;
    for (auto it = m_validStreams.begin(); it != m_validStreams.end(); ++it, ++index)
    {
      std::cout << "Index " << index << ": " << *it << '\n';
      args.push_back(*it);
    }
    for (size_t i = 0; i < m_validStreams.size(); ++i)
    {
      args.push_back(">");
    }

    req.push_range("XREADGROUP", args);
    redis::generic_response resp;

    req.get_config().cancel_if_not_connected = false;
    m_isConnected = 1;
    m_reconnectCount = 0; // reset

    for (boost::system::error_code ec;;)
    {
      std::cout << "- Consumer::receiver blocking with 5000 until message response" << std::endl;
      co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));
      if (ec)
      {
        if (ec == asio::error::operation_aborted)
        {
          std::cout << "- Consumer::receiver operation_aborted " << ec.message() << " " << ec.value() << std::endl;
          co_return; // true; // false; // do not reconnect this ec
        }

        std::cout << "Perform a full reconnect to redis. Reason for error: " << std::endl;
        throw std::runtime_error(
            make_ops_error(
                "XREADGROUP", "(n/a)",
                WORKER_GROUP, m_worker_id,
                ec.message(),
                "Check Redis connectivity and authentication"));
        //
      }

      read_stream(resp, awakener);

      resp.value().clear(); // Clear the response value to avoid processing old messages again.
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
    // disable health check:
    cfg.health_check_interval = std::chrono::seconds(0); // disable for tls friendly
    std::cout << "Worker id: " << m_worker_id << std::endl;

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif
    std::cout << "- Consumer co_main signal set" << std::endl;
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          std::cout << "- Consumer is signaled" << std::endl;
          m_signalStatus = 1;
          awakener.stop();
        });

    // bool should_reconnect = false;
    for (;;)
    {
      if (std::string(REDIS_USE_SSL) == "on")
      {
        asio::ssl::context ssl_ctx{asio::ssl::context::tlsv12_client};
        ssl_ctx.set_verify_mode(asio::ssl::verify_peer);
        load_certificates(ssl_ctx, "tls/ca.crt", /** Your self-signed CA*/ "tls/redis.crt", /** Your client certificate*/ "tls/redis.key" /** Your private key */);
        ssl_ctx.set_verify_callback(verify_certificate);
        m_conn = std::make_shared<redis::connection>(ex, std::move(ssl_ctx));
      }
      else
      {
        m_conn = std::make_shared<redis::connection>(ex);
      }

      m_conn->async_run(
          cfg,
          redis::logger{redis::logger::level::info},
          [self = m_conn](boost::system::error_code ec)
          {
            std::cerr << "[async_run] ended: " << ec.message() << " " << ec.value() << std::endl;
          });

      std::cerr << "WorkQStream consumer co_main: run receiver" << std::endl;
      try
      {
        co_await ensure_group_exists();
        co_await receiver(awakener);
      }
      catch (const std::exception &e)
      {
        std::cerr << "WorkQStream consumer error: " << e.what() << std::endl;
      }

      m_isConnected = 0;
      m_reconnectCount++;
      std::cout << "Receiver exited " << m_reconnectCount << " times, reconnecting in " << CONNECTION_RETRY_DELAY << " second..." << std::endl;
      if (CONNECTION_RETRY_AMOUNT == -1)
        continue;
      if (m_reconnectCount >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY)).async_wait(asio::use_awaitable);
    }
    m_signalStatus = 1;
    awakener.stop();
  }

  // asio::awaitable<void> Consumer::xack(std::string_view stream, std::string_view id)
  // {
  //   redis::request req;
  //   req.push("XACK", stream, WORKER_GROUP, id);
  //   std::cout << "- XACK'd work item:      [STREAM " << stream << "  ID " << id << "]  WORKER GROUP " << WORKER_GROUP << std::endl;
  //   co_await m_conn->async_exec(req, redis::ignore, asio::use_awaitable);
  // }

  // void Consumer::xack_now(const std::string &stream, const std::string &id)
  // {
  //   asio::co_spawn(
  //       m_ioc,
  //       xack(stream, id),
  //       asio::detached);
  // }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif
