
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
  static const int CONNECTION_RETRY_DELAY = 10;
  static const int RECOVER_PENDING_DELAY = 10;
  static const int TRIM_STREAM_DELAY = 60;
  static const int TRIM_STREAM_SIZE = 50000;

#if defined(BOOST_ASIO_HAS_CO_AWAIT)

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
    //std::cout << "set_verify_callback" << std::endl;
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
                            m_conn_read{},
                            m_conn_write{},
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
      co_await m_conn_write->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

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

      awakener.broadcast_single(
          std::string(item.stream),       // service name
          std::string(item.id),           // message ID
          std::move(convert_fields(item)) // all fields
      );
    }

    // Debug
    cstokenMessageCount += dispatch_items.size();
    std::cout << "\n******************************************************\n";
    std::cout << cstokenMessageCount << " successful messages received. " << std::endl
              << dispatch_items.size() << " messages in this response received. "
              << std::endl;
    std::cout << "******************************************************\n";
  }

  auto Consumer::receiver(Awakener &awakener) -> asio::awaitable<void>
  {
    // std::cout << "Valid streams\n";
    // for (const auto &stream : m_validStreams)
    // {
    //   std::cout << stream << std::endl;
    // }

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
      //std::cout << "Index " << index << ": " << *it << '\n';
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
      D(std::cout << "- Consumer::receiver blocking with 5000 until message response" << std::endl;)
      co_await m_conn_read->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));
      if (ec)
      {
        if (ec == asio::error::operation_aborted)
        {
          std::cout << "- Consumer::receiver operation_aborted " << ec.message() << " " << ec.value() << std::endl;
          co_return; // true; // false; // do not reconnect this ec
        }

        D(std::cout << "Perform a full reconnect to redis. Reason for error: " << std::endl;)
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
      // disable health check:
      cfg.health_check_interval = std::chrono::seconds(0); // set 0 for tls friendly
    }
    std::cout << "Worker id: " << m_worker_id << std::endl;

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif
    D(std::cout << "- Consumer co_main signal set" << std::endl;)
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          D(std::cout << "- Consumer is signaled" << std::endl;)
          m_signalStatus = 1;
          awakener.stop();
        });

    // bool should_reconnect = false;
    for (;;)
    {
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
            std::cerr << "[m_conn_read async_run] ended: " << ec.message() << " " << ec.value() << std::endl;
          });
      m_conn_write->async_run(
          cfg,
          redis::logger{redis::logger::level::err},
          [self = m_conn_write](boost::system::error_code ec)
          {
            std::cerr << "[m_conn_write async_run] ended: " << ec.message() << " " << ec.value() << std::endl;
          });

      asio::co_spawn(
          m_ioc,
          recover_pending("ttt_player_Move", awakener),
          asio::detached);
      asio::co_spawn(
          m_ioc,
          trim_stream("ttt_player_Move"),
          asio::detached);

      //std::cerr << "WorkQStream consumer co_main: run receiver" << std::endl;
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
    awakener.stop();
  }

  void Consumer::xack_now(const std::string &stream, const std::string &id)
  {
    asio::co_spawn(
        m_ioc,
        xack(stream, id),
        asio::detached);
  }

  void Consumer::send_to_dlq_now(const std::string &stream, const std::string &id,
                                 const std::unordered_map<std::string, std::string> &fields)
  {
    asio::co_spawn(
        m_ioc,
        send_to_dlq(stream, id, fields),
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
    std::cout << "- XACK'd work item:      [STREAM " << stream << "  ID " << id << "]  WORKER GROUP " << WORKER_GROUP << std::endl;
    co_await m_conn_write->async_exec(req, redis::ignore, asio::use_awaitable);
  }

  asio::awaitable<void> Consumer::send_to_dlq(std::string_view stream, std::string_view id,
                                              const std::unordered_map<std::string, std::string> &fields)
  {
    redis::request req;
    push_dlq_xadd(req, std::string(stream) + ".DLQ", id, fields);
    std::cout << "- DLQ'd work item:      [STREAM " << std::string(stream) + ".DLQ" << "  ID " << id << "]  WORKER GROUP " << WORKER_GROUP << std::endl;
    co_await m_conn_write->async_exec(req, redis::ignore, asio::use_awaitable);

    // Remove from PEL
    co_await xack(stream, id);
  }

  asio::awaitable<void> Consumer::recover_pending(std::string stream, Awakener &awakener)
  {
    auto ex = co_await asio::this_coro::executor;

    while (true)
    {
      try
      {
        // 1. Get up to 10 pending messages
        std::cout << "#### XPENDING data " << stream << " " << std::string(WORKER_GROUP) << std::endl;
        redis::request req;
        req.push("XPENDING", stream, std::string(WORKER_GROUP), "-", "+", "10");

        redis::generic_response resp;
        co_await m_conn_write->async_exec(req, resp, asio::use_awaitable);
        auto pendings = parse_xpending(resp);

        for (auto &p : pendings)
        {
          std::cout << " - Pending ID: " << p.id
                    << " consumer=" << p.consumer
                    << " idle=" << p.idle_ms
                    << " deliveries=" << p.delivery_count << "\n";
        }

        if (pendings.empty())
        {
          co_await asio::steady_timer(ex, std::chrono::seconds(RECOVER_PENDING_DELAY)).async_wait(asio::use_awaitable);
          continue;
        }

        for (auto &p : pendings)
        {
          // Optional: DLQ logic
          if (p.delivery_count > 5)
          {
            co_await send_to_dlq(stream, p.id, {/** field are missing can be retirnwith XREADGROUP see later */});
            continue;
          }

          // 2. Claim the message
          redis::request claim;
          claim.push("XCLAIM", stream, WORKER_GROUP, m_worker_id,
                     "0", p.id);

          redis::generic_response claim_resp;
          co_await m_conn_write->async_exec(claim, claim_resp, asio::use_awaitable);

          // 3. Fetch the message fields
          redis::request read_req;
          read_req.push("XREADGROUP", "GROUP", WORKER_GROUP, m_worker_id,
                        "STREAMS", stream, p.id);

          redis::generic_response read_resp;
          co_await m_conn_write->async_exec(read_req, read_resp, asio::use_awaitable);

          auto items = parse_dispatch_view(read_resp);

          for (auto &item : items)
          {
            std::cout << "- Broadcasted XCLAIMED work item: [STREAM " << item.stream << "  ID " << item.id << "]  Fields: ";
            for (auto &[k, v] : item.fields)
              std::cout << "  " << k << " = " << v;
            std::cout << std::endl;
            // 4. Push into your Awakener queue
            awakener.broadcast_single(
                std::string(item.stream),
                std::string(item.id),
                std::move(convert_fields(item)));
          }
        }
      }
      catch (std::exception &e)
      {
        std::cerr << "[XPENDING recovery] error: " << e.what() << "\n";
      }

      // Sleep before next scan
      co_await asio::steady_timer(ex, std::chrono::seconds(RECOVER_PENDING_DELAY)).async_wait(asio::use_awaitable);
    }
  }

  asio::awaitable<void> Consumer::trim_stream(std::string stream)
  {
    auto ex = co_await asio::this_coro::executor;

    while (true)
    {
      redis::request req;
      std::cout << "#### XTRIM "  << stream << " " <<  std::to_string(TRIM_STREAM_SIZE) << "\n";
      req.push("XTRIM", stream, "MAXLEN", "~", std::to_string(TRIM_STREAM_SIZE));

      co_await m_conn_write->async_exec(req, redis::ignore, asio::use_awaitable);

      co_await asio::steady_timer(ex, std::chrono::seconds(TRIM_STREAM_DELAY)).async_wait(asio::use_awaitable);
    }
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace WorkQStream */
#endif
