
#ifndef LIB_REDIS_SUBSCRIBE_H_
#define LIB_REDIS_SUBSCRIBE_H_

#ifdef NDEBUG
#define D(x)
#else
#define D(x) x
#endif

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <stdexcept>

#ifdef HAVE_ASIO
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/redis/connection.hpp>
#include <boost/redis/logger.hpp>
#include <boost/asio/io_context.hpp>
#include <thread>
#include <iostream>
#include <workqstream/common/Common.h>

namespace asio = boost::asio;
namespace redis = boost::redis;

namespace WorkQStream
{

  class Awakener
  {

  public:
    // The base class will print the messages.
    virtual void broadcast_single(
        std::string stream_name,
        std::string message_id,
        std::unordered_map<std::string, std::string> fields)
    {
      std::cout << "Awakener::broadcast_singles\n";
      std::cout << "- Broadcast work item message\n";
      std::cout << "STREAM: " << stream_name << "\n";
      std::cout << "  ID: " << message_id << "\n";
      for (auto &[k, v] : fields)
        std::cout << "    " << k << " = " << v << "\n";

      std::cout << std::endl;
      D(std::cout << "******************************************************#\n\n";)
    }

    virtual void on_subscribe() {
      //  do nothing in base class
    };

    virtual void stop() {
      //  do nothing in base class
    };
  };

  class Consumer
  {
    asio::io_context m_ioc;
    std::shared_ptr<redis::connection> m_conn_read;
    std::shared_ptr<redis::connection> m_conn_write;
    std::thread m_receiver_thread;
    volatile std::sig_atomic_t m_signal_status;
    volatile std::sig_atomic_t m_is_connected;
    int m_cstoken_message_count{0};
    int m_reconnect_count{0};
    std::string m_worker_id;
    GroupConfigMap m_group_config{};
    std::unordered_set<std::string> m_valid_streams{};

  public:
    /// Constructor
    Consumer(const std::string &workerId, Awakener &awakener);

    /// Deconstructor
    virtual ~Consumer();

    virtual bool is_signal_stopped() { return (m_signal_status == 1); };
    bool is_redis_connected() { return (m_is_connected == 1); };
    void xack_now(const std::string &stream, const std::string &id);
    void send_to_dlq_now(const std::string &stream, const std::string &id,
                         const std::unordered_map<std::string, std::string> &fields);

  private:
    asio::awaitable<void> ensure_group_exists();
    asio::awaitable<void> receiver(Awakener &awakener);
    asio::awaitable<void> co_main(Awakener &awakener);
    asio::awaitable<void> xack(std::string_view stream, std::string_view id);
    asio::awaitable<void> send_to_dlq(std::string_view stream, std::string_view id,
                                      const std::unordered_map<std::string, std::string> &fields);

    // Timed routines co spawn in co_main                                      
    asio::awaitable<void> recover_pending(std::string stream, Awakener &awakener);
    asio::awaitable<void> trim_stream(std::string stream);
                                        
    void read_stream(const redis::generic_response &resp, Awakener &awakener);

  };

} /* namespace WorkQStream */
#endif // HAVE_ASIO
#endif /* LIB_REDIS_SUBSCRIBE_H_ */
