#ifndef WORKQSTREAM_PRODUCER_H_
#define WORKQSTREAM_PRODUCER_H_

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/redis/connection.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>

#include <atomic>
#include <thread>
#include <set>
#include <map>
#include <vector>
#include <string>
#include <workqstream/common/Common.h>

#ifdef HAVE_ASIO

namespace asio = boost::asio;
namespace redis = boost::redis;

namespace WorkQStream
{

  struct ProduceField
  {
    std::string field;
    std::string value;
  };

  struct ProduceMessage
  {
    std::string stream;
    std::vector<ProduceField> fields;
  };

  enum class ConnectionState
  {
    Idle,
    Connecting,
    Authenticating,
    Ready,
    Broken,
    Reconnecting,
    Shutdown
  };

  class Producer
  {
  public:
    Producer();
    ~Producer();

    bool is_signal_stopped() { return (m_signal_status.load()); };
    // Safe to call from any thread
    void enqueue_message(
        const std::string &stream,
        const std::vector<std::pair<std::string, std::string>> &fields);

  private:
    asio::io_context m_ioc;
    asio::strand<asio::io_context::executor_type> m_strand;
    std::shared_ptr<redis::connection> m_conn;
    std::thread m_sender_thread;

    GroupConfigMap m_group_config;
    std::unordered_set<std::string> m_valid_streams;

    std::atomic<bool> m_signal_status{false};
    std::atomic<bool> m_shutting_down{false};
    std::atomic<bool> m_conn_alive{false};
    std::atomic<std::sig_atomic_t> m_reconnect_count{0};
    std::atomic<ConnectionState> m_state{ConnectionState::Idle};

    static std::atomic<std::sig_atomic_t> MESSAGE_QUEUED_COUNT;
    static std::atomic<std::sig_atomic_t> MESSAGE_COUNT;
    static std::atomic<std::sig_atomic_t> MESSAGE_SUCCESS_COUNT;

  private:
    asio::awaitable<void> co_main();
    asio::awaitable<void> produce_one(ProduceMessage msg);

    void set_state(ConnectionState new_state, std::string_view reason);
  };

  class Sender : public std::enable_shared_from_this<Sender>
  {
    WorkQStream::Producer &m_redis_producer;

  public:
    Sender(WorkQStream::Producer &publisher) : m_redis_producer{publisher} {};
    ~Sender() {};

    void Send(const std::string &channel, const std::vector<std::pair<std::string, std::string>> &message)
    {
      m_redis_producer.enqueue_message(channel, message);
    };
  };
  
} // namespace WorkQStream

#endif // HAVE_ASIO
#endif // WORKQSTREAM_PRODUCER_H_
