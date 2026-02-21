
#pragma once

#ifdef NDEBUG
#define D(x)
#else
#define D(x) x
#endif

#include <fstream>
#include <iostream>
#include <string>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <atomic>
#include <vector>
#include <utility>

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
#include <boost/lockfree/queue.hpp>
#include <thread>
#include <iostream>
#include <memory>
#include <workqstream/common/Common.h>

namespace asio = boost::asio;
namespace redis = boost::redis;

namespace WorkQStream
{

  static std::atomic<int> MESSAGE_QUEUED_COUNT = 0;
  static std::atomic<int> MESSAGE_COUNT = 0;
  static std::atomic<int> MESSAGE_SUCCESS_COUNT = 0;

  constexpr int BATCH_SIZE = 10;
  constexpr int QUEUE_LENGTH = 128;
  constexpr int MESSAGE_LENGTH = 256;
  constexpr int CHANNEL_LENGTH = 64;
  constexpr int FIELD_NAME_LENGTH = 64;
  constexpr int FIELD_VALUE_LENGTH = 256;
  constexpr int MAX_FIELDS = 8;

  struct FieldValue
  {
    char field[FIELD_NAME_LENGTH];
    char value[FIELD_VALUE_LENGTH];
  };

  struct ProduceMessage
  {
    char channel[CHANNEL_LENGTH];
    FieldValue fields[MAX_FIELDS];
    int field_count;
  };

  class Producer
  {
    asio::io_context m_ioc;
    std::shared_ptr<redis::connection> m_conn;
    boost::lockfree::queue<ProduceMessage, boost::lockfree::capacity<QUEUE_LENGTH>> msg_queue; // Lock-free queue
    volatile std::sig_atomic_t m_signal_status;
    volatile std::sig_atomic_t m_is_connected;
    std::thread m_sender_thread;
    int m_reconnect_count{0};
    GroupConfigMap m_group_config{};
    std::unordered_set<std::string> m_valid_streams{};

  public:
    Producer();
    virtual ~Producer();

    bool is_signal_stopped() { return (m_signal_status == 1); };
    bool is_redis_connected() { return (m_is_connected == 1); };
    void enqueue_message(const std::string &channel, const std::vector<std::pair<std::string, std::string>> &fields);

  private:
    asio::awaitable<void> co_main();
    asio::awaitable<void> process_messages();
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

} /* namespace WorkQStream */
#endif // HAVE_ASIO
