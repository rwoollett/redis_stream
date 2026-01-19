
#ifndef LIB_REDIS_PUBLISH_H_
#define LIB_REDIS_PUBLISH_H_

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

namespace asio = boost::asio;
namespace redis = boost::redis;

namespace RedisPublish
{

  static std::atomic<int> cstokenQueuedCount = 0;
  static std::atomic<int> cstokenMessageCount = 0;
  static std::atomic<int> cstokenSuccessCount = 0;
  static std::atomic<int> cstokenPublishedCount = 0;

  constexpr int BATCH_SIZE = 10;
  constexpr int CHANNEL_LENGTH = 64;
  constexpr int MESSAGE_LENGTH = 256;
  constexpr int QUEUE_LENGTH = 128;

  struct PublishMessage
  {
    char channel[CHANNEL_LENGTH];
    char message[MESSAGE_LENGTH];
  };

  class Publish
  {
    asio::io_context m_ioc;
    std::shared_ptr<redis::connection> m_conn;
    boost::lockfree::queue<PublishMessage, boost::lockfree::capacity<QUEUE_LENGTH>> msg_queue; // Lock-free queue
    volatile std::sig_atomic_t m_signalStatus;
    volatile std::sig_atomic_t m_isConnected;
    std::thread m_sender_thread;
    int m_reconnectCount{0};

  public:
    /// Constructor
    Publish();

    /// Deconstructor
    virtual ~Publish();

    bool isRedisSignaled() { return (m_signalStatus == 1); };
    bool isRedisConnected() { return (m_isConnected == 1); };

    void enqueue_message(const std::string &channel, const std::string &message);

  private:
    asio::awaitable<void> co_main();
    asio::awaitable<void> process_messages();
    // void handleError(const std::string &msg);
  };

  class Sender : public std::enable_shared_from_this<Sender>
  {
    RedisPublish::Publish &m_redisPublisher;

  public:
    Sender(RedisPublish::Publish &publisher) : m_redisPublisher{publisher} {};
    ~Sender() {};

    void Send(const std::string &channel, const std::string &message)
    {
      m_redisPublisher.enqueue_message(channel, message);
    };
  };

} /* namespace RedisPublish */
#endif // HAVE_ASIO
#endif /* LIB_REDIS_PUBLISH_H_ */
