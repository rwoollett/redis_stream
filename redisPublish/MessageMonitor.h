
#ifndef LIB_REDIS_PUBLISH_MESSAGE_MONITOR_H_
#define LIB_REDIS_PUBLISH_MESSAGE_MONITOR_H_

#include <queue>
#include <mutex>
#include <condition_variable>

namespace RedisPublish
{

  struct PublishMessage
  {
    std::string channel;
    std::string message;
  };

  template <typename T>
  class MessageMonitor
  {
  private:
    const int MAX = 10;
    //    std::queue<std::string> m_msg_queue;
    std::queue<T> m_msg_queue;
    std::mutex m_class_lock;
    std::condition_variable m_cond_not_empty;
    std::condition_variable m_cond_not_full;
    int m_timeOut;
    int m_size;
    bool m_stop;

  public:
    MessageMonitor() : m_msg_queue{}, m_class_lock(),
                       m_cond_not_empty(), m_cond_not_full(),
                       m_size(0), m_timeOut(0), m_stop(false) {};
    MessageMonitor(int timeOut) : m_msg_queue{}, m_class_lock(),
                                  m_cond_not_empty(), m_cond_not_full(),
                                  m_size(0), m_timeOut(timeOut), m_stop(false) {};
    ~MessageMonitor() {};
    int Size()
    {
      std::lock_guard<std::mutex> cl(m_class_lock);
      return m_size;
    };
    void PushMessage(const T &message);
    T PopMessage();

    void stop();
  };
}
#endif // LIB_REDIS_PUBLISH_MESSAGE_MONITOR_H_
