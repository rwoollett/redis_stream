
#include "MessageMonitor.h"
#include <iostream>
#include <string>

namespace RedisPublish
{

  template class MessageMonitor<PublishMessage>;

  template <typename T>
  void MessageMonitor<T>::PushMessage(const T &message)
  {
    {
      std::unique_lock<std::mutex> cl(m_class_lock);
      m_cond_not_full.wait(cl, [this]
                           { return m_size < (MAX) || m_stop; });
      m_msg_queue.push(message);
      m_size += 1;
    }
    m_cond_not_empty.notify_all();
  }

  template <typename T>
  T MessageMonitor<T>::PopMessage()
  {
    T message{};
    {
      std::unique_lock<std::mutex> cl(m_class_lock);
      if (m_timeOut > 0)
      {
        m_cond_not_empty.wait_for(cl, std::chrono::milliseconds(m_timeOut), [this]
                                  { return m_size > 0 || m_stop; });
      }
      else
      {
        m_cond_not_empty.wait(cl, [this]
                              { return m_size > 0 || m_stop; });
      }
      if (m_size > 0) // if not timed out
      { 
        message = m_msg_queue.front();
        m_msg_queue.pop();
        m_size -= 1;
      }
    }
    m_cond_not_full.notify_all();
    return std::move(message);
  }

  template <typename T>
  void MessageMonitor<T>::stop()
  {
    {
      std::lock_guard<std::mutex> lock(m_class_lock);
      m_stop = true;
    }
    m_cond_not_empty.notify_all();
    m_cond_not_full.notify_all();
  }
}