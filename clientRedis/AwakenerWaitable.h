#ifndef REDISCLIENT_AWAKENER_WAITABLE_H_
#define REDISCLIENT_AWAKENER_WAITABLE_H_

#include "../redisSubscribe/Subscribe.h"

class AwakenerWaitable : public RedisSubscribe::Awakener
{
  std::atomic<int> awake{0};
  std::mutex m_class_lock;
  std::condition_variable m_cond_not_awake;
  bool shall_stop_awaken = false;

public:
  int awake_load() { return awake.load(); };

  // The wait_broadcast function is optional if you want to use the Awakener class
  // to synchronize the main thread with the broadcast messages.

  // This function will block until there is at least one message to process.
  // It is important to call this function in a loop, as it will block until
  // there is at least one message to process.
  // If you want to stop waiting for messages, call stop() to set shall_stop_awaken to true.
  void wait_broadcast()
  {
    std::unique_lock<std::mutex> cl(m_class_lock);
    m_cond_not_awake.wait(cl, [this]
                          { return awake.load() > 0 || shall_stop_awaken; });
    if (awake.load() > 0)
      awake.fetch_sub(1);
  };

  // This function will broadcast the messages to the main thread.
  // It will print the messages to the standard output.
  // This function is called by the receiver when it receives a message from Redis.
  // The base class will print the messages.
  // It is able to be overridden in a derived class if you want to handle the messages differently.
  // If you want to handle the messages differently, you can override this function in a derived class.
  // Plus then it may not be required to use the Awakener class wait_broadcast method to
  // synchronize another thread with the broadcast messages.
  virtual void broadcast_messages(const std::list<std::string> &broadcast_messages)
  {
    std::cout << "AwakenerWaitable::broadcast_messages\n";
    {
      std::unique_lock<std::mutex> cl(m_class_lock);
      if (broadcast_messages.empty())
        return; // If there are no messages, do not update.
      // The base class will print the messages.
      std::cout << "- Broadcast subscribed messages\n";
      for (const auto &msg : broadcast_messages)
      {
        std::cout << msg << std::endl;
      }
      std::cout << std::endl;
      D(std::cout << "******************************************************#\n\n";)
      awake.fetch_add(1);
    }
    m_cond_not_awake.notify_one();
  };

  virtual void stop()
  {
    std::cout << "AwakenerWaitable::stop\n";
    {
      std::unique_lock<std::mutex> cl(m_class_lock);
      shall_stop_awaken = true;
    }
    m_cond_not_awake.notify_one();
  }
};

#endif // NETPROC_AWAKENER_WAITABLE_H_