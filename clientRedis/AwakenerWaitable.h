#ifndef REDISCLIENT_AWAKENER_WAITABLE_H_
#define REDISCLIENT_AWAKENER_WAITABLE_H_

#include "../workqstream/consume/Consumer.h"
#include <queue>

struct WorkItem {
  std::string stream;
  std::string id;
  std::unordered_map<std::string, std::string> fields;
};


class AwakenerWaitable : public WorkQStream::Awakener
{
  std::mutex m_class_lock;
  std::condition_variable m_cond_not_awake;
  bool shall_stop_awaken;
  std::queue<WorkItem> m_work_queue;

public:
  AwakenerWaitable():shall_stop_awaken{false},m_work_queue{}, WorkQStream::Awakener()
  {
  };
  ~AwakenerWaitable() 
  {
    std::cout << "Size awakener work queue " << m_work_queue.size() << std::endl;
  };

  // This function will block until there is at least one message to process.
  // It is important to call this function in a loop, as it will block until
  // there is at least one message to process.
  // If you want to stop waiting for messages, call stop() to set shall_stop_awaken to true.
  WorkItem wait_broadcast()
  {
    std::unique_lock<std::mutex> cl(m_class_lock);
    m_cond_not_awake.wait(cl, [this] {
      return !m_work_queue.empty() || shall_stop_awaken;
    });

    if (shall_stop_awaken && m_work_queue.empty())
      return {}; // or some sentinel

    auto item = std::move(m_work_queue.front());
    m_work_queue.pop();
    return item;
  }

  // This function will broadcast the messages to the main thread.
  // It will print the messages to the standard output.
  // This function is called by the receiver when it receives a message from Redis.
  // The base class will print the messages.
  // It is able to be overridden in a derived class if you want to handle the messages differently.
  // If you want to handle the messages differently, you can override this function in a derived class.
  // Plus then it may not be required to use the Awakener class wait_broadcast method to
  // synchronize another thread with the broadcast messages.
  virtual void broadcast_single(
      std::string stream_name,
      std::string message_id,
      std::unordered_map<std::string, std::string> fields)
  {
    WorkItem work_item;
    work_item.stream = std::move(stream_name);
    work_item.id = std::move(message_id);
    work_item.fields.reserve(fields.size());
    for (auto& [k, v] : fields)
        work_item.fields.emplace(k, v);

    D(std::cout << "AwakenerWaitable::broadcast_single\n";)
    {
      std::unique_lock<std::mutex> cl(m_class_lock);
      m_work_queue.push(work_item);

    }
    m_cond_not_awake.notify_one();
    
  }

  virtual void stop()
  {
    D(std::cout << "AwakenerWaitable::stop\n";)
    {
      std::unique_lock<std::mutex> cl(m_class_lock);
      shall_stop_awaken = true;
    }
    m_cond_not_awake.notify_one();
  }
};

#endif // NETPROC_AWAKENER_WAITABLE_H_