#include <csignal>
#include <cstdlib> // For std::getenv
#include "../workqstream/consume/Consumer.h"
#include "../workqstream/recovery/Recovery.h"
#include "AwakenerWaitable.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <future>
#include <iostream>
#include <boost/redis/connection.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/redis/src.hpp> // boost redis implementation
#include <mtlog/mt_log.hpp>
#include <string>
#include <stdexcept>

void worker_thread(std::string worker_id)
{
  std::mutex cs_lock; // Critical section lock (DB + XACK)

  AwakenerWaitable awakener;
  WorkQStream::Consumer redisConsumer(worker_id, awakener);

  while (true)
  {
    // Wait for next message from Redis
    WorkItem work = awakener.wait_broadcast();

    if (redisConsumer.is_signal_stopped())
    {
      mt_logging::logger().log(
          {fmt::format("Consumer {} signaled to Stop {}", worker_id, 1),
           mt_logging::LogLevel::Info,
           true});
      return;
    }

    const std::string &stream = work.stream;
    const std::string &xid = work.id;
    const auto &fields = work.fields;

    mt_logging::logger().log(
        {fmt::format("---  Consumer got msg:   [WORKER {}    STREAM {}      XID {}]  Fields: {}",
                     worker_id, stream, xid, fmt::join(fields, ", ")),
         mt_logging::LogLevel::Info,
         true});

    //
    // 1. GLOBAL ORDERING BARRIER
    //
    while (true)
    {
      if (redisConsumer.is_signal_stopped())
      {
        mt_logging::logger().log(
            {fmt::format("Consumer {} signaled to Stop {}", worker_id, 2),
             mt_logging::LogLevel::Info,
             true});
        return;
      }

      std::atomic<bool> xp_pending_ready{false};
      std::string oldest;

      redisConsumer.xpending_oldest_now(
          stream,
          std::getenv("WORKER_GROUP"),
          [&](std::string xid)
          {
            oldest = xid;
            xp_pending_ready.store(true);
            //
          });

      for (int i = 0; i < 200; ++i)
      {
        if (redisConsumer.is_signal_stopped())
        {
          mt_logging::logger().log(
              {fmt::format("Consumer {} signaled to Stop {}", worker_id, 3),
               mt_logging::LogLevel::Info,
               true});
          return;
        }

        if (xp_pending_ready.load())
          break;

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      if (redisConsumer.is_signal_stopped())
      {
        mt_logging::logger().log(
            {fmt::format("Consumer {} signaled to Stop {}", worker_id, 4),
             mt_logging::LogLevel::Info,
             true});
        return;
      }

      if (oldest == xid)
        break; // I am next in order

      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    if (redisConsumer.is_signal_stopped())
    {
      mt_logging::logger().log(
          {fmt::format("Consumer {} signaled to Stop {}", worker_id, 5),
           mt_logging::LogLevel::Info,
           true});
      return;
    }

    //
    // 2. CRITICAL SECTION (DB + XACK)
    //
    {
      std::lock_guard<std::mutex> guard(cs_lock);

      // Simulate DB work
      auto td = 0;
      if (worker_id == "worker_3")
        td = 250;
      else if (worker_id == "worker_2")
        td = 120 + (rand() % 200);
      else
        td = 50;

      mt_logging::logger().log(
          {fmt::format("#&!  {}  Process for XID:  [WORKER {}    STREAM {}      XID {}]",
                       td, worker_id, stream, xid),
           mt_logging::LogLevel::Info,
           true});

      std::this_thread::sleep_for(std::chrono::milliseconds(td));
      if (redisConsumer.is_signal_stopped())
      {
        mt_logging::logger().log(
            {fmt::format("Consumer {} signaled to Stop {}", worker_id, 6),
             mt_logging::LogLevel::Info,
             true});
        return;
      }

      // XACK MUST BE INSIDE THE LOCK
      auto fut = redisConsumer.xack_wait_now(stream, xid);
      auto ec = fut.get();
      mt_logging::logger().log(
          {fmt::format("Consumer {} xack wait ec {}", worker_id, ec.message()),
           mt_logging::LogLevel::Info,
           true});

      if (ec)
      {
        mt_logging::logger().log(
            {fmt::format("#&!  XACK failed:      [WORKER {}    STREAM {}      XID {}] {}",
                         worker_id, stream, xid, ec.message()),
             mt_logging::LogLevel::Info, true});
      }
      else
      {
        mt_logging::logger().log(
            {fmt::format("#&!  XACK OK           [WORKER {}    STREAM {}      XID {}]",
                         worker_id, stream, xid),
             mt_logging::LogLevel::Info, true});
      }
    }

    // Lock released here
    std::cerr << "running " << worker_id << "\n";
  }
}

int main(int argc, char **argv)
{
  int result = EXIT_SUCCESS;
  const char *redis_host = std::getenv("REDIS_HOST");
  const char *redis_port = std::getenv("REDIS_PORT");
  const char *redis_password = std::getenv("REDIS_PASSWORD");
  const char *redis_use_ssl = std::getenv("REDIS_USE_SSL");
  const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");
  const char *WORKER_GROUP = std::getenv("WORKER_GROUP");
  const char *WORKER_RECOVER_PENDING = std::getenv("WORKER_RECOVER_PENDING");

  if (!(redis_host && redis_port && redis_password))
  {
    std::cerr << "Environment variables MTLOG_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD or REDIS_USE_SSL are not set." << std::endl;
    exit(1);
  }

  mt_logging::logger().log(
      {MTLOG_LOGFILE,
       mt_logging::LogLevel::Error,
       true});

  bool m_worker_shall_stop{false};
  try
  {

    if (std::string(WORKER_RECOVER_PENDING) == "on")
    {
      // Recovery worker on workq stream - in groupconfig env var
      WorkQStream::Recovery redisRecovery(argv[1]);

      while (!m_worker_shall_stop)
      {
        if (redisRecovery.is_signal_stopped())
        {
          m_worker_shall_stop = true;
          mt_logging::logger().log(
              {"Recovery Signal to Stopped",
               mt_logging::LogLevel::Info,
               true});
          continue;
        }
      }
    }
    else
    {

      std::thread w1(worker_thread, "worker_1");
      std::thread w2(worker_thread, "worker_2");
      //std::thread w3(worker_thread, "worker_3");

      w1.join();
      w2.join();
      //w3.join();

    } // else of WORKER_RECOVER_PENDING==on
  }
  catch (const std::exception &e)
  {
    std::cout << e.what() << "\n";
    result = EXIT_FAILURE;
  }
  catch (const std::string &e)
  {
    std::cout << e << "\n";
    result = EXIT_FAILURE;
  }

  return result;
}
