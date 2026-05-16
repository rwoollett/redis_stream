#include <csignal>
#include <cstdlib> // For std::getenv
#include "../workqstream/consume/Consumer.h"
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

int main(int argc, char **argv)
{
  int result = EXIT_SUCCESS;
  const char *redis_host = std::getenv("REDIS_HOST");
  const char *redis_port = std::getenv("REDIS_PORT");
  const char *redis_password = std::getenv("REDIS_PASSWORD");
  const char *redis_use_ssl = std::getenv("REDIS_USE_SSL");
  const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");
  const char *WORKER_GROUP = std::getenv("WORKER_GROUP");

  if (!(redis_host && redis_port && redis_password))
  {
    std::cerr << "Environment variables MTLOG_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD or REDIS_USE_SSL are not set." << std::endl;
    exit(1);
  }

  if (argc < 2)
  {
    std::cout << "Require cmd arg for unique id. ie. worker_$$_$count.." << std::endl;
    exit(1);
  }

  mt_logging::logger().log(
      {MTLOG_LOGFILE,
       mt_logging::LogLevel::Error,
       true});

  bool m_worker_shall_stop{false};
  try
  {
    AwakenerWaitable awakener;
    WorkQStream::Consumer redisSubscribe(argv[1], awakener);

    while (!m_worker_shall_stop)
    {
      WorkItem work = awakener.wait_broadcast();

      if (redisSubscribe.is_signal_stopped())
      {
        m_worker_shall_stop = true;
        mt_logging::logger().log(
            {"Signal to Stopped",
             mt_logging::LogLevel::Info,
             true});
        continue;
      }

      // Now you actually have the data:
      const std::string &stream = work.stream;
      const std::string &id = work.id;
      const auto &fields = work.fields;
      mt_logging::logger().log(
          {fmt::format("- Consumer work item: [STREAM {}       ID {}]  Fields: {}", stream, id, fmt::join(fields, ", ")),
           mt_logging::LogLevel::Info,
           true});
      bool ok = false; // process_job(stream, fields);

      while (true)
      {
        std::promise<std::string> p;
        auto f = p.get_future();

        redisSubscribe.xpending_oldest_now(stream, std::string(WORKER_GROUP),
                                           [&](std::string oldest)
                                           { p.set_value(oldest); });

        std::string oldest = f.get();

        if (oldest == id)
          break; // I am next in order

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }

      // if (ok)
      // {
      auto fut = redisSubscribe.xack_wait_now(stream, id);

      // release lock ONLY after XACK is confirmed
      auto ec = fut.get();

      if (ec)
      {
        mt_logging::logger().log(
            {fmt::format("XACK failed: {}", ec.message()), mt_logging::LogLevel::Info, true});
      }
      // } else {
      //   redisSubscribe.send_to_dlq_now(stream, id, fields);
      // }
    }
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
