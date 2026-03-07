#include <csignal>
#include <cstdlib> // For std::getenv
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>
#include "../workqstream/produce/Producer.h"
#include <boost/redis/src.hpp>
#include <mtlog/mt_log.hpp>

int main(int argc, char **argv)
{

  // Check all environment variable
  const char *redis_host = std::getenv("REDIS_HOST");
  const char *redis_port = std::getenv("REDIS_PORT");
  const char *redis_password = std::getenv("REDIS_PASSWORD");
  const char *REDIS_STREAM_PRODUCER_LOGFILE = std::getenv("REDIS_STREAM_PRODUCER_LOGFILE");

  if (!(redis_host && redis_port && redis_password && REDIS_STREAM_PRODUCER_LOGFILE))
  {
    std::cerr << "Environment variables REDIS_STREAM_PRODUCER_LOGFILE, REDIS_HOST, REDIS_PORT or REDIS_PASSWORD are not set." << std::endl;
    exit(1);
  }
  if (argc > 1)
  {
    std::cout << "Using command line arguments as channels to publish messages." << std::endl;
  }
  mt_logging::logger().log(
      {REDIS_STREAM_PRODUCER_LOGFILE,
       REDIS_STREAM_PRODUCER_LOGFILE,
       std::ios::out,
       true});

  try
  {

    WorkQStream::Producer producer;
    // Before running do a sanity check on connections for Redis.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    // Log and create ClientProducer log
    mt_logging::logger().log(
        {REDIS_STREAM_PRODUCER_LOGFILE,
         fmt::format("Redis producer connected: {}", (producer.is_redis_connected() ? "true" : "false")),
         std::ios::app,
         true});

    auto doWork = [&producer, REDIS_STREAM_PRODUCER_LOGFILE](const std::string &channel,
                              const std::vector<std::pair<std::string, std::string>> &fields = {{"postid", "c1234"}})
    {
      if (!producer.is_redis_connected())
      {
        mt_logging::logger().log(
            {REDIS_STREAM_PRODUCER_LOGFILE,
             fmt::format("Redis connection failed, cannot publish message to channel: {}", channel),
             std::ios::app,
             true});
      }
      else
      {
        producer.enqueue_message(channel, fields);

        D(mt_logging::logger().log(
            {REDIS_STREAM_PRODUCER_LOGFILE,
             fmt::format("Published message to channel: {} with message {}", channel, fmt::join(fields, ", ")),
             std::ios::app,
             true});)
      }
    };

    // The only messages to console
    //std::cout << "Application loop stated (Ctrl-C to signal stop)\n";
    bool m_worker_shall_stop{false}; // false
    while (!m_worker_shall_stop)
    {

      if (producer.is_signal_stopped())
      {
        m_worker_shall_stop = true;
        continue;
      }

      if (argc > 1)
      {
        for (int i = 1; i < argc; ++i)
        {
          doWork(argv[i]);
        }
      }
      else
      {
        doWork("ttt_player_Move", {{"postid", "c1234"}, {"postname", "category"}});
        doWork("ttt_player_Move");
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    }
  }
  catch (const std::exception &e)
  {
    std::cerr << e.what() << "\n";
    return EXIT_FAILURE;
  }
  catch (const std::string &e)
  {
    std::cerr << e << "\n";
    return EXIT_FAILURE;
  }

  //std::cout << "Exited normally\n";
  return EXIT_SUCCESS;
}
