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

  if (!(redis_host && redis_port && redis_password))
  {
    std::cerr << "Environment variables REDIS_HOST, REDIS_PORT, REDIS_PASSWORD or REDIS_USE_SSL are not set." << std::endl;
    exit(1);
  }
  if (argc > 1)
  {
    std::cout << "Using command line arguments as channels to publish messages." << std::endl;
  }
  mt_logging::logger().log(
      {"output_publ.log",
       "output_publ.log",
       std::ios::out,
       true});

  try
  {

    WorkQStream::Producer producer;
    // Before running do a sanity check on connections for Redis.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    // Log and create ClientProducer log
    mt_logging::logger().log(
        {"output_publ.log",
         fmt::format("Redis producer connected: {}", (producer.is_redis_connected() ? "true" : "false")),
         std::ios::app,
         true});

    auto doWork = [&producer](const std::string &channel,
                              const std::vector<std::pair<std::string, std::string>> &fields = {{"postid", "c1234"}})
    {
      if (!producer.is_redis_connected())
      {
        mt_logging::logger().log(
            {"output_publ.log",
             fmt::format("Redis connection failed, cannot publish message to channel: {}", channel),
             std::ios::app,
             true});
      }
      else
      {
        producer.enqueue_message(channel, fields);

        D(mt_logging::logger().log(
            {"output_publ.log",
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
        //std::cout << "Signal to Stopped" << std::endl;
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
