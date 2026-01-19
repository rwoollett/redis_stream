#include "io_utility/io_utility.h"
#include <csignal>
#include <cstdlib> // For std::getenv
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>
#include "../redisPublish/Publish.h" // RedisPublish class
#include <boost/redis/src.hpp>       // boost redis implementation

int main(int argc, char **argv)
{

  // Check all environment variable
  const char *redis_host = std::getenv("REDIS_HOST");
  const char *redis_port = std::getenv("REDIS_PORT");
  const char *redis_channel = std::getenv("REDIS_CHANNEL");
  const char *redis_password = std::getenv("REDIS_PASSWORD");

  if (!(redis_host && redis_port && redis_password && redis_channel))
  {
    std::cerr << "Environment variables REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, REDIS_PASSWORD or REDIS_USE_SSL are not set." << std::endl;
    exit(1);
  }
  if (argc > 1)
  {
    std::cout << "Using command line arguments as channels to publish messages." << std::endl;
  }

  try
  {

    RedisPublish::Publish redisPublish;
    // Before running do a sanity check on connections for Redis.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    std::cout << "Redis publisher connected: " << (redisPublish.isRedisConnected() ? "true" : "false") << std::endl;

    auto doPublish = [&redisPublish](const std::string &channel,
                                     const std::string &msg = "default message")
    {
      if (!redisPublish.isRedisConnected())
      {
        std::cout << "Redis connection failed, cannot publish message to channel: " << channel << std::endl;
      } else {
        redisPublish.enqueue_message(channel, msg);
        std::cout << "Published message to channel: " << channel << " with message: " << msg << std::endl;
      }
    };

    std::cout << "Application loop stated\n";
    bool m_worker_shall_stop{false}; // false
    while (!m_worker_shall_stop)
    {

      if (redisPublish.isRedisSignaled())
      {
        std::cout << "Signal to Stopped" << std::endl;
        m_worker_shall_stop = true;
        continue;
      }

      if (argc > 1)
      {
        for (int i = 1; i < argc; ++i)
        {
          doPublish(argv[i]);
        }
      }
      else
      {
        doPublish("csToken_request");
        doPublish("csToken_acquire");
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    }
  }
  catch (const std::string &e)
  {
    std::cout << e << "\n";
    return EXIT_FAILURE;
  }

  std::cout << "Exited normally\n";
  return EXIT_SUCCESS;
}
