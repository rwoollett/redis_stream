#include <csignal>
#include <cstdlib> // For std::getenv
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>
#include "../workqstream/produce/Producer.h"
#include <boost/redis/src.hpp>
#include <mtlog/mt_log.hpp>
#include <termios.h>
#include <unistd.h>

char getch()
{
  termios oldt, newt;
  tcgetattr(STDIN_FILENO, &oldt); // save old settings
  newt = oldt;
  newt.c_lflag &= ~(ICANON | ECHO); // disable buffering + echo
  tcsetattr(STDIN_FILENO, TCSANOW, &newt);

  char c = getchar(); // read one char

  tcsetattr(STDIN_FILENO, TCSANOW, &oldt); // restore settings
  return c;
}

int main(int argc, char **argv)
{

  // Check all environment variable
  const char *redis_host = std::getenv("REDIS_HOST");
  const char *redis_port = std::getenv("REDIS_PORT");
  const char *redis_password = std::getenv("REDIS_PASSWORD");
  const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");

  if (!(redis_host && redis_port && redis_password && MTLOG_LOGFILE))
  {
    std::cerr << "Environment variables MTLOG_LOGFILE, REDIS_HOST, REDIS_PORT or REDIS_PASSWORD are not set." << std::endl;
    exit(1);
  }
  if (argc > 1)
  {
    std::cout << "Using command line arguments as channels to publish messages." << std::endl;
  }
  mt_logging::logger().log(
      {MTLOG_LOGFILE,
       true});

  try
  {

    WorkQStream::Producer producer;
    // Before running do a sanity check on connections for Redis.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    auto doWork = [&producer](const std::string &channel,
                              const std::vector<std::pair<std::string, std::string>> &fields = {{"postid", "c1234"}})
    {
      if (producer.is_signal_stopped())
      {
        mt_logging::logger().log(
            {fmt::format("Redis connection failed, cannot publish message to channel: {}", channel),
             true});
      }
      else
      {
        producer.enqueue_message(channel, fields);

        D(mt_logging::logger().log(
            {fmt::format("Published message to channel: {} with message {}", channel, fmt::join(fields, ", ")),
             true});)
      }
    };

    // The only messages to console
    // std::cout << "Application loop stated (Ctrl-C to signal stop)\n";
    bool m_worker_shall_stop{false}; // false
    while (!m_worker_shall_stop)
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));

      if (producer.is_signal_stopped())
      {
        m_worker_shall_stop = true;
        continue;
      }

      std::cout << "Press any key to publish..." << std::endl;
      char key = getch();

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

  // std::cout << "Exited normally\n";
  return EXIT_SUCCESS;
}
