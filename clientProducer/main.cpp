#include <csignal>
#include <cstdlib> // For std::getenv
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>
#include "../workqstream/produce/Producer.h"
#include <boost/redis/src.hpp>
#include <mtlog/mt_log.hpp>

#if defined(_WIN32)
    #include <conio.h>
    // Windows console handling
#else
    #include <termios.h>
    #include <unistd.h>
    // POSIX console handling
#endif

char read_getch()
{
#if defined(_WIN32)
    return _getch();
#else
    termios oldt, newt;
    tcgetattr(STDIN_FILENO, &oldt);
    newt = oldt;
    newt.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);

    char c = getchar();

    tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
    return c;
#endif
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
       mt_logging::LogLevel::Error,
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
             mt_logging::LogLevel::Error,
             true});
      }
      else
      {
        producer.enqueue_message(channel, fields);

        mt_logging::logger().log(
            {fmt::format("Published message to channel: {} with message {}", channel, fmt::join(fields, ", ")),
             mt_logging::LogLevel::Info,
             true});
      }
    };

    // The only messages to console
    // std::cout << "Application loop stated (Ctrl-C to signal stop)\n";
    bool m_worker_shall_stop{false}; // false
    while (!m_worker_shall_stop)
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::cout << "Press any key to publish..." << std::endl;
      char key = read_getch();
      std::cerr << "[" << key << "]" << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

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
        doWork("liveposts_post_Create", {{"postid", "c1234"}, {"postname", "category"}});
       // doWork("liveposts_moderate_Job");
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
