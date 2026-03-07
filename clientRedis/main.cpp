#include <csignal>
#include <cstdlib> // For std::getenv
#include "../workqstream/consume/Consumer.h"
#include "AwakenerWaitable.h"
#include <mutex>
#include <condition_variable>
#include <thread>
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
  const char *REDIS_STREAM_CONSUMER_LOGFILE = std::getenv("REDIS_STREAM_CONSUMER_LOGFILE");

  if (!(redis_host && redis_port && redis_password))
  {
    std::cerr << "Environment variables REDIS_STREAM_CONSUMER_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD or REDIS_USE_SSL are not set." << std::endl;
    exit(1);
  }

  if (argc < 2)
  {
    std::cout << "Require cmd arg for unique id. ie. worker_$$_$count.." << std::endl;
    exit(1);
  }

  //   boost::asio::io_context main_ioc;
  //   boost::asio::signal_set sig_set(main_ioc.get_executor(), SIGINT, SIGTERM);
  // #if defined(SIGQUIT)
  //    sig_set.add(SIGQUIT);
  // #endif // defined(SIGQUIT)

  //   sig_set.async_wait(
  //     [&](const boost::system::error_code &, int)
  //     {
  //       D(std::cout << "Main ioc is signalled" << std::endl;)
  //       m_worker_shall_stop = 1;
  //       //awakener.stop();
  //     });

  //   auto main_ioc_thread = std::thread([&main_ioc]()
  //     { main_ioc.run(); });

  mt_logging::logger().log(
      {REDIS_STREAM_CONSUMER_LOGFILE,
       REDIS_STREAM_CONSUMER_LOGFILE,
       std::ios::out,
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
            {REDIS_STREAM_CONSUMER_LOGFILE,
             "Signal to Stopped",
             std::ios::app,
             true});
        continue;
      }

      // Now you actually have the data:
      const std::string &stream = work.stream;
      const std::string &id = work.id;
      const auto &fields = work.fields;
      mt_logging::logger().log(
          {REDIS_STREAM_CONSUMER_LOGFILE,
           fmt::format("- Consumer work item: [STREAM {}       ID {}]  Fields: {}", stream, id, fmt::join(fields, ", ")),
           std::ios::app,
           true});
      bool ok = false; // process_job(stream, fields);

      // if (ok)
      // {
      redisSubscribe.xack_now(stream, id);
      // } else {
      //   redisSubscribe.send_to_dlq_now(stream, id, fields);
      // }
    }
    mt_logging::logger().log(
        {REDIS_STREAM_CONSUMER_LOGFILE,
         "Awakener stop",
         std::ios::app,
         true});
//    awakener.stop();
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

  // main_ioc.stop();
  // if (main_ioc_thread.joinable())
  // {
  //   main_ioc_thread.join();
  // }
  // std::cout << "exiting\n";

  return result;
}
