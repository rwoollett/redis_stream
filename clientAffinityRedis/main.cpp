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

  const std::string group = std::getenv("WORKER_GROUP");

  auto set_guard = [&](const std::string &stream,
                       const std::string &xid) -> bool
  {
    // processing:{stream}:{xid} = worker_id NX EX 30
    return redisConsumer.set_guard_key_now(stream, xid, worker_id, 30);
  };

  auto clear_guard = [&](const std::string &stream,
                         const std::string &xid)
  {
    redisConsumer.del_guard_key_now(stream, xid);
  };

  while (true)
  {
    if (redisConsumer.is_signal_stopped())
    {
      mt_logging::logger().log(
          {fmt::format("Consumer {} signaled to Stop {}", worker_id, 1),
           mt_logging::LogLevel::Info, true});
      return;
    }

    //
    // 1. Try to find oldest pending (global ordering)
    //
    std::atomic<bool> xp_pending_ready{false};
    std::string oldest_stream;
    std::string oldest_xid;

    redisConsumer.xpending_oldest_across_streams_now(
        {"liveposts_post_Create",
         "liveposts_moderate_Job"},
        group,
        [&](std::string stream, std::string xid)
        {
          oldest_stream = stream;
          oldest_xid = xid;
          xp_pending_ready.store(true);
        });

    for (int i = 0; i < 200; ++i)
    {
      if (redisConsumer.is_signal_stopped())
        return;

      if (xp_pending_ready.load())
        break;

      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    mt_logging::logger().log(
        {fmt::format("---  Pending status:    [WORKER {}   STREAM {}      XID {}  PENDING {}]",
                     worker_id, oldest_stream, oldest_xid, xp_pending_ready.load()),
         mt_logging::LogLevel::Debug, true});

    // a) timeout: call never completed
    if (!xp_pending_ready.load())
    {
      // fall back to XREADGROUP via awakener
      mt_logging::logger().log(
          {fmt::format("---  Wait (timeout):    [WORKER {}]", worker_id),
           mt_logging::LogLevel::Debug, true});

      WorkItem work = awakener.wait_broadcast();
      // delivered messages go into PEL; next loop we’ll see them
      mt_logging::logger().log(
          {fmt::format("---  Woke (timeout):    [WORKER {}    STREAM {}      XID {}]  Fields: {}",
                       worker_id, work.stream, work.id, fmt::join(work.fields, ", ")),
           mt_logging::LogLevel::Debug, true});

      continue;
    }
    // b) call completed but no pending (empty stream/xid)
    if (oldest_stream.empty() || oldest_xid.empty())
    {
      mt_logging::logger().log(
          {fmt::format("---  Wait (No pending): [WORKER {}]", worker_id),
           mt_logging::LogLevel::Debug, true});

      WorkItem work = awakener.wait_broadcast();
      mt_logging::logger().log(
          {fmt::format("---  Woke (No pending): [WORKER {}    STREAM {}      XID {}]  Fields: {}",
                       worker_id, work.stream, work.id, fmt::join(work.fields, ", ")),
           mt_logging::LogLevel::Debug, true});
      continue;
    }

    // 2) we have a real oldest pending → proceed with stealing
    mt_logging::logger().log(
        {fmt::format("---  Oldest pending:    [WORKER {}    STREAM {}      XID {}]",
                     worker_id, oldest_stream, oldest_xid),
         mt_logging::LogLevel::Debug, true});
    //
    // 3. Try to steal ownership of oldest (XCLAIM) - min-idle > 0 to let owners process
    //
    redisConsumer.xclaim_now(oldest_stream, group, worker_id, oldest_xid);

    // 4. Check current owner
    std::string owner = redisConsumer.xpending_owner_now(
        oldest_stream, group, oldest_xid);

    mt_logging::logger().log(
        {fmt::format("---  Steal ownership:   [WORKER {}    STREAM {}      XID {}     STEALED OWNER {}, BACKOFF {}]",
                     worker_id, oldest_stream, oldest_xid, owner, owner != worker_id),
         mt_logging::LogLevel::Info, true});

    if (owner != worker_id)
    {
      // someone else owns it → small backoff, then re-loop
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    //
    // 5. Acquire atomic guard (SETNX)
    //
    mt_logging::logger().log(
        {fmt::format("---  Guard attempt:     [WORKER {}   STREAM {}      XID {}]",
                     worker_id, oldest_stream, oldest_xid),
         mt_logging::LogLevel::Debug, true});

    if (!set_guard(oldest_stream, oldest_xid))
    {
      // someone else is already processing it
      mt_logging::logger().log(
          {fmt::format("---  guard failed:      [WORKER {}    STREAM {}      XID {}]",
                       worker_id, oldest_stream, oldest_xid),
           mt_logging::LogLevel::Info, true});

      // DO NOT XCLAIM again immediately
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }

    //
    // 6. Acquire network CS lock
    //
    {
      std::lock_guard<std::mutex> guard(cs_lock);

      if (redisConsumer.is_signal_stopped())
      {
        clear_guard(oldest_stream, oldest_xid);
        // Unlock CS
        return;
      }

      // 7. Re-check ownership inside CS
      owner = redisConsumer.xpending_owner_now(
          oldest_stream, group, oldest_xid);

      mt_logging::logger().log(
          {fmt::format("#&!  Owner recheck:     [WORKER {}    STREAM {}      XID {}    OWNER {}   CHECK {}]",
                       worker_id, oldest_stream, oldest_xid, owner, owner == worker_id),
           mt_logging::LogLevel::Debug, true});

      if (owner != worker_id)
      {
        mt_logging::logger().log(
            {fmt::format("#&!  Owner stolen:      [WORKER {}    STREAM {}      XID {}    OWNER {}   CHECK {}]",
                         worker_id, oldest_stream, oldest_xid, owner, owner == worker_id),
             mt_logging::LogLevel::Info, true});

        clear_guard(oldest_stream, oldest_xid);
        // Unlock CS
        continue; // someone stole it while we waited
      }

      //
      // 8. Process (DB work)
      //
      int td = 0;
      if (worker_id == "worker_3")
        td = 250;
      else if (worker_id == "worker_2")
        td = 120 + (rand() % 200);
      else
        td = 50;

      mt_logging::logger().log(
          {fmt::format("#&!  Process XID:       [WORKER {}    STREAM {}      XID {}  TIME {}]",
                       worker_id, oldest_stream, oldest_xid, td),
           mt_logging::LogLevel::Info, true});

      std::this_thread::sleep_for(std::chrono::milliseconds(td));

      if (redisConsumer.is_signal_stopped())
      {
        clear_guard(oldest_stream, oldest_xid);
        // Unlock CS
        return;
      }

      //
      // 9. XACK inside lock
      //
      auto fut = redisConsumer.xack_wait_now(oldest_stream, oldest_xid);
      auto ec = fut.get();
      mt_logging::logger().log(
          {fmt::format("#&!  xack wait ec       [WORKER {}    EC {}", worker_id, ec.message()),
           mt_logging::LogLevel::Debug, true});

      if (ec)
      {
        mt_logging::logger().log(
            {fmt::format("#&!  XACK failed:       [WORKER {}    STREAM {}      XID {}]",
                         worker_id, oldest_stream, oldest_xid, ec.message()),
             mt_logging::LogLevel::Error, true});
      }
      else
      {
        mt_logging::logger().log(
            {fmt::format("#&!  XACK OK            [WORKER {}    STREAM {}      XID {}]",
                         worker_id, oldest_stream, oldest_xid),
             mt_logging::LogLevel::Info, true});
      }
    }

    //
    // 10. Release atomic guard
    //
    clear_guard(oldest_stream, oldest_xid);

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
      // std::thread w3(worker_thread, "worker_3");

      w1.join();
      w2.join();
      // w3.join();

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
