
#pragma once

#ifdef NDEBUG
#define D(x)
#else
#define D(x) x
#endif

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <stdexcept>

#ifdef HAVE_ASIO
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/redis/connection.hpp>
#include <boost/redis/logger.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <thread>
#include <future>
#include <iostream>
#include <workqstream/common/Common.h>

namespace asio = boost::asio;
namespace redis = boost::redis;

namespace WorkQStream
{

  /**
   * Recovery logic is a batch job. Runs short lived Redis jobs
   */
  class Recovery
  {
    asio::io_context m_ioc;
    boost::asio::signal_set m_signals;
    std::thread m_receiver_thread;
    std::atomic<bool> m_signal_status;
    std::string m_worker_id;
    GroupConfigMap m_group_config{};
    std::unordered_set<std::string> m_valid_streams{};

  public:
    /// Constructor
    Recovery(const std::string &workerId);

    /// Deconstructor
    virtual ~Recovery();

    virtual bool is_signal_stopped() { return m_signal_status.load(); };
    void request_stop();
    void join();

  private:
    void push_dlq_xadd(redis::request &req,
                       const std::string &stream,
                       std::string_view id,
                       const std::unordered_map<std::string, std::string> &fields);
    std::shared_ptr<redis::connection> make_connection(const boost::asio::any_io_executor &ex, bool use_ssl);

    void setup_signals();
    asio::awaitable<void> run_recovery_mode(std::shared_ptr<redis::connection> conn);
    asio::awaitable<void> co_main();

    asio::awaitable<void> xack(std::string_view stream, std::string_view id,
                               std::shared_ptr<redis::connection> conn);
    asio::awaitable<void> send_to_dlq(std::string_view stream, std::string_view id,
                                      const std::unordered_map<std::string, std::string> &fields,
                                      std::shared_ptr<redis::connection> conn);

    asio::awaitable<void> recover_pending_with_conn(
        std::string stream,
        std::shared_ptr<redis::connection> conn);
    asio::awaitable<void> trim_stream_with_conn(
        std::string stream,
        std::shared_ptr<redis::connection> conn);
  };

} /* namespace WorkQStream */
#endif // HAVE_ASIO
