#pragma once

#include "Groups.h"
#include "ParseRedisResp.h"
#include "json.h"
#include <boost/asio/connect.hpp>
#include <mtlog/mt_log.hpp>

namespace asio = boost::asio;


namespace WorkQStream
{
  inline GroupConfigMap load_group_config()
  {
    const char *cfgEnv = std::getenv("REDIS_GROUP_CONFIG");
    if (!cfgEnv)
      throw std::runtime_error("REDIS_GROUP_CONFIG not set");

    try
    {
      nlohmann::json j = nlohmann::json::parse(cfgEnv);
      return j.get<GroupConfigMap>();
    }
    catch (const std::exception &ex)
    {
      throw std::runtime_error(std::string("Invalid REDIS_GROUP_CONFIG: ") + ex.what());
    }
  }

  inline GroupConfig const &get_worker_group(const GroupConfigMap &map)
  {
    const char *groupEnv = std::getenv("WORKER_GROUP");
    if (!groupEnv)
      throw std::runtime_error("WORKER_GROUP not set");

    std::string groupName = groupEnv;

    auto it = map.find(groupName);
    if (it == map.end())
      throw std::runtime_error("WORKER_GROUP '" + groupName + "' not found in config");

    return it->second;
  }

  inline std::string make_ops_error(
      std::string_view operation,
      std::string_view stream,
      std::string_view group,
      std::string_view worker,
      std::string_view redisError,
      std::string_view remediation)
  {
    std::ostringstream oss;
    oss << "[REDIS ERROR]\n"
        << "Operation: " << operation << "\n"
        << "Stream:    " << stream << "\n"
        << "Group:     " << group << "\n"
        << "Worker:    " << worker << "\n"
        << "Error:     " << redisError << "\n"
        << "Action:    " << remediation << "\n";
    return oss.str();
  }

  inline void validate_stream_or_throw(
      const std::string &stream,
      const std::unordered_set<std::string> &validStreams,
      const std::string &workerName)
  {
    if (validStreams.find(stream) == validStreams.end())
    {
      throw std::runtime_error(
          make_ops_error(
              "XADD",
              stream,
              "(n/a)",
              workerName,
              "Unknown stream",
              "Ensure the stream is defined in REDIS_GROUP_CONFIG"));
    }
  }

  inline bool verify_certificate(bool, asio::ssl::verify_context &)
  {
    return true;
  }
  // Helper to load a file into an SSL context
  inline void load_certificates(asio::ssl::context &ctx,
                                const std::string &ca_file,
                                const std::string &cert_file,
                                const std::string &key_file)
  {
    try
    {
      // Load trusted CA
      ctx.load_verify_file(ca_file);
      // Load client certificate
      ctx.use_certificate_file(cert_file, asio::ssl::context::pem);
      // Load private key
      ctx.use_private_key_file(key_file, asio::ssl::context::pem);
    }
    catch (const std::exception &e)
    {
      mt_logging::logger().log(
          {fmt::format("Consumer::load certiciates {}", e.what()),
           mt_logging::LogLevel::Info,
           true});
    }
  }

}
