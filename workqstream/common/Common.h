#pragma once

#include "Groups.h"
#include "json.h"

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
}
