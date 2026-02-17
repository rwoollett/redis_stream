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
}
