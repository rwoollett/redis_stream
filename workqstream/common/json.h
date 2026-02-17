#pragma once

#include "Groups.h"
#include <string>
#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace WorkQStream
{

  inline void to_json(nlohmann::json &j, GroupConfig const &cfg)
  {
    j = nlohmann::json{
        {"streams", cfg.streams}};
  }

  // inline void from_json(nlohmann::json const &j, GroupConfig &cfg)
  // {
  //   j.at("streams").get_to(cfg.streams);
  // }

  inline void from_json(nlohmann::json const &j, GroupConfig &cfg)
  {
    if (!j.contains("streams"))
      throw std::runtime_error("GroupConfig missing required field: streams");

    const auto &arr = j.at("streams");
    if (!arr.is_array())
      throw std::runtime_error("'streams' must be an array");

    if (arr.empty())
      throw std::runtime_error("'streams' array cannot be empty");

    cfg.streams.clear();
    for (const auto &item : arr)
    {
      if (!item.is_string())
        throw std::runtime_error("All items in 'streams' must be strings");
      cfg.streams.push_back(item.get<std::string>());
    }
  }

  inline void to_json(nlohmann::json &j, GroupConfigMap const &map)
  {
    j = nlohmann::json::object();
    for (auto const &[groupName, cfg] : map)
    {
      j[groupName] = cfg;
    }
  }

  // inline void from_json(nlohmann::json const &j, GroupConfigMap &map)
  // {
  //   map.clear();
  //   for (auto it = j.begin(); it != j.end(); ++it)
  //   {
  //     map[it.key()] = it.value().get<GroupConfig>();
  //   }
  // }
  inline void from_json(nlohmann::json const &j, GroupConfigMap &map)
  {
    if (!j.is_object())
      throw std::runtime_error("Top-level JSON must be an object of groups");

    map.clear();

    for (auto it = j.begin(); it != j.end(); ++it)
    {
      const std::string groupName = it.key();
      const auto &groupObj = it.value();

      if (!groupObj.is_object())
        throw std::runtime_error("Group '" + groupName + "' must be an object");

      map[groupName] = groupObj.get<GroupConfig>();
    }
  }

} /* namespace WorkQStream */
