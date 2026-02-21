#pragma once

#include <string>
#include <vector>
#include <unordered_map>

namespace WorkQStream
{

  struct GroupConfig
  {
    std::vector<std::string> streams;
  };

  using GroupConfigMap = std::unordered_map<std::string, GroupConfig>;


} /* namespace WorkQStream */
