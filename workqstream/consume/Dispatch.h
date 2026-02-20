#pragma once

#include <string>
#include <vector>
#include <boost/redis/connection.hpp>

namespace redis = boost::redis;

namespace WorkQStream
{

  struct DispatchView
  {
    std::string_view stream;
    std::string_view id;
    std::vector<std::pair<std::string_view, std::string_view>> fields;
  };

  std::vector<DispatchView> parse_dispatch_view(const redis::generic_response &resp);

}