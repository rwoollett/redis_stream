#include "Dispatch.h"
#include <iostream>

namespace WorkQStream
{

  void trace_parse(
      const redis::generic_response &resp,
      int index,
      const boost::redis::resp3::node &n)
  {
    auto ancestorNode = (index > 1) ? resp.value().at(index - 2) : n;
    auto prevNode = (index > 0) ? resp.value().at(index - 1) : n;
    std::cout << "\n----parse_dispatch_view-----------------------------------" << std::endl;
    std::cout << "Reference " << index << std::endl;
    if (ancestorNode != n)
    {
      std::cout << "index " << index << " ancestor node index " << index - 2 << std::endl;
      std::cout << " value          " << ancestorNode.value << std::endl;
      std::cout << " data_type      " << ancestorNode.data_type << std::endl;
      std::cout << " aggregate_size " << ancestorNode.aggregate_size << std::endl;
      std::cout << " depth          " << ancestorNode.depth << std::endl;
      std::cout << std::endl;
    }
    if (prevNode != n)
    {
      std::cout << "index " << index << " previous node index " << index - 1 << std::endl;
      std::cout << " value          " << prevNode.value << std::endl;
      std::cout << " data_type      " << prevNode.data_type << std::endl;
      std::cout << " aggregate_size " << prevNode.aggregate_size << std::endl;
      std::cout << " depth          " << prevNode.depth << std::endl;
      std::cout << std::endl;
    }
    std::cout << "index " << index << " current node" << std::endl;
    std::cout << " value          " << n.value << std::endl;
    std::cout << " data_type      " << n.data_type << std::endl;
    std::cout << " aggregate_size " << n.aggregate_size << std::endl;
    std::cout << " depth          " << n.depth << std::endl;
    std::cout << std::endl;
  }

  std::vector<DispatchView>
  parse_dispatch_view(const redis::generic_response &resp)
  {
    std::vector<DispatchView> out;

    std::string_view current_stream;
    DispatchView current_msg;
    std::string_view current_key;
    int index = 0;

    for (auto const &n : resp.value())
    {

      // DEBUG
      //trace_parse(resp, index, n);
      // STREAM NAME
      if (n.depth == 1 &&
          n.data_type == boost::redis::resp3::type::blob_string)
      {
        current_stream = n.value;
        continue;
      }

      // MESSAGE ID
      if (n.depth == 3 &&
          n.data_type == boost::redis::resp3::type::blob_string)
      {
        // If we already have a message pending, push it
        if (!current_msg.id.empty())
        {
          out.push_back(std::move(current_msg));
          current_msg = DispatchView{};
        }

        current_msg.stream = current_stream;
        current_msg.id = n.value;
        continue;
      }

      // FIELD KEY/VALUE
      if (n.depth == 4 &&
          n.data_type == boost::redis::resp3::type::blob_string)
      {
        if (current_key.empty())
        {
          current_key = n.value;
        }
        else
        {
          current_msg.fields.emplace_back(current_key, n.value);
          current_key = {};
        }
        continue;
      }
      index++;
    }

    // Push last message
    if (!current_msg.id.empty())
    {
      out.push_back(std::move(current_msg));
    }

    return out;
  }

}