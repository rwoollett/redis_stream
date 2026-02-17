#!/bin/sh
# Run this for stopping sample nmtoken network on multiple nodes
if [ -z "$1" ]; then
  echo "Usage: $0 <cmake build dir>"
  exit 1
fi

cmakedir=$1
kill $(ps -ef | grep [.]/$cmakedir/client | awk 'BEGIN{}{if (FNR==NR) {codes[$2]=$2;} else{next}}END{i=0;for (key in codes){i++;if (i<=6 && i<=length(codes)) {print codes[key]}}}')
(docker compose down)