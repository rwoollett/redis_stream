#!/bin/sh
# Run this for stopping sample nmtoken network on multiple nodes

kill $(ps -ef | grep [.]/build/client | awk 'BEGIN{}{if (FNR==NR) {codes[$2]=$2;} else{next}}END{i=0;for (key in codes){i++;if (i<=6 && i<=length(codes)) {print codes[key]}}}')
(docker compose down)