#!/bin/bash
# Run this (redisnet_go.sh) for sample redis pub/sub network with publisher and subscriber 

DIE=0
srcdir=`dirname $0`
test -z "$srcdir" && srcdir=.
pwd

(test -f ./build/clientRedis/ClientRedis) || {
  echo
  echo "**Error**: You must have a \"build/clientRedis\" folder with file \"ClientRedis\" built from CMakeLists"
  DIE=1
}
(test -f ./build/clientPublisher/ClientPublish) || {
  echo
  echo "**Error**: You must have a \"build/clientPublisher\" folder with file \"ClientPublish\" built from CMakeLists"
  DIE=1
}


if test "$DIE" -eq 1; then
  cd ..
  echo "Finished with failure"
  exit 1
fi

(docker compose up -d)

. ./set_env.sh


count=1
while [ $count -le 5 ]; do
  sleep .4
  (./build/clientRedis/ClientRedis > output_scrb_$$_$count.log 2>&1 &)
  ((count++))
done

sleep .4
(./build/clientPublisher/ClientPublish > output_publ_$$.log 2>&1 &)

cd ..
echo "Redisnet running in "\`$srcdir\'". Use redisnet_stop.sh to end the processes running."
echo "Type \"docker compose logs -f\" to show redis container logs."
exit 0



