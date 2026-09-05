#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <cmake build dir> affinity?"
  exit 1
fi

if test "$2" = "affinity"; then
  clientMode=clientAffinityRedis/ClientAffinityRedis
else
  clientMode=clientRedis/ClientRedis
fi

echo "Client mode: " $clientMode

cmakedir=$1
DIE=0
srcdir=`dirname $0`
test -z "$srcdir" && srcdir=.
pwd

(test -f ./$cmakedir/$clientMode) || {
  echo
  echo "**Error**: You must have a \"$cmakedir/$clientMode\" file built from CMakeLists"
  DIE=1
}
(test -f ./$cmakedir/clientProducer/ClientProducer) || {
  echo
  echo "**Error**: You must have a \"$cmakedir/clientProducer\" folder with file \"ClientProducer\" built from CMakeLists"
  DIE=1
}


if test "$DIE" -eq 1; then
  cd ..
  echo "Finished with failure"
  exit 1
fi

. ./set_env.sh

(docker compose up -d)
if compgen -G "output_*" > /dev/null; then 
  echo "Cleared output_*" 
  rm output_* 
fi

export MTLOG_LEVEL=error

sleep .4
export WORKER_RECOVER_PENDING=off
export MTLOG_LOGFILE=output_rs_consumer.log
(./$cmakedir/$clientMode > output_consumer.log 2>&1 &)

sleep .4
export WORKER_RECOVER_PENDING=on
export MTLOG_LOGFILE=output_rs_consumer_recovery.log
(./$cmakedir/clientRedis/ClientRedis worker_recovery > output_consumer_recovery.log 2>&1 &)

sleep .4
. ./set_env.sh
(./$cmakedir/clientProducer/ClientProducer > output_producer.log 2>&1 &)

cd ..
echo "Redisnet running in "\`$srcdir\'". Use redisnet_stop.sh to end the processes running."
echo "Type \"docker compose logs -f\" to show redis container logs."
exit 0



