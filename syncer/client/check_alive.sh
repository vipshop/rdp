#!/bin/bash
data_root=/apps/svr/rdp_syncer/data

if [ $# -eq 1 ]; then
    instance_id=$1
elif [ $# -eq 2 ]; then
    instance_id=$1
    data_root=$2
else
    echo "param error"
    exit 1
fi



count=$(ps -ef |  grep "[r]dp_syncer $data_root/$instance_id/conf/syncer.cfg" | wc -l)
if [ $? -ne 0 ]; then
    exit 1
fi

if [ $count -eq 0 ]; then
    echo "$pid is not alive, start now"
    ../scripts/alarm.sh "rdp instance[$instance_id] not alive, start now"
    ./start.sh $instance_id $data_root
    exit $?
fi

