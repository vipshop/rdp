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



pid=$(ps -ef |  grep "[r]dp_syncer $data_root/$instance_id/conf/syncer.cfg" | awk '{print $2}')
if [ $? -ne 0 ]; then
    echo "find process failed"
    exit 1
fi

if [ -n "$pid"  ]; then
    kill $pid
    if [ $? -ne 0 ]; then
        echo "kill process failed"
        exit 1
    fi
fi



crontab -l | grep -v "check_alive.sh $instance_id" > cron.tmp
crontab cron.tmp

if [ $? -ne 0 ]; then
    echo "install crontab failed"
    exit 1
fi

rm -rf cron.tmp
