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


# install crontab
path=$(pwd)
cmd="cd $path && ./check_alive.sh $instance_id $data_root > /dev/null  2>&1"

crontab -l | grep -v "$cmd" > cron.tmp
echo "* * * * * $cmd" >> cron.tmp
crontab cron.tmp
if [ $? -ne 0 ]; then
    echo "install crontab failed"
    exit 1
fi
rm -rf cron.tmp


