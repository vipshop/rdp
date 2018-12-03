#/bin/bash
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

conf_file=$data_root/$instance_id/conf/syncer.cfg

if [ ! -e $data_root/$instance_id ]; then
    mkdir -p $data_root/$instance_id/conf
fi
if [ ! -e $data_root/$instance_id/logs ]; then
    mkdir -p $data_root/$instance_id/logs
fi
if [ ! -e $data_root/$instance_id/metrics ]; then
    mkdir -p $data_root/$instance_id/metrics
fi

if [ ! -e $conf_file ]; then
    echo "$conf_file does not exist"
    exit 1
fi




export LD_LIBRARY_PATH=../lib:$LD_LIBRARY_PATH
ulimit -c unlimited
ulimit -SHn 65535

#export GLOG_v=1 

./rdp_syncer $conf_file &
if [ $? -ne 0 ]; then
    echo "start failed"
    exit 1
fi
