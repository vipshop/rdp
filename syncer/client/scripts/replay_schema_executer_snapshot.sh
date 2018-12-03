#!/bin/bash
PATH=$PATH:/usr/local/bin
meta_db_host=$1
meta_db_port=$2
meta_db_user=$3
meta_db_password=$4
meta_db_database=$5

executer_db_host=$6
executer_db_port=$7
executer_db_user=$8
executer_db_password=$9

if [ -z "$meta_db_host" ] || [ -z "$meta_db_port" ] || [ -z "$meta_db_user" ] || [ -z "$meta_db_database" ]; then
  echo "exists some empty params" >&2
  exit 1
fi

if [ -z "$executer_db_host" ] || [ -z "$executer_db_port" ] || [ -z "$executer_db_user" ] ; then
  echo "exists some empty params" >&2
  exit 1
fi

meta_db_connect_str="-h$meta_db_host -P$meta_db_port -u$meta_db_user"
if [ -n "$meta_db_password" ]; then
    meta_db_connect_str="${meta_db_connect_str} -p$meta_db_password"
fi
executer_db_connect_str="-h$executer_db_host -P$executer_db_port -u$executer_db_user"
if [ -n "$executer_db_password" ]; then
    executer_db_connect_str="${executer_db_connect_str} -p$executer_db_password"
fi

count=$(mysql $meta_db_connect_str $meta_db_database -Nse 'select count(*) from tb_schema_backup')
if [ "$count" != "1" ]; then 
  echo "The numer of backup file is not equal to one" >&2
  exit 1
fi

mysql $meta_db_connect_str $meta_db_database -NEe 'select schema_backup from tb_schema_backup order by id desc limit 1' | tail -n +2 | mysql $executer_db_connect_str
if [ $? -ne 0  ]; then
  echo "Failed to execute mysql" >&2
  exit 1
fi
