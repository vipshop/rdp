#!/bin/bash
# This script is used to take a snapshot of the Schema executer db
# WARNING: The snapshot do not contain these databases: mysql,P_S,I_S,sys

executer_db_host=$1
executer_db_port=$2
executer_db_user=$3
executer_db_password=$4
meta_db_database=$5

cd $(dirname $0)

if [ -z "$executer_db_host" ] || [ -z "$executer_db_port" ] || [ -z "$executer_db_user" ]; then
  echo "exists some empty params" >&2
  exit 1
fi

mysql_connect_str="-h$executer_db_host -P$executer_db_port -u$executer_db_user"
if [ -n "$executer_db_password" ]; then
    mysql_connect_str="${mysql_connect_str} -p$executer_db_password"
fi


if [ -z "$meta_db_database" ]; then
  need_to_drop=($(mysql $mysql_connect_str -Nse "select distinct(SCHEMA_NAME)  from information_schema.SCHEMATA where SCHEMA_NAME not in('mysql', 'information_schema', 'sys', 'performance_schema')"))
  if [ $? -ne 0  ]; then
    echo "Failed to execute mysql" >&2
    exit 1
  fi
else 
  # Maybe meta db and executer is the same mysql, so not need to delete the meta db
  need_to_drop=($(mysql $mysql_connect_str -Nse "select distinct(SCHEMA_NAME)  from information_schema.SCHEMATA where SCHEMA_NAME not in('mysql', 'information_schema', 'sys', 'performance_schema', \"$meta_db_database\")"))
  if [ $? -ne 0  ]; then
    echo "Failed to execute mysql" >&2
    exit 1
  fi
fi

for database in ${need_to_drop[@]}; do
  mysql $mysql_connect_str -Nse "drop database $database"
  if [ $? -ne 0  ]; then
    echo "Failed to drop $database" >&2
    exit 1
  fi
done




