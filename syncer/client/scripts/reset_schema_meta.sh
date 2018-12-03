#!/bin/bash

# This script help to create 'db_rdp_schema' database in schema store

EC_OK=0
EC_UNKOWN_ERR=100
EC_PARAM_ERR=101
EC_DB_ERR=102

function exit_wrapper()
{
  local final_errno=$1
  local error=$2

  for f in ${files_to_rm[@]}; do
    rm -rf $f
  done

  if [ -z "$3" ]; then
    echo "{\"errno\":$final_errno,\"error\":\"$error\"}" 
  else
    echo "{\"errno\":$final_errno,\"error\":\"$error\",$3}" 
  fi
  exit $final_errno
}

function test_errno()
{
  local errno=$?
  if [ $errno -ne 0 ]; then
    local final_errno=$1
    local error=$2
    if [ $errno -ne 0 ]; then
      exit_wrapper $final_errno $error
    fi
  fi
}


ARGS=`getopt -o h:,P:,u:,p:,d: -- $0 $@`
test_errno $EC_PARAM_ERR "parse param failed"

eval set -- "${ARGS}"
while true; do
  case "$1" in
    -h)
      host=$2
      shift 2
      ;;
    -P)
      port=$2
      shift 2
      ;;
    -u)
      user=$2
      shift 2
      ;;
    -p)
      passwd=$2
      shift 2
      ;;
    -d)
      database=$2
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      exit_wrapper $EC_PARAM_ERR "parse param failed"
      ;;
  esac
done


if [ -z $host ] || [ -z $port ] || [ -z $user ] || [ -z $database ]; then
  exit_wrapper $EC_PARAM_ERR "exists some empty params"
fi

mysql_connect_str="-h$host -P$port -u$user"
if [ -n "$passwd" ]; then
  mysql_connect_str="${mysql_connect_str} -p$passwd"
fi

mysql ${mysql_connect_str}    << EOF
CREATE DATABASE IF NOT EXISTS $database ;
USE $database;
CREATE TABLE tb_rdp_schema (
schema_id int(11) NOT NULL AUTO_INCREMENT,
db_name varchar(256) NOT NULL DEFAULT '',
table_name varchar(256) NOT NULL DEFAULT '',
version int(11) NOT NULL DEFAULT '1' comment '该表的版本号，从1开始，每次表结构变更，版本号加1',
schema_value text comment '该表每个字段的属性, json格式',
statement text comment '该版本对应的sql语句',
gtid varchar(256) NOT NULL DEFAULT '' comment 'sql语句对应的gtid',
status int(11) NOT NULL DEFAULT '1',
PRIMARY KEY (schema_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE tb_ddl_gtid_executed (
ddl_gtid_executed longtext NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

INSERT INTO tb_ddl_gtid_executed VALUES('');

CREATE TABLE tb_schema_backup (
id int NOT NULL AUTO_INCREMENT,
schema_backup longblob NOT NULL,
PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ;
EOF

if [ $? -ne 0 ]; then
  exit_wrapper $EC_DB_ERR "execute sql failed"
fi

exit_wrapper $EC_OK "ok"
