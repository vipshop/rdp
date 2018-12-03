#!/bin/bash

# This script help to get a snapshot of the schema from the src mysqld,
# and return the corresponding GTID_EXECUTED in the form of
# '{"errno":0,"error":"ok","gtid_executed":"ccb0bb83-7370-11e7-8294-fa163e735fe2:1-5"}'

EC_OK=0
EC_UNKOWN_ERR=100
EC_PARAM_ERR=101
EC_DB_ERR=102
EC_PUMP_ERR=103
EC_EXCEED_ERR=104


# Register the file that need to delete at exit
function rm_at_exit()
{
  files_to_rm=(${files_to_rm[@]} $1)
}

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
      exit_wrapper $final_errno "$error"
    fi
  fi
}


function get_ddl_count()
{
  local count=0
  sql_result=$(mysql $src_mysql_connect_str -Nse "SHOW GLOBAL STATUS WHERE variable_name LIKE 'Com_create%' OR variable_name LIKE 'Com_alter%' OR variable_name LIKE 'Com_drop%' OR variable_name LIKE 'com_rename%';" )
  if [ $? -ne 0 ]; then
    exit $EC_DB_ERR
  fi

  count=$(echo -n "$sql_result" | awk 'BEGIN{sum=0;}{sum+=$2}END{print sum;}')
  if [ $? -ne 0 ]; then
    exit $EC_UNKOWN_ERR
  fi

  echo $count
}



ARGS=`getopt -l src-host:,src-port:,src-user:,src-passwd:,dst-host:,dst-port:,dst-user:,dst-passwd:,dst-database:,reset-dst -- $0 $@`
test_errno $EC_PARAM_ERR "parse param failed"

eval set -- "${ARGS}"
while true; do
  case "$1" in
    --src-host)
      src_host=$2
      shift 2
      ;;
    --src-port)
      src_port=$2
      shift 2
      ;;
    --src-user)
      src_user=$2
      shift 2
      ;;
    --src-passwd)
      src_passwd=$2
      shift 2
      ;;
    --dst-host)
      dst_host=$2
      shift 2
      ;;
    --dst-port)
      dst_port=$2
      shift 2
      ;;
    --dst-user)
      dst_user=$2
      shift 2
      ;;
    --dst-passwd)
      dst_passwd=$2
      shift 2
      ;;
    --dst-database)
      dst_database=$2
      shift 2
      ;;
    --reset-dst)
      reset_dst=true
      shift 1
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

if [ -z $src_host ] || [ -z $src_port ] || [ -z $src_user ]; then
  exit_wrapper $EC_PARAM_ERR "exists some empty params"
fi
if [ -z $dst_host ] || [ -z $dst_port ] || [ -z $dst_user ] || [ -z $dst_database ]; then
  exit_wrapper $EC_PARAM_ERR "exists some empty params"
fi

# Connect string to connect the mysql
src_mysql_connect_str="-h$src_host -P$src_port -u$src_user"
if [ -n "$src_passwd" ]; then
  src_mysql_connect_str="${src_mysql_connect_str} -p$src_passwd"
fi
dst_mysql_connect_str="-h$dst_host -P$dst_port -u$dst_user"
if [ -n "$dst_passwd" ]; then
  dst_mysql_connect_str="${dst_mysql_connect_str} -p$dst_passwd"
fi

if [ "$reset_dst" == "true" ]; then
  ./reset_schema_meta.sh  $dst_mysql_connect_str -d$dst_database > /dev/null
  test_errno $EC_DB_ERR "reset schema store failed"
fi


# Temp file to store the mysqldump output
sql_file_name=${src_host}_${src_port}_$(date +%Y%m%d%H%M%S%N).sql
rm_at_exit $sql_file_name

#Check if exists db_rdp_schema
count=$(mysql $src_mysql_connect_str -Nse "SELECT count(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA like \"$dst_database\"")
test_errno $EC_DB_ERR "check db_rdp_schema failed"
if [ $count -ne 0 ]; then
  exit_wrapper $EC_DB_ERR "db_rdp_schema already exists in src db"
fi


i=0
got_it=false
while [ $i -lt 5 ]; do
  ((i += 1))

  ddl_count1=$(get_ddl_count)
  test_errno $EC_DB_ERR "get ddl count failed"
  echo "ddl count before mysqldump: $ddl_count1" >&2

  # Get snapshot of the schema from src mysql
  ./mysqldump $src_mysql_connect_str   --all-databases --skip-lock-tables --add-drop-database --add-drop-table --no-data --set-gtid-purged=OFF --skip-triggers  --ignore-database=mysql --ignore-database=sys --ignore-database=$dst_database > $sql_file_name
  test_errno $EC_PUMP_ERR "execute mysqldump failed"

  gtid_pos=$(mysql $src_mysql_connect_str -Nse "SELECT @@global.gtid_executed;")
  test_errno $EC_DB_ERR "get gtid pos failed"

  ddl_count2=$(get_ddl_count)
  test_errno $EC_DB_ERR "get ddl count failed"
  echo "ddl count after mysqldump: $ddl_count2" >&2

  if [ $ddl_count1 == $ddl_count2 ]; then
    got_it=true
    break;
  fi
done

if [ $got_it == false ]; then
  exit_wrapper $EC_EXCEED_ERR "try times exceeded"  
fi

# Load schema into dst mysql

export LD_LIBRARY_PATH=../lib:$LD_LIBRARY_PATH
./save_file_into_mysql $dst_mysql_connect_str -d$dst_database -ttb_schema_backup -cschema_backup -f$sql_file_name 
test_errno $EC_DB_ERR "insert backup file into meta db failed"

# Output the GTID_EXECUTED 
exit_wrapper $EC_OK "ok" "\"gtid_binlog_pos\":\"$gtid_pos\""



