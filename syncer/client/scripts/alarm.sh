#!/bin/bash
export PATH=$PATH:/sbin/:/usr/local/bin/
if [ $# -ne 1 ]; then
  echo "param error"
  exit 1
fi

message=$1

fid=12216
app=rdp_syncer
key=bc77919a4386eca8d2b19544cadc32d4
alarm_time=$(date +"%Y-%m-%d %H:%M:%S")
host=$(ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6 | awk '{print $2}' | tr -d "addr:")
# 替换冒号为空格
message=${message//:/ }
message=${message:0:100}
message="From ${host}, ${message}"

data=$(cat <<EOF
{
	"pigeon":{
		"alarms":[
			{
				"fid":"$fid",
				"subject":"$message",
				"message":"$message",
				"priority":"2",
				"domain":"***.***.com",
				"transfer":"",
				"alarm_time":"$alarm_time"
			}
		],
		"app":{
			"source":"$app",
			"key":"$key"
		}
	}
}
EOF
)
data=$(echo $data)
curl -d "data=$data&requestType=json" 'http://*.**.***.com'

if [ $? -ne 0 ]; then
  echo "curl failed"
  exit 1
fi
