# Java SDK & Tools 详解

## 1. Java SDK

代码位于源码下java_sdk目录。

rdp_convert子项目为RDP事务处理逻辑，包括新旧数据格式兼容处理、重复数据过滤、无效数据过滤等。

kafka_0_8_example子项目为消费0.8.X Kafka集群示例代码，包含两种消费模式：

- 要求按事务特性顺序消费，业务通过SDK每次消费到一个事务，重复或者乱序数据会在SDK中过滤掉，要求业务按照SDK返回信息保存Kafka Offset。

- 不要求事务特性可重复消费，业务每次消费一个Kafka消息（可能是不完整事务），允许消费重复数据或者乱序数据（保证数据最终一致性），业务只需要定时记录Kafka Offset即可。

实例代码说明：

**多线程消费实例**

代码：

```
kafka_0_8_example/src/main/java/com/rdp/example/MultiThreadConsumerExample.java
```

执行：

```
java -Xms128m -Xmx256m -cp kafka_0_8_example-0.0.1-jar-with-dependencies.jar com.rdp.example.MultiThreadConsumerExample -b 192.168.0.1:2181 -c rdp_test -t default_topic_test -n 1 -s 300
```

参数说明：

| 参数 | 说明                                  |
| ---- | ------------------------------------- |
| -b   | Kafka集群对应Zookeeper地址，包括目录  |
| -c   | Consumer ID                           |
| -t   | 消费Topic列表，如果多个Topic用','分隔 |
| -n   | 消费线程数                            |
| -s   | 运行时间，单位秒                      |
| -e   | 要求事务特性消费                      |
| -p   | 打印详细事务信息                      |
| -m   | 打印Metric信息                        |
| -h   | 打印帮助信息                          |

**单线程消费实例**

代码：

```
kafka_0_8_example/src/main/java/com/rdp/example/SimpleConsumerExample.java
```

执行：

```
java -Xms128m -Xmx256m -cp kafka_0_8_example-0.0.1-jar-with-dependencies.jar com.rdp.example.SimpleConsumerExample -b 192.168.0.1:2181 -c rdp_test -t default_topic_test
```

参数说明：

| 参数 | 说明                                 |
| ---- | ------------------------------------ |
| -b   | Kafka集群对应Zookeeper地址，包括目录 |
| -c   | Consumer ID                          |
| -t   | 消费Topic，只能消费一个Topic         |
| -m   | 消费m个kafka消息后推出               |
| -e   | 要求事务特性消费                     |
| -h   | 打印帮助信息                         |

**代码编译**

```
cd java_sdk
mvn clean
mvn compile
mvn package
#jar包
java_sdk/kafka_0_8_example/target/kafka_0_8_example-0.0.1-jar-with-dependencies.jar
```

## 2. fulltopic_check

RDP数据连续性校验工具，根据MySQL Binlog的连续性验证Kafka中数据是否完整。

代码目录：tools/fulltopic_check

代码编译：

```
#先编译好syncer才能编译fulltopic_check
cd build
./build.sh fulltopic_check
./build.sh syncer_install
#可执行文件
package/syncer/bin/rdp-topic-check
#依赖动态库
package/syncer/lib/*.so
```

执行：

```
./rdp-topic-check -b 192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092 -p 0 -t default_topic_test -f 1.1.0 -o 0 -e 10000 -j ./out.json
```

参数说明：

| 参数 | 说明                                           |
| ---- | ---------------------------------------------- |
| -p   | 消费Topic对应Partition                         |
| -o   | 校验开始Offset                                 |
| -e   | 校验结束Offset                                 |
| -a   | 校验Kafka Topic中全部数据                      |
| -b   | Kafka Brokers List，逗号分隔                   |
| -t   | Kafka Topic                                    |
| -r   | 消费限流，单位：MB/s                           |
| -w   | 输出所有Transaction至csv文件                   |
| -d   | 开启Debug模式                                  |
| -f   | Kafka版本                                      |
| -m   | fetch.message.max.bytes                        |
| -g   | 日志目录                                       |
| -v   | 日志等级                                       |
| -j   | 输出校验结果json文件                           |
| -Z   | 默认解压Buffer大小，程序会根据消息大小自动调整 |

校验结果json文件说明：

```
{
    "broken_count":"1",#不连续点个数
    "event_miss":[

    ],#事务内不连续位点
    "start_offset":"0",#校验开始Offset
    "stop_offset":"10000000",#校验结束Offset
    "trans_miss":[
        {
            "next":{
                "GTID":"0-30511806-18791610756",#GTID
                "begin_offset":"752460530",#事务对应Kafka开始位置
                "end_offset":"752460530",#事务对应Kafka结束位置
                "epoch":"11",#RDP epoch
                "trans_seqno":"1006322553"#事务序号
            },#不连续点后Offset
            "pre":{
                "GTID":"0-30511806-18791609294",#GTID
                "begin_offset":"752460529",#事务对应Kafka开始位置
                "end_offset":"752460529",#事务对应Kafka结束位置
                "epoch":"11",#RDP epoch
                "trans_seqno":"1006321086"#事务序号
            }#不连续点前Offset
        }
    ]#事务间不连续位点
}
```

json中标记的不连续位点不一定是数据丢失位点，因为MySQL主从切换可能出现不连续位点（因为源数据的Binlog连续性已经发生了变化），但是数据是完整的，这时需要人工查看Kafka中的数据进行对比。

## 3. topic_translate

用于查看Kafka中数据

代码目录：tools/user_topic_trans

代码编译：

```
#先编译好syncer才能编译topic_translate
cd build
./build.sh topic_translate
./build.sh syncer_install
#可执行文件
package/syncer/bin/kafka_topic_trans
#依赖动态库
package/syncer/lib/*.so
```

执行：

```
./kafka_topic_trans -C -b 192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092 -p 0 -t default_topic_test -w -e -y 1.0.0 -o 2
```

参数说明：

| 参数 | 说明                                         |
| ---- | -------------------------------------------- |
| -C   | Consumer模式，Producer模式无效               |
| -L   | 展示Kafka Metadata                           |
| -S   | 过滤需要的Database                           |
| -T   | 过滤需要的Table                              |
| -t   | Kafka Topic                                  |
| -p   | Partition                                    |
| -b   | Kafka Brokers List，逗号分隔                 |
| -o   | 开始Offset                                   |
| -e   | Consumer完消息后自动退出                     |
| -d   | 设置Debug等级                                |
| -E   | 结束Offset                                   |
| -y   | Kafka版本                                    |
| -l   | 一次消费Kafka消息数                          |
| -B   | 启动按批次消费，配合-l一起使用               |
| -Z   | 默认解压Buffer大小，可以不设，程序会自动调整 |
| -J   | 过滤需要的操作                               |
| -c   | 过滤需要的列                                 |
| -D   | 使用简约模式输出                             |
| -w   | 输出Kafka Topic高低Offset                    |

一般输出模式输出示例：

```
Read msg at offset:230587299,Read index:1
Key:b7eac3ee-96e4-11e8-863b-c88d834bfdaf:399614738
********************KafkaPkg********************
Gtid:b7eac3ee-96e4-11e8-863b-c88d834bfdaf:399614738,Seqno:0,Split flag:0,Epoch:170,Transaction Seq No:399202685,Data length:948,Source data len:139947516921784,Checksum:-1
Transaction size:948, after decompressed:1527--------------------Transaction-----------------
Gtid:b7eac3ee-96e4-11e8-863b-c88d834bfdaf:399614738,Seqno:825306420,Position:144073369,Binlog File Name:mysql_bin.000229,Next position:144073832,Next binlog file name:mysql_bin.000229,Events number:5
Event Type[33]:Unknown,Database Name:,Table Name:,Timestamp:1539689959,Timestamp Of receipt:1539689981,Position:144073369,Binlog file name:mysql_bin.000229,Next position:144073434
Event Type[2]:Query,Database Name:test,Table Name:,Timestamp:1539689959,Timestamp Of receipt:1539689981,Position:144073434,Binlog file name:mysql_bin.000229,Next position:144073506,Sql:BEGIN
Event Type[19]:Table_map,Database Name:test,Table Name:drc_test1,Timestamp:1539689959,Timestamp Of receipt:1539689981,Position:144073506,Binlog file name:mysql_bin.000229,Next position:144073615
Event Type[32]:delete,Database Name:test,Table Name:drc_test1,Schema Id:1394,Timestamp:1539689959,Timestamp Of receipt:1539689981,Position:144073615,Binlog file name:mysql_bin.000229,Next position:144073801,Row count:1
[0]Before row-->Column count:28-->0:varchar_var:varchar(20):b504a|1:tinyint_var:tinyint(4):3|2:text_var:text:b504a|3:date_var:date:2018:01:01|4:smallint_var:smallint(6):27868|5:mediumint_var:mediumint(9):315313|6:id:bigint(20):195541|7:bigint_var:bigint(20):315313|8:float_var:float(10,2):15.85|9:double_var:double:15.845700000000000784|10:decimal_var:decimal(10,2):15.85|11:datetime_var:datetime:2018-10-16 19:38:06.0|12:update_time:timestamp:1539689886000|13:time_var:time:00:00:00.0|14:year_var:year(4):2018|15:char_var:char(10):b504a|16:tinyblob_var:tinyblob:b504a|17:tinytext_var:tinytext:b504a|18:blob_var:blob:b504a|19:mediumblob_var:mediumblob:b504a|20:mediumtext_var:mediumtext:b504a|21:longblob_var:longblob:b504a|22:longtext_var:longtext:b504a|23:enum_var:enum('3','2','1'):3|24:set_var:set('3','2','1'):1|25:bool_var:tinyint(1):1|26:varbinary_var:varbinary(20):b504a|27:bit_var:bit(64):315313
[0]After  row-->Column count:0-->
Event Type[16]:Xid,Database Name:,Table Name:,Timestamp:1539689959,Timestamp Of receipt:1539689981,Position:144073801,Binlog file name:mysql_bin.000229,Next position:144073832
--------------------Transaction End-------------

```

简约模式输出示例：

```
gtid:b7eac3ee-96e4-11e8-863b-c88d834bfdaf:399622987
timestamp|database|table|operation|id|binlog_file_name|position|kafka_topic|kafka_offset|lwm_offset|update_time
1539690058|drc_checkpoint_db|drc_ckt_10092|befor:update|736|mysql_bin.000303|975787423|default_channel_new_drc92|231153463|231153420|2018-10-16 19:39:57.0
1539690058|drc_checkpoint_db|drc_ckt_10092|after:update|736|mysql_bin.000303|980270802|default_channel_new_drc92|231158463|231158394|2018-10-16 19:40:58.0

```

## 4. generate_package

模拟造RDP事务包工具

代码目录：tools/generate_package

代码编译：

```
#先编译好syncer才能编译generate_package
cd build
./build.sh generate_package
./build.sh syncer_install
#可执行文件
package/syncer/bin/generate_package
#依赖动态库
package/syncer/lib/*.so
```

执行：

```
./generate_package -b 192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092 -t default_topic_test -y 1.1.0 -p 0  -c 1 -i "0" -e 1 -s 80100578 -Z -T '{"gtid": "b7eac3ee-96e4-11e8-863b-c88d834bfdaf:399614738", "seq": 2, "position": 80100578, "binlog_file_name": "mysql-bin.000008", "next_position": 80100937, "next_binlog_file_name": "mysql-bin.000008", "events": [{"event_type": 33, "timestamp": 1514369144, "timestamp_of_receipt": 1514369235, "ddl": 0, "position": 80100578, "next_position": 80100643, "binlog_file_name": "mysql-bin.000008", "server_id": 6522}, {"event_type": 2, "timestamp": 1514369144, "timestamp_of_receipt": 1514369235, "ddl": 0, "position": 80100643, "next_position": 80100727, "binlog_file_name": "mysql-bin.000008", "server_id": 6522, "sql_statement": "BEGIN"}, {"database_name": "vip_sales_digger", "table_name": "user_attention", "event_type": 19, "timestamp": 1514369144, "timestamp_of_receipt": 1514369235, "ddl": 0, "position": 80100727, "next_position": 80100811, "binlog_file_name": "mysql-bin.000008", "server_id": 6522}, {"database_name": "vip_sales_digger", "table_name": "user_attention", "event_type": 30, "schema_id": 28306, "timestamp": 1514369144, "timestamp_of_receipt": 1514369235, "ddl": 0, "position": 80100811, "next_position": 80100906, "binlog_file_name": "mysql-bin.000008", "server_id": 6522, "rows": [{"after": [{"name": "id","type": "bigint(20) unsigned", "value": "2"}, {"name": "oa_code", "type": "varchar(20)", "value": "admin后台操作"}, {"name": "department_id", "type": "bigint(20) unsigned", "value": "10"}, {"name": "brand_store_sn", "type": "varchar(20)", "value": "10007700"}, {"name": "vendor_code", "type": "varchar(50)", "value": "103530"}, {"name": "is_deleted", "type": "tinyint(4)", "value": "0"}, {"name": "create_time", "type": "timestamp", "value": "0"}, {"name": "update_time", "type": "timestamp", "value": "1514369144000"}]}]}, {"event_type": 16, "timestamp": 1514369144, "timestamp_of_receipt": 1514369235, "ddl": 0, "position": 80100906, "next_position": 80100937, "binlog_file_name": "mysql-bin.000008", "server_id": 6522}]}'
```

参数说明：

| 参数 | 说明                      |
| ---- | ------------------------- |
| -t   | Kafka Topic               |
| -p   | Partition                 |
| -b   | Kafka brokers list        |
| -d   | Debug设置                 |
| -y   | Kafka Version             |
| -c   | 分包个数                  |
| -i   | 分包序号                  |
| -e   | 版本号                    |
| -s   | Binlog Start Position     |
| -T   | Transaction json个数数据  |
| -Z   | 启用压缩模式              |
| -v   | 设置生成Transaction的版本 |

## 5. zk

Zookeeper读写工具

代码目录：tools/zk

代码编译：

```
#先编译好syncer才能编译zk
cd build
./build.sh zk
./build.sh syncer_install
#可执行文件
package/syncer/bin/zk
#依赖动态库
package/syncer/lib/*.so
```

执行：

```
./zk -b 192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181 -l /
./zk -b 192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181 -c /test 'value'
./zk -b 192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181 -g /test
./zk -b 192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181 -s /test 'new value'
./zk -b 192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181 -d /test 
```

参数说明：

| 参数 | 说明              |
| ---- | ----------------- |
| -b   | Zookeeper连接地址 |
| -c   | 创建节点          |
| -d   | 删除节点          |
| -g   | 读取节点          |
| -l   | 获取子节点        |
| -s   | 设置节点值        |