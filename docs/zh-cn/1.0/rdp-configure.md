# RDP高端定制

## 1.多副本部署

RDP采用Zookeeper的Session timeout机制来实现Leader Election，缩短系统不可用时间，提高系统可用性。使用多副本部署注意点如下：

- 使用相同group.id，副本间使用syncer.host进行区分。

- 只能同步同一个数据库，即mysql.vip、mysql.port、mysql.user、mysql.password、mysql.version、syncer.uuid、server.id相同。

- 使用同一个Schema Store，即schema.meta.db.host、schema.meta.db.port、schema.meta.db.user、schema.meta.db.password、schema.meta.db.database相同。
- Zookeeper配置相同，即zk.hosts、zk.recvtimeout、node.list.path、leader.path、filter.path、checkpoint.path、checkpoint.interval_ms相同。
- Kafka配置相同，即kafka.b_version_fback、kafka.brokerlist、kafka.topic、kafka.partition等相同。
- 压缩功能配置相同，即compress.enable相同。
- 分包功能配置相同，即kafka.producer.enable_split_msg相同。

## 2.过滤规则配置

RDP可以根据配置，对Database、Table、Column级别的有效数据进行下发。对于不在配置内的事务，只下发事务基本信息，对于只包含部分有效数据的事务，只下发无效Event的基本信息和有效的Event（保证数据的连续性，需要消费端忽略这部分无效数据）。过滤规则以json格式保存于Zookeeper的filter.path中。过滤规则格式如下：

```
{
    "database":{
        "table":[
			"column_1",
			"column_2"
        ]
    }
}
```

配置例子：

```
[zk: localhost:2181(CONNECTED) 1] set /rdp_syncer/10001/filter/included '{"database":{"table":["column_1","column_2"]}}' -1
```

- 同步table下所有column配置：

```
{
    "database":{
        "table":[
        ]
    }
}
```



- 同步多个分库相同table配置：

```
{
    "":{
        "table":[
        ]
    }
}
```

- 同步database下所有表：

```
{
    "database":{
        "":[
        ]
    }
}
```

- 同步多个分库相同column：

```
{
    "":{
        "":[
			"column_1",
			"column_2"
        ]
    }
}
```

## 3.启用分包配置

由于Kafka消息大小有限制（默认1000000），RDP是以事务的粒度同步数据，所以存在单个事务数据（通过protobuf打包后的解析数据）超出Kafka消息大小的限制。为了解决上面问题，RDP以行为最小粒度（对于update事件包含前后镜像），对事务进行分包，将一个事务分保存至多个Kafka消息，每个消息可以独立解析，也可以重新合并为一个事务，具体处理方式根据业务要求处理。对于单行数据大小超出Kafka消息大小的，RDP将过滤这部分数据（写日志+调用Alarm.sh脚本告警），如果一定要支持，建议修改Kafka消息大小。

相关配置：

- 开启分包功能：kafka.producer.enable_split_msg=1，0为不启用，1启用

- 单个分包大小：kafka.producer.split_msg_bytes=900000，略小于Kafka消息大小kafka.producer.msg_max_bytes=1000000

## 4.启用压缩配置

RDP使用LZ4压缩算法进行压缩，使用独立的压缩方式，与Kafka压缩相互独立，两者没有任何关系。

- 启用压缩功能：compress.enable=1

- 设置默认解压Buffer大小：decompress.max.buf.size=10485760，大约为Kafka消息大小*5，RDP内部会对Buffer进行调整。

## 5.修改Kafka默认最大消息大小配置

如果事务数据过大，需要调整Kafka消息大小容纳更大的事务数据。

- 修改consumer消息大小：kafka.consumer.fetch_max_msg_bytes
- 修改producer消息大小：kafka.producer.msg_max_bytes
- 如果启用了分包功能，还需要修改分包大小：kafka.producer.split_msg_bytes、decompress.max.buf.size

## 6.MySQL限流配置

在RDP同步存量数据时，由于RDP处理速度较快，网路流量较大，可能对源数据库产生影响，需要对同步进行限流。

- 修改mysql.max.rate，默认10MB/s

## 7.多RDP共享Schema Store配置

RDP采用存储与运算分离的架构处理Schema，所以可以多个RDP共享一个高可用的Schema Store数据库实例，每个RDP组使用一个database。但是每个RDP的Execute DB必须独立，不然可能存在相互影响的可能。

- schema.meta.db.host

- schema.meta.db.port

- schema.meta.db.user

- schema.meta.db.password

- schema.meta.db.database，每个RDP组相同

## 8.调整解析线程配置

RDP使用MTS架构解析、压缩、打包Binlog数据，可以通过增加处理线程加快提高处理速度。

- parsing.threads.num

## 9.Binlog Dump配置

RDP提供将同步的Binlog写到本地的功能，如没特殊需要，默认不启用。如需启用需要如下配置：

- dump.local.file=1

- sync.method=1

## 10.启用MySQL Semi-sync Replication配置

RDP使用MySQL原生的同步协议进行同步，所以也提供半同步功能，默认不启用。如需启用需要如下配置：

- semi.sync=1

## 11.处理最大事务大小配置

RDP是以事务粒度处理Binlog数据，需要缓存完整事务数据再进行后续处理。为了避免超大事务对RDP的冲击（耗尽内存），需要对大事物进行限制。对于超出单个最大事务限制事务，直接忽略，至保留事务基本信息。

- trx.max_trans_size，单事务最大阈值
- memory.pool.size，RDP进程内存池阈值，根据物理内存配置

## 12.RDP内部缓冲队列配置

RDP有两个缓存队列，分别用于解析线程和kafka producer线程缓冲。如果数据库事务大小差异较大，或者网络带宽抖动厉害，可以适当增加缓存队列。

- trx.buffer.size，缓冲同步线程和解析线程间差异

- msg.map.size，缓冲解析线程和producer线程间差异

## 13.Checkpoint时间配置

RDP定时将处理进度保存到Zookeeper，默认为5s，如果时间过长，会增加Crash Recovery的时间，如果时间太短，会增加Zookeeper压力。

- checkpoint.interval_ms

## 14.Kafka Producer Ack配置

不同Kafka Ack会影响Producer的性能，如果对数据一致性要求不高（理论上Ack！=-1的情况下发生Kafka Leader切换，有丢数据的可能），可以降低Ack数，提高Producer的速度。如果设为0，Checkpoint中的Kafka offset信息一直为0，会大大增加Crash Recovery的时间。建议不能设置为0，默认-1。

- kafka.producer.acks

## 15.Kafka Producer批量发送设置

Librdkafka具有批量发送功能，如果对实时性要求不高，同时网络延时大，带宽足的情况，可以适当加大单批消息数量，提高处理速度

- kafka.producer.batch_num_msg，单批次最大消息数

- kafka.producer.qbuf_max_ms，单批次等待最长时间

## 16.不同Kafka版本配置

通过修改Kafka版本配置支持不同版本

- kafka.b_version_fback

## 17.Crash Recovery Consumer限流配置

RDP在Crash Recovery时会去消费Kafka的数据，如果消费速度过快会影响broker性能。默认100MB。可以根据网络状态和broker性能进行适当调整，会影响Crash Recovery的时间。

- kafka.comsumer.rate.limit.mb

## 18.Kafka Producer 缓冲配置

Librdkafka有内部缓存队列，用于缓存发送消息，修改该配置可以适当减缓网络等原因造成的影响。

- kafka.producer.q_buf_max_msgs，最大缓存消息

- kafka.producer.q_buf_max_kb，最大缓存消息总大小

## 19.Kafka Producer异常重试次数配置

Librdkafka Producer异常后重试次数，默认值为0，如果网络不稳定，可以适当增加重试次数，但可能会导致Kafka中存在重复数据。

- kafka.producer.send_max_retry

## 20.Kafka Producer无响应Timeout配置

如果一直有数据写到Kafka，超过一定时间内没有响应，可以认为Kafka出现异常。该时间不能设置太短，理论上不能小于RDP所在机器与Kafka borker间的RTT*2。

- kafka.no_rsp_timeout_ms

## 21.Kafka Producer Timeout是否重试配置

当Kafka Producer遇到Timeout时是否马上退出，对于很不稳定的网络情况，且对数据一致性要求不高的场景，可以启用。如果启用，在连续遇到超过5个timeout的消息，RDP才会触发切换流程。默认启用。

- kafka.producer.quit_when_timeout

## 22.空事务合并配置

对于启用了过滤规则，并且产生很多空事务，过多的空事务会对Kafka Producer产生压力，也消耗大量的存储资源。RDP每次在Encode Map中获取一定量的事务，如果一批次中有多个连续的空事务，会将多个连续的空事务合并为一个事务，写到Kafka。必须启用kafka.producer.enable_trans_clean和kafka.producer.enable_trans_merge才能进行空事务合并。如果启用分包，启用了空事务合并也无效。

- kafka.producer.enable_trans_merge=1，是否允许合并空事务，默认不启用

- kafka.producer.pop_map_msgs=20，单次从Encode Map中获取事务最大个数

- kafka.producer.enable_trans_clean=1，是否允许清空不关注事务，

## 23.Kafka Debug开启配置

启用Libradkafka Debug日志

- kafka.debug=all，debug level:generic, broker, topic, metadata, queue, msg, protocol, cgrp, security, fetch, feature, all

## 24.Slave Read After Write配置

Read After Write功能为RDP内部会回去Master下指定Slave的同步进度，如果Slave已经同步完成，或者超过一定时间阈值，再将事务写到Kafka。

- slaveview.read_after_write=0，是否启用read after write

- slaveview.slave_server_ids=123456,123457，监控slave server id列表

- slaveview.refresh_view_interval=60000，更新slave list时间间隔

- slaveview.wait_timeout=5000，等待slave同步时间最大值