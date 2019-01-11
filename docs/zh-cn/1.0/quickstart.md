# RDP快速开始
## 1. RDP简介
数据订阅服务RDP(Real-time Data Pipeline)

- 我们不生产数据，我们只是数据搬运工

RDP同步数据库Binlog数据，经事务边界解析、Binlog翻译、过滤、分包、压缩、打包等操作，最终生成Protobuf格式序列化数据，推送至下游消息系统。

RDP系统架构图如下：

![system_struct](../1.0/pictures/system_struct.png)
 - 数据源：RDP模拟Slave的同步方式，作为数据源的Slave拉取Binlog数据。
 - RDP：主从多节点模式，多线程并发解析Binlog，保证数据实时性。
 - Schema Store & Execute DB：Store DB用于存储源数据库表结构信息，最好是高可用的MySQL实例；Execute DB用于回放Binlog中的DDL，无需高可用。计算和存储分离，这样多个RDP共享一个高可用的Schema Store DB, 对于大规模RDP部署可以降低对MySQL实例的依赖。
 - Zookeeper：RDP节点间通过Zookeeper进行选主，在RDP Leader异常时自动进行切换，保证持续服务。同时定时记录同步进度(checkpoint)到Zookeeper，加快服务恢复。
 - Kafka：RDP使用原生Kafka协议写到单个Topic单个Partition，保证数据顺序。(对于需要对分片数据库合并到一个Topic，建议不同分片数据库写到同一个Topic的不同Partition)。


## 2. RDP的应用场景

 - 数据同步：同步数据库Binlog数据，解析后写入Kafka，业务消费Kafka获取数据库增量数据。

![scenario_1](../1.0/pictures/scenario_1.png)

 - 事件通知：通过RDP同步增量数据库数据至Kafka，业务消费Kafka，获取DataBase/Table/Column/Value的变动情况。

![scenario_2](../1.0/pictures/scenario_2.png)

 - 读一致性事件通知：RDP会主动等待Slave的同步进度，等待指定Slave追上同步进度或者超过一定阈值，才将事务写到Kafka。

![scenario_3](../1.0/pictures/scenario_3.png)

## 3. 接入要求

1. MySQL版本要求>=5.7.17
2. 确保开启GTID
3. 确保BINLOG_FORMAT为ROW格式
4. 确保BINLOG_ROW_IMAGE为FULL
5. 提供如下权限的用户:
```
GRANT SELECT, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, SHOW VIEW, EVENT ON *.* TO 'rdp'@'10.%' identified  by  ...;
```

## 4. 快速上手
执行以下命令，启动源数据库、RDP、Kafka容器:

```
cd docker-compose && docker-compose up
```
容器启动完成之后，进入到RDP容器:
```
docker exec -it rdp bash
```

进入容器之后，执行如下命令启动RDP进程:
```
cd /apps/svr/rdp_syncer/base/rdp_mysql/bin && ./start.sh 10000
```

RDP进程启动之后，在另一个终端执行以下命令，在源数据库执行SQL语句产生binlog:
```
docker exec -it source_db mysql -pyourpassword -Nse 'flush privileges'

```

此时可以查看RDP的输出，检查同步进度是否正常推进。



