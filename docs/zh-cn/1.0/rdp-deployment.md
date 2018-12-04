# RDP 安装部署流程

## 1. RDP编译

执行以下命令，下载源码并编译:

```
git clone https://github.com/vipshop/rdp.git
cd rdp
make && make install
```

可执行文件包在package目录下:
```
ls package/
rdp_mysql.20180822.tgz
```


## 2. RDP部署

RDP系统架构图如下：

![system_struct](../1.0/pictures/system_struct.png)

从架构图可以看出，RDP依赖Zookeeper集群、Kafka集群、Schema Store数据库实例、Execute DB数据库实例。其中Execute DB数据库版本最好与数据源数据库版本一致（因为Execute DB需要回放DDL），与Schema Store数据库版本可以不同。Schema Store与Execute DB可以**共用**一个数据库实例。


这里以如下所述环境为例，进行部署演示：
* RDP版本：rdp_mysql.20180822
* RDP实例ID：10001 
* 源数据库版本：mysql-5.7.22 
* 源数据库IP:127.0.0.1, Port:3306, User:\*\*\*, Passwd:\*\*\* 
* Schema Store/Execute DB IP:127.0.0.1, Port:10001, User:\*\*\*, Passwd:\*\*\* 
* Zookeeper集群：192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181 
* Kafka集群：192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092
* Kafka Topic：topic_10001，Partition:0

> 这里的部署流程不包含Zookeeper、Kafka、数据库（Schema Store/Execute DB）部署流程，需要提前准备

下面以上述环境作为例子进行部署说明：

### 2.1 安装MySQL Client

```
sudo yum install -y mysql
```


### 2.2 解压安装包

拷贝RDP安装包至/apps/svr/rdp_syncer/base, 并解压:

```
mkdir -p /apps/svr/rdp_syncer/base
mkdir rdp_mysql.20180822 && tar zxvf rdp_mysql.20180822.tgz -C /apps/svr/rdp_syncer/base/rdp_mysql.20180822
```

### 2.3 修改配置

```
#拷贝配置文件
mkdir -p /apps/svr/rdp_syncer/data/10001/conf
cp /apps/svr/rdp_syncer/base/rdp_mysql.20180822/syncer.cfg.example /apps/svr/rdp_syncer/data/10001/conf/syncer.cfg
#修改配置文件
vi /apps/svr/rdp_syncer/data/10001/conf/syncer.cfg
```

主要修改配置项：

| 配置项                          | 说明                                               | 值                                                 |
| :------------------------------ | :------------------------------------------------- | -------------------------------------------------- |
| mysql.vip                       | 源数据库IP                                         | 127.0.0.1                                          |
| mysql.port                      | 源数据库端口                                       | 3306                                               |
| mysql.user                      | 源数据库用户名                                     | \*\*\*                                             |
| mysql.password                  | 源数据库密码                                       | \*\*\*                                             |
| mysql.version                   | 源数据库版本                                       | 5.7.22-log                                         |
| group.id                        | RDP集群ID                                          | 10001                                              |
| syncer.host                     | RDP所在机器IP，用于标识RDP进程                     | 192.168.0.4                                        |
| log.dir                         | 日志目录                                           | /apps/svr/rdp_syncer/data/10001/logs               |
| schema.meta.db.host             | schema store ip，可以与execute db相同              | 127.0.0.1                                          |
| schema.meta.db.port             | schema store端口，可以与execute db相同             | 10001                                              |
| schema.meta.db.user             | schema store用户名，可以与execute db相同           | \*\*\*                                             |
| schema.meta.db.password         | schema store密码，可以与execute db相同             | \*\*\*                                             |
| schema.meta.db.database         | schema store database                              | rdp_schema_meta_10001                          |
| schema.executer.db.host         | execute db ip                                      | 127.0.0.1                                          |
| schema.executer.db.port         | execute db端口                                     | 10001                                              |
| schema.executer.db.user         | execute db用户名                                   | \*\*\*                                             |
| schema.executer.db.password     | execute db密码                                     | \*\*\*                                             |
| zk.hosts                        | zookeeper连接地址                                  | 192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181 |
| node.list.path                  | zookeeper RDP集群所有本节点目录，单RDP集群独享     | /rdp_syncer/10001/nodes                            |
| leader.path                     | zookeeper RDP集群leader节点目录，单RDP集群独享     | /rdp_syncer/10001/leader                           |
| filter.path                     | zookeeper过滤规则目录，单RDP集群独享               | /rdp_syncer/10001/filter                           |
| checkpoint.path                 | zookeeper checkpoint保存目录，单RDP集群独享        | /rdp_syncer/10001/checkpoint                       |
| kafka.b_version_fback           | kafka版本                                          | 1.0.1                                              |
| kafka.brokerlist                | kafka brokers list                                 | 192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092 |
| kafka.topic                     | 写入kafka topic                                    | topic_10001                                        |
| kafka.partition                 | 写入kafka topic partition，只支持写入单个partition | 0                                                  |
| kafka.producer.enable_split_msg | 是否启用分包                                       | 0                                                  |
| compress.enable                 | 是否启用压缩                                       | 0                                                  |
| metrics.file.dir                | metrics 目录                                       | /apps/svr/rdp_syncer/data/10001/metrics            |

### 2.4 启动RDP

```
cd /apps/svr/rdp_syncer/base/rdp_mysql.20180822/bin
./start.sh 10001
```

