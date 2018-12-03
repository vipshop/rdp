

## RDP

![logo](../../rdp-logo.png)

> 唯品会分布式数据管道系统

## 简介

RDP的全称是Real-Time Data Pipeline，是一个从MySQL同步数据到Kafka的数据管道。正如这个名字一样，RDP不生产数据，只是数据的“搬运工”。

基本原理是从上游MySQL中拉取binlog内容，解析之后推送到下游系统中，比如Apache Kafka。 下游业务可以从中订阅或查询数据，拿到数据后结合业务自身逻辑进行处理，RDP在其中扮演了上下游业务的数据管道角色。 RDP从上游获取数据到推送的过程中，吞吐量可以支撑20w+事务每秒（约100w+事件每秒），满足大并发的业务要求。 另外，RDP处理延迟是ms级别，也可满足业务流计算要求。

## 特性

### 高可用
* 完善的Failover能力，确保对业务系统持续服务能力。 保证数据的一致性（MySQL切换导致的数据丢失不在此范畴）。

### 低延时
* 精简IO路径，提升实效性。 数据的并行处理能力，保障高性能。

### 可追溯
* 数据丢没丢，透明可验证。 数据丢了，丢了哪些有据可循。

## Contributors

* [范力彪](https://github.com/libiaofan)
* [汤锦平](https://github.com/tom-tangjp)
* [赵百忠](https://github.com/firnsan)
* [陈非](https://github.com/flike)
* 简怀兵
* 陈世旺

[DOCS:  1.0](../../_sidebar.md)
