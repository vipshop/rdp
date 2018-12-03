# RDP Metrics指标说明

RDP进程内部Metrics信息，通过写文件的方式吐出，使用filebeat同步至InfluxDB，展现在grafana页面上。

Metrics文件数据格式：

```
{
    "metricName":"write_msg_avg_delay_ms", #指标名称
    "timestamp":1539670744000, #指标生成timestamp，单位毫秒
    "namespace":"app",
    "hostname":"192.168.0.1", #指标生成的主机IP
    "endpoint":"10151", #配置文件中的group.id
    "longValue":2000, #指标值
    "meta":{
        "duration":10, #生成指标时间间隔
        "endpointType":"app",
        "counterType":"gauge"
    }
}
```

Metrics列表：

| 指标名称                        | 指标含义                                                     |
| ------------------------------- | ------------------------------------------------------------ |
| connect_master_count            | 连接MySQL Master的次数                                       |
| mysql_switch_count              | 源MySQL集群切换的次数                                        |
| total_trx_count                 | 本次启动后，处理事务的计数                                   |
| total_msg_count                 | 本次启动后，处理消息的计数                                   |
| seconds_behind_master           | rdp与master之间binlog延迟秒数                                |
| binlog_heartbeat_received_count | rdp与master之间发送heartbeat个数                             |
| binlog_received_count           | rdp接收到binlog transaction总个数                            |
| in_rate                         | rdp同步master的网络输入流量                                  |
| io_thread_state                 | rdp slave io线程状态                                         |
| last_event_timestamp            | rdp同步的最后一个事件的timestamp                             |
| pending_trx_count               | pending在bounded buffer中的事务计数                          |
| pending_msg_count               | pending在bounded buffer中的消息计数                          |
| serial_exec_count               | 串行执行的次数                                               |
| parser_trans_avg_proc_time      | 每个事务平均处理延迟                                         |
| parser_event_count              | 总event数                                                    |
| parser_trans_count              | 总trans数                                                    |
| filte_trx_count                 | 非过滤数据中被过滤的transaction总个数                        |
| get_table_info_count            | 查询表结构信息次数                                           |
| get_table_info_rt               | 查询表结构rt                                                 |
| get_table_info_failed_count     | 查询表结构信息失败的次数                                     |
| execute_ddl_count               | 执行ddl的次数                                                |
| execute_ddl_rt                  | 执行ddl的rt                                                  |
| execute_ddl_failed_count        | 执行ddl失败的次数                                            |
| table_cache_count               | 缓存的表数量个数                                             |
| binlog_write_done_count         | 本次启动后完成处理的binlog总数据量(已经接收到Write Response) |
| binlog_sent_count               | 本次启动后完成处理的binlog总数据量(发送成功，未接收到Write Response) |
| msg_write_done_count            | 本次启动后完成写到Kafka总数据量(已经接收到Write Response)    |
| msg_sent_count                  | 本次启动后完成写到Kafka总数据量(发送成功，未接收到Write Response) |
| wait_rsp_msg_count              | 已经发送成功，但未接受到Write Response总数据量               |
| write_msg_avg_delay_ms          | 最近1000个写入Kafka RT平均消耗时间。每100个RT，循环Push至Metric。 |
| encoded_msg_count               | 等待发送至kafka的事务数量                                    |
| producer_msg_tps                | producer kafka message的吞吐量                               |
| producer_trans_tps              | producer kafka transaction的吞吐量                           |
| out_rate                        | rdp写往kafka集群的网络输出流量                               |