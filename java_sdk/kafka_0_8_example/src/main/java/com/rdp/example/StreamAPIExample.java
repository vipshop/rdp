//
//Copyright 2018 vip.com.
//
//Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
//the License. You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
//an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
//specific language governing permissions and limitations under the License.
//
package com.rdp.example;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.rdp.convert.AbstractTransaction;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static com.rdp.convert.RDPProcess.ConvertFromKafkaMsgDisableTrx;
import static com.rdp.convert.RDPProcess.ConvertFromKafkaMsgEnableTrx;
import static com.rdp.convert.RDPProcess.TransactionPrint;

public class StreamAPIExample {
    private final ConsumerConnector consumer;
    private String zk_hosts;
    private String group_id;
    private List<String> topic_list;
    private volatile boolean run_flag = true;
    private boolean enable_trx_model = false;
    private boolean print = false;
    private boolean metric = false;

    private final MetricRegistry metrics = new MetricRegistry();
    /**
     * 在控制台上打印输出
     */
    private ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();
    /**
     * 实例化一个Meter
     */
//    private static final Timer requests = metrics.timer(name(TestTimers.class, "request"));
    private final Timer convert_time = metrics.timer(MetricRegistry.name(StreamAPIExample.class, "rdp-convert"));

    //测试JMX
    //JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).build();

    public StreamAPIExample(String zk_hosts, String group_id) {
        this.zk_hosts = zk_hosts;
        this.group_id = group_id;
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zk_hosts, group_id));
    }

    private void ShutDown() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    private ConsumerConfig createConsumerConfig(String zk_hosts, String group_id) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zk_hosts);
        properties.put("group.id", group_id);
        properties.put("zookeeper.session.timeout.ms", "4000");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("consumer.timeout.ms", "1000");
        properties.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(properties);
    }

    public void Start(List<String> topic_list) {
        this.topic_list = topic_list;
        Map<String, Integer> topics_map = new HashMap<>();
        for (String topic : topic_list) {
            topics_map.put(topic, 1);
        }

        Map<String, List<KafkaStream<byte[], byte[]>>> consumer_map = consumer.createMessageStreams(topics_map);
        List<KafkaStream<byte[], byte[]>> streams_list = new ArrayList<>();
        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry: consumer_map.entrySet()) {
            for (KafkaStream stream: entry.getValue()) {
                streams_list.add(stream);
            }
        }

        System.out.println("thread: " + Thread.currentThread().getId() + " create message stream done");

        Timer.Context context = null;

        if (isMetric()) {
            reporter.start(3, TimeUnit.SECONDS);
            //jmxReporter.start();
        }

        while (isRun_flag()) {
            for (KafkaStream<byte[], byte[]> stream: streams_list){
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                if (!isRun_flag())
                    break;
                try {
                    if (!it.hasNext()) {
                        if (isPrint())
                            System.out.println("client id: " + it.clientId() + " reach end");
                        continue;
                    }
                    for (int i = 0; i < 100 && it.hasNext() && isRun_flag(); i++) {
                        try {
                            MessageAndMetadata<byte[], byte[]> msg = it.next();
                            AbstractTransaction.Transaction tran = null;
                            if (isMetric())
                                context = convert_time.time();
                            if (enable_trx_model) {
                                tran = ConvertFromKafkaMsgEnableTrx(msg.message(), msg.topic() + msg.partition());
                            } else {
                                tran = ConvertFromKafkaMsgDisableTrx(msg.message());
                            }
                            if (isMetric())
                                context.stop();
                            if (isPrint())
                                System.out.println("thread:" + Thread.currentThread().getId() + " consumer id: " + it.clientId() + " offset: " + msg.offset() + " msg topic: " + msg.topic() + " partition: " + msg.partition() + " can save offset: " + tran.isCan_save_offset());
                            if (isPrint()) {
                                if (tran.getTransaction() != null) {
                                    TransactionPrint(tran.getTransaction());
                                } else {
                                    System.out.println("transaction is null");
                                }
                            }
                            if (tran.getTransaction() != null && !tran.getTransaction().getGtid().isEmpty())
                                System.out.println(tran.getTransaction().getGtid().toStringUtf8());
                        } catch (Exception e) {
                            System.out.println(e);
                            return;
                        }
                    }
                } catch (ConsumerTimeoutException e) {
                    if (isPrint())
                        System.out.println("thread: " + Thread.currentThread().getId() + " client id: " + it.clientId() + " timeout msg: " + e.getMessage());
                }
            }
        }
        ShutDown();
    }

    public boolean isRun_flag() {
        return run_flag && !Thread.currentThread().isInterrupted();
    }

    public void setRun_flag(boolean run_flag) {
        this.run_flag = run_flag;
    }

    public void setEnable_trx_model(boolean enable_trx_model) {
        this.enable_trx_model = enable_trx_model;
    }

    public boolean isPrint() {
        return print;
    }

    public void setPrint(boolean print) {
        this.print = print;
    }

    public boolean isMetric() {
        return metric;
    }

    public void setMetric(boolean metric) {
        this.metric = metric;
    }
}
