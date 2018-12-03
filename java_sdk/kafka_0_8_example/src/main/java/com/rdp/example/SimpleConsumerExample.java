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

import com.rdp.convert.AbstractTransaction;
import com.rdp.convert.RDPTrxProcess;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.rdp.convert.RDPProcess.TransactionPrint;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

/**
 * This program reads messages from two topics. Messages on "fast-messages" are analyzed
 * to estimate latency (assuming clock synchronization between producer and consumer).
 * <p/>
 * Whenever a message is received on "slow-messages", the stats are dumped.
 */
public class SimpleConsumerExample {

    public static ConsumerConnector createConsumer(String zkHost, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkHost);
        props.put("group.id", groupId);
        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("consumer.timeout.ms", "1000");
        ConsumerConfig config = new ConsumerConfig(props);

        return Consumer.createJavaConsumerConnector(config);
    }

    public static void main(String[] args) throws Exception{
        String zkHost = "192.168.0.1:2181";
        String groupId = "rdp_test";
        String topic = "default_topic_rdp";
        long maxcount = -1;
        boolean enable_trx_model = false;

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("h", "help", false, "usage:./program -b zookeeper_host -c consumer_id -t topic_name [-m max_message_number] [-e]");
        options.addRequiredOption("b", "zk_hosts", true, "zookeeper hosts");
        options.addRequiredOption("c", "consumer", true, "consumer id");
        options.addRequiredOption("t", "topic", true, "topic name");
        options.addOption("m", "msgnum", true, "consumer message number");
        options.addOption("e", "enable-trx-model", true, "enable trx model");

        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e){
            System.out.println(e.getMessage());
            System.out.println(options.getOption("h").getDescription());
            System.exit(0);
        }

        if (commandLine.hasOption("h")) {
            System.out.println(options.toString());
            System.exit(0);
        }
        if (commandLine.hasOption("b")) {
            zkHost = commandLine.getOptionValue('b');
        }
        if (commandLine.hasOption('c')) {
            groupId = commandLine.getOptionValue('c');
        }
        if (commandLine.hasOption('t')) {
            topic = commandLine.getOptionValue('t');
        }
        if (commandLine.hasOption('m')) {
            maxcount = Integer.parseInt(commandLine.getOptionValue('m'));
        }
        if (commandLine.hasOption('e')) {
            enable_trx_model = Boolean.valueOf(commandLine.getOptionValue('e'));
        }

        ConsumerConnector consumer = createConsumer(zkHost, groupId);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        System.out.println("Start Subscribe");
        long beginTimeMs = System.currentTimeMillis();
        long currTimeMs = System.currentTimeMillis();
        long startOffset = 0;
        long count = 0;
        long stat = 0;
        RDPTrxProcess proc = new RDPTrxProcess(10*1024*1024, enable_trx_model);
        try {
            while (it.hasNext() && (maxcount == -1 ? true : count++ < maxcount)) {
                MessageAndMetadata<byte[], byte[]> msg = it.next();
                try {
                    if (startOffset == 0) {
                        startOffset = msg.offset();
                    }
                    if ((currTimeMs - beginTimeMs) >= 5 * 1000) {
                        System.out.println("Consume offset:" + msg.offset() + " count:" + stat);
                        stat = 0;
                        beginTimeMs = currTimeMs;
                    }
                    currTimeMs = System.currentTimeMillis();
                    stat++;
                    long startTime = System.nanoTime();
                    System.out.println("Consume topic:" + msg.topic() + " partition:" + msg.partition() + " offset:" + msg.offset() + " msg len:" + msg.message().length);
                    System.out.println(printHexBinary(msg.message()));
                    AbstractTransaction.Transaction tran = proc.ConvertFromKafkaMsg(msg.message());
                    if (tran == null) {
                        System.out.println("trans filted can save offset: " + tran.isCan_save_offset());
                        continue;
                    }
                    //long stopTime = System.nanoTime();
                    //long endTimeMs = System.currentTimeMillis();
                    //System.out.println("Start Offset: " + startOffset + " End Consumer offset: " + msg.offset() + " cost: " + (stopTime-startTime) + " ns" + " total:" + (endTimeMs - beginTimeMs) +" ms");
                    System.out.println("Start Offset: " + startOffset + " End Consumer offset: " + msg.offset() + " can save offset:" + tran.isCan_save_offset());
                    TransactionPrint(tran.getTransaction());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (ConsumerTimeoutException e) {
            e.printStackTrace();
        }
        consumer.shutdown();
    }
}

