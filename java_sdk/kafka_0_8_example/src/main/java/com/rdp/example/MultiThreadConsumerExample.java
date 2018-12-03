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

import org.apache.commons.cli.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiThreadConsumerExample {
    public class ConsumerThread implements Runnable {
        private StreamAPIExample consumer;
        private List<String> topic_list = new ArrayList<>();

        public ConsumerThread(String zk_hosts, String group_id, boolean enable_trx_model) {
            consumer = new StreamAPIExample(zk_hosts, group_id);
            consumer.setEnable_trx_model(enable_trx_model);
        }

        public void AddTopic(String topic) {
            topic_list.add(topic);
        }

        @Override
        public void run() {
            System.out.println("thread: " + Thread.currentThread().getId() + " start");
            if (!topic_list.isEmpty())
                consumer.Start(topic_list);
            System.out.println("thread: " + Thread.currentThread().getId() + " stop");
        }

        public void Stop() {
            consumer.setRun_flag(false);
        }

        public void enablePrint(boolean print) {
            consumer.setPrint(print);
        }
        public void enableMetric(boolean metric) {
            consumer.setMetric(metric);
        }
    }

    private String zk_hosts;
    private String group_id;
    private List<String> topic_list;
    private ExecutorService executor;
    private List<ConsumerThread> thread_list = new ArrayList<>();
    private boolean print = false;
    private boolean metric = false;

    public MultiThreadConsumerExample(String zk_hosts, String group_id, List<String> topic_list, boolean print, boolean metric) {
        this.zk_hosts = zk_hosts;
        this.group_id = group_id;
        this.topic_list = topic_list;
        this.print = print;
        this.metric = metric;
    }

    public void ShutDown() {
        for (ConsumerThread t: thread_list) {
            t.Stop();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Time out waiting for consumer threads to shut down");
                System.exit(1);
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted Exception:" + e.getMessage());
            System.exit(-1);
        }
    }

    public void run(int thread_num, boolean enable_trx_model) {
        for (int i = 0; i < thread_num; i++) {
            ConsumerThread t = new ConsumerThread(zk_hosts, group_id, enable_trx_model);
            t.enablePrint(this.print);
            t.enableMetric(this.metric);
            thread_list.add(t);
        }

        if (thread_list.size() <= topic_list.size()) {
            int index = 0;
            for (String topic: topic_list) {
                thread_list.get(index%thread_num).AddTopic(topic);
                index++;
            }
        } else {
            int index = 0;
            for (ConsumerThread t: thread_list) {
                t.AddTopic(topic_list.get(index%topic_list.size()));
                index++;
            }
        }

        executor = Executors.newFixedThreadPool(thread_num);
        for (ConsumerThread thread: thread_list) {
            executor.execute(thread);
        }
    }



    public static void main(String[] args) {
        String zk_host = "192.168.0.1:9092";
        String group_id = "rdp_test";
        List<String> topic_list = new ArrayList<>();
        int thread_num = 1;
        long run_second = 60;
        boolean enable_trx_model = false;
        boolean print = false;
        boolean metric = false;

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("h", "help", false, "usage:./program -b zookeeper_host -c consumer_id -t topic_name -n thread_num [-s run_second] [-e] [-p] [-m]");
        options.addRequiredOption("b", "zk_hosts", true, "zookeeper hosts");
        options.addRequiredOption("c", "consumer", true, "consumer id");
        options.addRequiredOption("t", "topic", true, "topic name");
        options.addOption("n", "thdnum", true, "thread number");
        options.addOption("s", "second", true,"run second");
        options.addOption("e", "enable-trx-model", false,"enable trx model");
        options.addOption("p", "print", false,"print transaction");
        options.addOption("m", "metric", false,"metric model");
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
            zk_host = commandLine.getOptionValue('b');
        }
        if (commandLine.hasOption('c')) {
            group_id = commandLine.getOptionValue('c');
        }
        if (commandLine.hasOption('n')) {
            thread_num = Integer.parseInt(commandLine.getOptionValue('n'));
        }
        if (commandLine.hasOption('s')) {
            run_second = Integer.parseInt(commandLine.getOptionValue('s'));
        }
        if (commandLine.hasOption('t')) {
            String t = commandLine.getOptionValue('t');
            if (t.contains(",")) {
                String[] topics = t.split(",");
                for (String x: topics) {
                    topic_list.add(x);
                }
            } else {
                topic_list.add(t);
            }
        }
        if (commandLine.hasOption('e')) {
            enable_trx_model = true;
        }
        if (commandLine.hasOption('m')) {
            metric = true;
        }
        if (commandLine.hasOption('p')) {
            print = Boolean.valueOf(commandLine.getOptionValue('p'));
        }

        MultiThreadConsumerExample example = new MultiThreadConsumerExample(zk_host, group_id, topic_list, print, metric);
        example.run(thread_num, enable_trx_model);
        try {
            Thread.sleep(run_second*1000);
            example.ShutDown();
        } catch (InterruptedException e) {
            System.out.println(e);
            System.exit(1);
        }
        System.exit(0);
    }
}
