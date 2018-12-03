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
package com.rdp.convert;

import java.util.concurrent.ConcurrentHashMap;
import rdp.messages.RDPProbuf;

public class RDPProcess {
    private static final int DEFAULT_DECOMPRESS_SIZE = 1024 * 1024 * 5;
    private static ConcurrentHashMap<String, RDPTrxProcess> processes_map = new ConcurrentHashMap<String, RDPTrxProcess>();
    private static int decompress_buffer_size = DEFAULT_DECOMPRESS_SIZE;

    public static void SetDecompressBufferSize(int buffer_size) {
        decompress_buffer_size = buffer_size;
    }

    public static AbstractTransaction.Transaction ConvertFromVMSMsgDisableTrx(byte[] data) throws Exception {
        String key = String.valueOf(Thread.currentThread().getId());
        RDPTrxProcess proc = GetRDPProcess(key, false);
        return proc.ConvertFromVMSMsg(data);
    }

    public static AbstractTransaction.Transaction ConvertFromVMSMsgEnableTrx(byte[] data, String topic_partition_name) throws Exception {
        RDPTrxProcess proc = GetRDPProcess(topic_partition_name, true);
        synchronized (proc) {
            return proc.ConvertFromVMSMsg(data);
        }
    }

    public static AbstractTransaction.Transaction ConvertFromKafkaMsgDisableTrx(byte[] data) throws Exception {
        String key = String.valueOf(Thread.currentThread().getId());
        RDPTrxProcess proc = GetRDPProcess(key, false);
        return proc.ConvertFromKafkaMsg(data);
    }

    public static AbstractTransaction.Transaction ConvertFromKafkaMsgEnableTrx(byte[] data, String topic_partition_name) throws Exception {
        RDPTrxProcess proc = GetRDPProcess(topic_partition_name, true);
        synchronized (proc) {
            return proc.ConvertFromKafkaMsg(data);
        }
    }

    private static RDPTrxProcess GetRDPProcess(String key, boolean enable_trx_model) {
        RDPTrxProcess proc = null;
        if (processes_map.containsKey(key)) {
            proc = processes_map.get(key);
            if (proc != null) {
                return proc;
            }
        }
        synchronized (processes_map) {
            if (processes_map.containsKey(key)) {
                proc = processes_map.get(key);
                if (proc != null) {
                    return proc;
                }
            }
            proc = new RDPTrxProcess(decompress_buffer_size, enable_trx_model);
            processes_map.put(key, proc);
        }
        return proc;
    }

    public static void TransactionPrint(RDPProbuf.Transaction tran) throws Exception {
        System.out.println("================================tramsaction start=======================================");
        for (int i = 0; i < tran.getEventsCount(); i++) {
            RDPProbuf.Event event = tran.getEvents(i);
            System.out.println("database: " + event.getDatabaseName().toStringUtf8() + " table: " + event.getTableName().toStringUtf8());
            for (int j = 0; j < event.getRowsCount(); j++) {
                RDPProbuf.Row row = event.getRows(j);
                System.out.println("event type:" + event.getEventType());
                System.out.println("--------------------------------before row---------------------------------------");
                for (int k = 0; k < row.getBeforeCount(); k++) {
                    RDPProbuf.Column column = row.getBefore(k);
                    System.out.println("column name:" + column.getName().toStringUtf8() + " value:" + column.getValue().toStringUtf8());
                }
                System.out.println("--------------------------------after row---------------------------------------");
                for (int k = 0; k < row.getAfterCount(); k++) {
                    RDPProbuf.Column column = row.getAfter(k);
                    System.out.println("column name:" + column.getName().toStringUtf8() + " value:" + column.getValue().toStringUtf8());
                }
            }
        }
        System.out.println("================================tramsaction end=========================================");
    }
}