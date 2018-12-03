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

import rdp.messages.RDPProbuf;

public abstract class AbstractTransaction {
    protected boolean enable_trx_model = false;

    protected long epoch = -1;
    protected long transaction_seq_no = -1;
    protected long split_seq_no = -1;

    protected RDPProbuf.Transaction.Builder tran_builder = RDPProbuf.Transaction.newBuilder();

    public AbstractTransaction() {
    }

    public AbstractTransaction(boolean enable_trx_model) {
        this.enable_trx_model = enable_trx_model;
    }

    // 用于兼容topic中数据版本不相同情况
    public AbstractTransaction(AbstractTransaction t) {
        setEpoch(t.getEpoch());
        setTransaction_seq_no(t.getTransaction_seq_no());
        // 数据版本变动，不存在分包情况
        setSplit_seq_no(-1);
        this.enable_trx_model = t.isEnable_trx_model();
        // 数据版本变动，原有事务数据需要清空
        tran_builder.clear();
    }

    public abstract Transaction Convert(byte[] data, int data_len, long epoch, long split_seq_no, long transaction_seq_no, int split_flag) throws Exception;

    protected abstract boolean IsAvailableTrx(long epoch, long split_seq_no, long transaction_seq_no, int split_flag);

    public abstract rdp.messages.RDPProbuf.PBVERSION GetVersion();

    public boolean isEnable_trx_model() {
        return enable_trx_model;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getTransaction_seq_no() {
        return transaction_seq_no;
    }

    public void setTransaction_seq_no(long transaction_seq_no) {
        this.transaction_seq_no = transaction_seq_no;
    }

    public long getSplit_seq_no() {
        return split_seq_no;
    }

    public void setSplit_seq_no(long split_seq_no) {
        this.split_seq_no = split_seq_no;
    }

    protected void filterInvalidEvents() {
        if (!tran_builder.isInitialized())
            return;
        for (int i = 0; i < tran_builder.getEventsCount() && tran_builder.getEventsCount() > 0; ) {
            /*
            MariaDB 10.0.21
            WRITE_ROWS_EVENT_V1 = 23, 对应insert操作
            UPDATE_ROWS_EVENT_V1 = 24, 对应update操作
            DELETE_ROWS_EVENT_V1 = 25, 对应delete/update操作
            MySQL 5.7.17
            WRITE_ROWS_EVENT = 30,
            UPDATE_ROWS_EVENT = 31,
            DELETE_ROWS_EVENT = 32
             */
            if (tran_builder.getEvents(i).getEventType() == 23 ||
                    tran_builder.getEvents(i).getEventType() == 24 ||
                    tran_builder.getEvents(i).getEventType() == 25 ||
                    tran_builder.getEvents(i).getEventType() == 30 ||
                    tran_builder.getEvents(i).getEventType() == 31 ||
                    tran_builder.getEvents(i).getEventType() == 32) {
                if (tran_builder.getEvents(i).getRowsCount() > 0) {
                    i++;
                    continue;
                }
            }
            tran_builder.removeEvents(i);
        }
    }

    protected RDPProbuf.Transaction getTransaction() {
        if (!tran_builder.isInitialized())
            return null;
        if (tran_builder.getEventsCount() <= 0)
            return null;
        return tran_builder.build();
    }


    public class Transaction {
        private RDPProbuf.Transaction transaction = null;
        boolean can_save_offset = false;
        public Transaction(RDPProbuf.Transaction transaction, boolean  can_save_offset) {
            this.transaction = transaction;
            this.can_save_offset = can_save_offset;
        }

        public RDPProbuf.Transaction getTransaction() {
            return transaction;
        }

        public boolean isCan_save_offset() {
            return can_save_offset;
        }
    }
}
