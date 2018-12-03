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

public class Transaction_V1 extends AbstractTransaction {
    public Transaction_V1(boolean enable_trx_model) {
        super(enable_trx_model);
    }

    public Transaction_V1(AbstractTransaction t) {
        setEpoch(t.getEpoch());
        setTransaction_seq_no(t.getTransaction_seq_no());
        // 数据版本变动，不存在分包情况
        setSplit_seq_no(-1);
        this.enable_trx_model = t.isEnable_trx_model();
        // 数据版本变动，原有事务数据需要清空
        tran_builder.clear();
    }

    @Override
    public Transaction Convert(byte[] data, int data_len, long epoch, long split_seq_no, long transaction_seq_no, int split_flag) throws Exception {
        if (!IsAvailableTrx(epoch, split_seq_no, transaction_seq_no, split_flag)) {
            if (isEnable_trx_model())
                return new Transaction(null, false);
            else
                return new Transaction(null, true);
        }
        // 没有启用事务特性
        if (!isEnable_trx_model()) {
            tran_builder.clear();
            tran_builder.mergeFrom(data, 0, data_len);
            setEpoch(epoch);
            setSplit_seq_no(split_seq_no);
            setTransaction_seq_no(transaction_seq_no);
            filterInvalidEvents();
            return new Transaction(getTransaction(), true);
        }
        switch (split_flag) {
            case 0:
                // 启用事务特性，但没有分包数据
                tran_builder.clear();
                tran_builder.mergeFrom(data, 0, data_len);
                setEpoch(epoch);
                setTransaction_seq_no(transaction_seq_no);
                filterInvalidEvents();
                return new Transaction(getTransaction(), true);
            case 1:
                // 启用事务特性，且是分包数据
                tran_builder.mergeFrom(data, 0, data_len);
                setSplit_seq_no(split_seq_no);
                return new Transaction(null, false);
            case 2:
                // 分包结束
                tran_builder.mergeFrom(data, 0, data_len);
                setEpoch(epoch);
                setTransaction_seq_no(transaction_seq_no);
                // 下一个分包序号为0
                setSplit_seq_no(-1);
                filterInvalidEvents();
                return new Transaction(getTransaction(), true);
            default:
                return new Transaction(null, true);
        }
    }

    @Override
    protected boolean IsAvailableTrx(long epoch, long split_seq_no, long transaction_seq_no, int split_flag) {
        // 不启用事务特性，允许消费重复数据
        if (!isEnable_trx_model())
            return true;
        // 读取到第一个数据包
        if (getEpoch() == -1 && getTransaction_seq_no() == -1) {
            if (getSplit_seq_no() == -1) {
                // 不论是否是分包数据，一定要消费到分包序号为0，才是第一个事务的开始
                if (split_seq_no != 0)
                    return false;
                return true;
            }
            // 已经消费了第一个分包数据，但是未能组成一个完整的事务
            // 如果分包序号不连续，说明之前消费的数据包为不完整的事务，需要清空旧事务
            if (split_seq_no != (getSplit_seq_no() + 1)) {
                tran_builder.clear();
            }
            return true;
        }
        // 到达这里表示已经消费到一个完整的事务
        if (getEpoch() > epoch)
            return false;

        // 不分包数据，事务序号不是连续递增
        if (transaction_seq_no != (getTransaction_seq_no() + 1))
            return false;

        switch (split_flag) {
            case 0:
                // 可能之前存在不完整的分包
                tran_builder.clear();
                return true;
            case 1:
            case 2:
                // 分包数据
                if (split_seq_no != (getSplit_seq_no() + 1)) {
                    // 分包序号为0表示分包开始，不能过滤
                    if (split_seq_no == 0) {
                        tran_builder.clear();
                        return true;
                    }
                    return false;
                }
                return true;
            default:
                return false;
        }
    }

    @Override
    public RDPProbuf.PBVERSION GetVersion() {
        return RDPProbuf.PBVERSION.kPBVersion_1;
    }
}
