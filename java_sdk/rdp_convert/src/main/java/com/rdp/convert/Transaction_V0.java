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

public class Transaction_V0 extends AbstractTransaction {
    public Transaction_V0(boolean enable_trx_model) {
        super(enable_trx_model);
    }

    public Transaction_V0(AbstractTransaction t) {
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
        // 校验epoch split_seq_no transaction_seq_no
        if (!IsAvailableTrx(epoch, split_seq_no, transaction_seq_no, split_flag)) {
            if (isEnable_trx_model())
                return new Transaction(null, false);
            else
                return new Transaction(null, true);
        }
        tran_builder.clear();
        tran_builder.mergeFrom(data, 0, data_len);

        setEpoch(epoch);
        setSplit_seq_no(split_seq_no);
        setTransaction_seq_no(transaction_seq_no);

        filterInvalidEvents();
        return new Transaction(getTransaction(), true);
    }

    @Override
    protected boolean IsAvailableTrx(long epoch, long split_seq_no, long transaction_seq_no, int split_flag) {
        // V0版本分包逻辑为物理分包，线上实例未启用分包逻辑，所以遇到分包时直接过滤
        if (split_flag != 0)
            return false;
        // 不关注事务特性，允许消费重复数据，不对重复数据进行过滤
        if (!isEnable_trx_model()) {
            return true;
        }
        // epoch为-1，表示消费的第一个数据包
        if (getEpoch() == -1) {
            return true;
        }
        // 分包在这个版本不启用，不需要校验分包序号
        // 版本回退，过滤数据
        if (epoch < getEpoch())
            return false;
        // 事务序号回退，过滤数据
        if (transaction_seq_no <= getTransaction_seq_no())
            return false;
        return true;
    }

    @Override
    public RDPProbuf.PBVERSION GetVersion() {
        return RDPProbuf.PBVERSION.kPBVersion_0;
    }
}
