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

import com.google.protobuf.InvalidProtocolBufferException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import rdp.messages.RDPProbuf;

public class RDPTrxProcess {
    private boolean enable_trx_model = false;

    private byte[] decompress_buffer = null;
    private LZ4SafeDecompressor decompressor = LZ4Factory.safeInstance().safeDecompressor();

    private RDPProbuf.VMSMessage.Builder vms_msg_builder = RDPProbuf.VMSMessage.newBuilder();
    private RDPProbuf.KafkaPkg.Builder pkg_builder = RDPProbuf.KafkaPkg.newBuilder();

    private AbstractTransaction trans_process = null;

    public RDPTrxProcess(int buffer_size, boolean enable_trx_model) {
        this.decompress_buffer = new byte[buffer_size];
        this.enable_trx_model = enable_trx_model;
    }

    private AbstractTransaction getTransProcess() {
        if (trans_process == null) {
            if (pkg_builder.hasVersion()) {
                switch (pkg_builder.getVersion()) {
                    case kPBVersion_1:
                        trans_process = new Transaction_V1(this.enable_trx_model);
                        return trans_process;
                    default:
                        trans_process = new Transaction_V0(this.enable_trx_model);
                        return trans_process;
                }
            } else {
                trans_process = new Transaction_V0(this.enable_trx_model);
                return trans_process;
            }
        }

        if (pkg_builder.hasVersion()) {
            // 如果已经存在version字段，表示至少是V1及以上版本
            switch (pkg_builder.getVersion()) {
                // 这种情况一般不存在，但是还是写在这里
                case kPBVersion_0:
                    if (trans_process.GetVersion() != RDPProbuf.PBVERSION.kPBVersion_0) {
                        trans_process = new Transaction_V0(this.enable_trx_model);
                    }
                    return trans_process;
                case kPBVersion_1:
                    if (trans_process.GetVersion() != RDPProbuf.PBVERSION.kPBVersion_1) {
                        trans_process = new Transaction_V1(this.enable_trx_model);
                    }
                    return trans_process;
                // version无效
                default:
                    if (trans_process.GetVersion() != RDPProbuf.PBVERSION.kPBVersion_0) {
                        trans_process = new Transaction_V0(this.enable_trx_model);
                    }
                    return trans_process;
            }
        } else {
            // 没有version字段，表示数据为V0版本
            if (trans_process.GetVersion() == RDPProbuf.PBVERSION.kPBVersion_0) {
                return trans_process;
            } else {
                // 如果trans_process不为V0版本则转换为V0版本
                trans_process = new Transaction_V0(trans_process);
                return trans_process;
            }
        }
    }

    private void ConvertToVMSMsg(byte[] data) throws Exception {
        vms_msg_builder.clear();
        vms_msg_builder.mergeFrom(data);
    }

    private void ConvertToKafkaPkg(byte[] data) throws Exception {
        pkg_builder.clear();
        pkg_builder.mergeFrom(data);
    }

    public AbstractTransaction.Transaction ConvertFromKafkaMsg(byte[] data) throws Exception {
        ConvertToVMSMsg(data);
        ConvertToKafkaPkg(vms_msg_builder.getPayload().toByteArray());
        AbstractTransaction proc = getTransProcess();
        if (proc == null) {
            throw new Exception("Version:" + pkg_builder.getVersion() + " Not Available");
        }
        KafkaData kafkaData = getKafkaPkgData();
        try {
            return proc.Convert(kafkaData.getData(), kafkaData.getLen(), pkg_builder.getEpoch(), pkg_builder.getSeqNo(), pkg_builder.getTransSeqNo(), pkg_builder.getSplitFlag());
        } catch (InvalidProtocolBufferException e) {
            // 如果KafkaPkg没有Flag字段，也有可能是压缩后的数据（最早的RDP版本，没加入自适应压缩）
            int pkg_data_len = Decompress(pkg_builder.getData().toByteArray());
            byte[] pkg_data = decompress_buffer;
            return proc.Convert(pkg_data, pkg_data_len, pkg_builder.getEpoch(), pkg_builder.getSeqNo(), pkg_builder.getTransSeqNo(), pkg_builder.getSplitFlag());
        }
    }

    public AbstractTransaction.Transaction ConvertFromVMSMsg(byte[] data) throws Exception {
        ConvertToKafkaPkg(data);
        AbstractTransaction proc = getTransProcess();
        if (proc == null) {
            throw new Exception("Version:" + pkg_builder.getVersion() + " Not Available");
        }
        KafkaData kafkaData = getKafkaPkgData();
        return proc.Convert(kafkaData.getData(), kafkaData.getLen(), pkg_builder.getEpoch(), pkg_builder.getSeqNo(), pkg_builder.getTransSeqNo(), pkg_builder.getSplitFlag());
    }

    private int Decompress(byte[] src) throws LZ4Exception {
        try {
            return decompressor.decompress(src, decompress_buffer);
        } catch (LZ4Exception e) {
            if (!e.getMessage().contains("Malformed")) {
                int buffer_size = decompress_buffer.length * 5;
                decompress_buffer = new byte[buffer_size];
                return decompressor.decompress(src, decompress_buffer);
            }
            throw e;
        }
    }

    private KafkaData getKafkaPkgData() throws Exception {
        int len = 0;
        if (pkg_builder.hasFlag() && (pkg_builder.getFlag() & RDPProbuf.KafkaPkgFlag.kKfkPkgCompressData_VALUE) == RDPProbuf.KafkaPkgFlag.kKfkPkgCompressData_VALUE) {
            if (pkg_builder.hasSourceDataLen() && pkg_builder.getSourceDataLen() > decompress_buffer.length) {
                int size = (int)(pkg_builder.getSourceDataLen());
                if (size <= 0) {
                    throw new Exception(pkg_builder.getGtid() + " source_data_len:" + pkg_builder.getSourceDataLen() + " more then max int");
                }
                this.decompress_buffer = new byte[size];
            }
            len = Decompress(pkg_builder.getData().toByteArray());
            return new KafkaData(decompress_buffer, len);
        } else {
            len = pkg_builder.getData().toByteArray().length;
            return new KafkaData(pkg_builder.getData().toByteArray(), len);
        }
    }

    private class KafkaData {
        private byte[] data = null;
        private int len = 0;
        public KafkaData(byte[] data, int len) {
            this.data = data;
            this.len = len;
        }

        public byte[] getData() {
            return data;
        }

        public int getLen() {
            return len;
        }
    }

}