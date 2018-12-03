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

#ifndef _REBUILD_H_
#define _REBUILD_H_

#include <string>
#include <rdp-comm/kafka_consumer.h>
#include "checkpoint.h"

using std::string;

namespace syncer {

bool Rebuild(CheckPoint *checkpoint);

void FilteRebuildRemainTrans(rdp::messages::Transaction *trans, SeqTransCtx *seq_trans_ctx);

// return 
// 0: continue send to kafka
// 1: ignore, no need send to kafka
// -1: meet error, first gtid not empty transaction is not the g_transaction
int CompareRebuildRemainTrans(rdp::messages::Transaction *trans);

} // namespace syncer

#endif // _REBUILD_H_

