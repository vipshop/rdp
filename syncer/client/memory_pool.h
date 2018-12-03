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

#ifndef _MEMORY_POOL_H_
#define _MEMORY_POOL_H_

#include "syncer_incl.h"
#include "syncer_app.h"
#define TBB_PREVIEW_MEMORY_POOL 1
#include <tbb/memory_pool.h>

namespace syncer {

extern tbb::fixed_pool* g_memory_pool;
extern void CreateMemoryPool(void);
extern void CloseMemoryPool(void);

} // namespace syncer

#endif // _MEMORY_POOL_H_

