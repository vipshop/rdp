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

#include "memory_pool.h"

namespace syncer {

static char* g_mem_buf = NULL;
tbb::fixed_pool* g_memory_pool = NULL;

void CreateMemoryPool(void) {
  uint64_t pool_size = 
    g_syncer_app->app_conf_->GetValue<uint64_t>("memory.pool.size", 1024);
  pool_size *= 1024*1024;
  g_mem_buf = new char[pool_size];
  assert(g_mem_buf);
  
  g_memory_pool = new tbb::fixed_pool(g_mem_buf, pool_size);
  assert(g_memory_pool);
}

void CloseMemoryPool(void) {
  if (g_mem_buf) {
    delete [] g_mem_buf;
    g_mem_buf = NULL;
  }

  if (g_memory_pool) {
    delete g_memory_pool;
    g_memory_pool = NULL;
  }
}

}  // namespace syncer
