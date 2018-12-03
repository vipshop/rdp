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

#include "fiu_engine.h"

namespace fiu {

// Printf debug info to stdout
#define LOG_INFO(x, args...) \
    fprintf(stdout, "%s(%s:%d) "#x"\n", __func__, __FILE__, __LINE__, ##args)
#define LOG_FLUSH() fflush(stdout) 

// Common request packet
#pragma pack(push)
#pragma pack(1)

struct ReqMsg {
  uint32_t type_code_;
  uint32_t status_code_;
  uint32_t int_parameter_;
  char keyword_[129];
  char str_parameter_[33];
  char reserved_[82];

  explicit ReqMsg(void) : type_code_(0), status_code_(0), int_parameter_(0) {
    memset(keyword_, 0x0, sizeof(keyword_));
    memset(str_parameter_, 0x0, sizeof(str_parameter_));
  }
};

#pragma pack(pop)

// Return error message
static std::string GetLastErrorMsg(int err) {
  char err_msg[128] = {0x0};
  return std::string(strerror_r(err, err_msg, sizeof(err_msg)));
}

enum { kShmSize = (1 << 20) };
// Each ReqMsg is 256 bytes, thus 1M shared memory can held up to
// 4K injection points; For each test case, this should be enough
static const char *g_fiu_shm_name = "/fiu.shm.keyword";
static int g_shm_fd = -1;
static void *g_shm_ptr = NULL;
static bool g_lib_inited = false;
pthread_once_t g_init_once = PTHREAD_ONCE_INIT;

// Open shm once
static void Init(void) {
  assert(g_shm_ptr == NULL && g_shm_fd == -1);
  // Open 1st
  g_shm_fd = shm_open(g_fiu_shm_name, O_RDWR, 0666);
  if (-1 == g_shm_fd) {
    LOG_INFO("shm_open failed: (%s)", GetLastErrorMsg(errno).c_str());
    return;
  }

  // ftruncate(fd_, kShmSize);
  g_shm_ptr = mmap(0, kShmSize, PROT_READ | PROT_WRITE, MAP_SHARED, g_shm_fd, 0);
  if (MAP_FAILED == g_shm_ptr) {
    LOG_INFO("mmap failed: (%s)", GetLastErrorMsg(errno).c_str());
    return;
  }

  uint32_t idx = *(uint32_t *)g_shm_ptr;
  LOG_INFO("(%u) sync points already exist", idx);
  // Encountered corrupted share memory
  if (sizeof(uint32_t) + idx * sizeof(ReqMsg) > kShmSize) {
    LOG_INFO("shm corrupted");
    LOG_FLUSH();
    assert(0);
  }

  g_lib_inited = true;
}

// Create shm once
bool Open(void) {
  int ret = 0;
  ret = pthread_once(&g_init_once, &Init);
  if (0 != ret) {
    LOG_INFO("pthread_once failed: (%s)", GetLastErrorMsg(ret).c_str());
    LOG_FLUSH();
    assert(0);
  }

  return g_lib_inited;
}

// Destroy shm if exist
void Close() {
  if (NULL != g_shm_ptr) {
    // TODO: Do we need to unmap ?
    if (-1 == munmap(g_shm_ptr, kShmSize)) {
      LOG_INFO("munmap failed: (%s)", GetLastErrorMsg(errno).c_str());
    }
  }
}

// Check if a sync point exists
bool IsSyncPointExist(const char *sp) {
  assert(sp);
  // Check if dbug instance had been initiated
  if (NULL == g_shm_ptr || !g_lib_inited) {
    // LOG_INFO("dbug instance NOT initiated");
    return false;
  }

  // Encountered corrupted share memory
  uint32_t idx = *(uint32_t *)g_shm_ptr;
  if (sizeof(uint32_t) + idx * sizeof(ReqMsg) > kShmSize) {
    LOG_INFO("shm corrupted");
    LOG_FLUSH();
    assert(0);
  }

  // Check if specified key already exist
  ReqMsg *p =
      (ReqMsg *)((char *)g_shm_ptr + sizeof(uint32_t));
  for (uint32_t i = 0; i < idx; i++, p++) {
    if (0 == strncasecmp(p->keyword_, sp, 128)) {
      if (0 == p->int_parameter_++) { 
        return true;
      } else {
        return false;
      }
    }
  }

  // LOG_INFO("sync point (%s) NOT found", sp);
  return false;
}

bool IsSyncPointSet(const char *sp) {
  assert(sp);
  // Check if dbug instance had been initiated
  if (NULL == g_shm_ptr || !g_lib_inited) {
    // LOG_INFO("dbug instance NOT initiated");
    return false;
  }

  // Encountered corrupted share memory
  uint32_t idx = *(uint32_t *)g_shm_ptr;
  if (sizeof(uint32_t) + idx * sizeof(ReqMsg) > kShmSize) {
    LOG_INFO("shm corrupted");
    LOG_FLUSH();
    assert(0);
  }

  // Check if specified key already exist
  ReqMsg *p =
      (ReqMsg *)((char *)g_shm_ptr + sizeof(uint32_t));
  for (uint32_t i = 0; i < idx; i++, p++) {
    if (0 == strncasecmp(p->keyword_, sp, 128)) {
      return true;
    }
  }

  // LOG_INFO("sync point (%s) NOT found", sp);
  return false;
}
} // namespace fiu
