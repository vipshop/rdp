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

#include "statistics_writer.h"
#include "syncer_app.h"

static map<string, FILE *> g_files_map;
static pthread_mutex_t g_files_map_mutex = PTHREAD_MUTEX_INITIALIZER;

static bool g_stat_init_flag = false;

//static struct timeval g_time;

static pthread_mutex_t g_finish_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_write_cond = PTHREAD_COND_INITIALIZER;

static queue<syncer::StatPoint*> g_finish[2];
static uint32_t g_change_index = 0;
static queue<syncer::StatPoint *> *g_curr_finish =
    &(g_finish[g_change_index % 2]);
static queue<syncer::StatPoint *> *g_curr_write =
    &(g_finish[(g_change_index + 1) % 2]);

namespace syncer {

/*
static void *TimeThread(void *args) {
  PthreadCall("pthread_detach", pthread_detach(pthread_self()));

  rdp_comm::signalhandler::AddThread(pthread_self(), "timeval.thd");
  while (rdp_comm::signalhandler::IsRunning()) {
    gettimeofday(&g_time, NULL);
    usleep(20);
  }
  rdp_comm::signalhandler::Wait();

  return NULL;
}
*/

static void *StatThread(void *args) {
  PthreadCall("pthread_detach", pthread_detach(pthread_self()));

  rdp_comm::signalhandler::AddThread(pthread_self(), "stat.thd");

  char buff[1024] = {0x0};
  FILE *ttl_fd = StatGetFileHandle("ttl_stat");
  assert(NULL != ttl_fd);

  uint64_t t1;
  uint64_t t2;
  uint64_t t3;
  uint64_t t4;
  uint64_t t5;
  uint64_t t6;
  uint64_t t7;
  uint64_t t8;
  uint64_t t9;
  uint64_t t10;
  uint64_t t11;
  uint64_t t12;
  uint64_t t13;
  
  size_t buf_str_len = 0;

  StatPoint *stat = NULL;

  while (rdp_comm::signalhandler::IsRunning()) {

    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&g_finish_mutex));

    while (g_curr_write->empty()) {
      PthreadCall("pthread_cond_wait",
                  pthread_cond_wait(&g_write_cond, &g_finish_mutex));
    }

    while (!g_curr_write->empty()) {
      stat = g_curr_write->front();
      g_curr_write->pop();
      assert(stat != NULL);

      t1 = rdp_comm::TimevalToUs(stat->t1);
      t2 = rdp_comm::TimevalToUs(stat->t2);
      t3 = rdp_comm::TimevalToUs(stat->t3);
      t4 = rdp_comm::TimevalToUs(stat->t4);
      t5 = rdp_comm::TimevalToUs(stat->t5);
      t6 = rdp_comm::TimevalToUs(stat->t6);
      t7 = rdp_comm::TimevalToUs(stat->t7);
      t8 = rdp_comm::TimevalToUs(stat->t8);
      t9 = rdp_comm::TimevalToUs(stat->t9);
      t10 = rdp_comm::TimevalToUs(stat->t10);
      t11 = rdp_comm::TimevalToUs(stat->t11);
      t12 = rdp_comm::TimevalToUs(stat->t12);
#ifdef NO_KAFKA
      t13 = t12;
#else
      t13 = rdp_comm::TimevalToUs(stat->t13);
#endif

      // write stat file
      buf_str_len = snprintf(buff, sizeof(buff) - 1,
                             "%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64
                             " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64
                             " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64
                             " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
                             stat->seq_no, t1, t2, t3, t4, t5, t6, t7, t8, t9,
                             t10, t11, t12, t13, stat->event_num);

      StatWrite(string(buff, buf_str_len), ttl_fd);
      delete stat;
      stat = NULL;
    }
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&g_finish_mutex));
  }

  rdp_comm::signalhandler::Wait();

  return NULL;
}

static void StartStat(void) {
  //gettimeofday(&g_time, NULL);
  pthread_t tid;
  //PthreadCall("pthread_create", pthread_create(&tid, NULL, TimeThread, NULL));
  PthreadCall("pthread_create", pthread_create(&tid, NULL, StatThread, NULL));
}

void StatTimePoint(StatIndicator type, uint64_t seq_no, StatPoint *stat) {
  if (!g_stat_init_flag) {
    StartStat();
    g_stat_init_flag = true;
  }

  DLOG(INFO) << "Stat Add Type: " << type << " Seq No: " << seq_no;
  struct timeval now;
  gettimeofday(&now, NULL);

  switch (type) {
    case STAT_T1:
      stat->t1 = now;
      break;

    case STAT_T2:
      stat->t2 = now;
      break;

    case STAT_T3:
      stat->t3 = now;
      break;

    case STAT_T4:
      stat->t4 = now;
      break;

    case STAT_T5:
      stat->t5 = now;
      break;

    case STAT_T6:
      stat->t6 = now;
      break;

    case STAT_T7:
      stat->t7 = now;
      break;

    case STAT_T8:
      stat->t8 = now;
      break;

    case STAT_T9:
      stat->t9 = now;
      break;

    case STAT_T10:
      stat->t10 = now;
      break;

    case STAT_T11:
      stat->t11 = now;
      break;

    case STAT_T12:
      stat->t12 = now;
      break;

    case STAT_T13:
      stat->t13 = now;
      break;

    default:
      assert(0);
  }
}

void StatTimePointEnd(StatPoint *stat) {
  assert(NULL != stat);
  g_curr_finish->push(stat);

  if (g_curr_finish->size() > 5000) {
    int ret = pthread_mutex_trylock(&g_finish_mutex);
    switch (ret) {
      case 0:
        g_change_index++;
        g_curr_finish = &(g_finish[g_change_index % 2]);
        g_curr_write = &(g_finish[(g_change_index + 1) % 2]);
        PthreadCall("pthread_cond_signal", pthread_cond_signal(&g_write_cond));
        PthreadCall("pthread_mutex_unlock",
                    pthread_mutex_unlock(&g_finish_mutex));
        break;

      case EINVAL:
      case EAGAIN:
      case EDEADLK:
      case EPERM:
      case EBUSY:
        break;

      default:
        assert(0);
    }
  }

  DLOG(INFO) << "Stat Finish: " << stat->seq_no;
}

FILE *StatGetFileHandle(const string file_name) {
  char tmp[256];
  FILE *fd = NULL;

  pthread_mutex_lock(&g_files_map_mutex);

  AppConfig *conf = g_syncer_app->app_conf_;
  snprintf(tmp, sizeof(tmp), "%s/stat_file-%s-%d",
           conf->GetValue<std::string>("log.dir", "./").c_str(),
           file_name.c_str(), getpid());

  map<string, FILE *>::iterator it = g_files_map.find(file_name);
  if (it == g_files_map.end()) {
    fd = fopen(tmp, "w+b");
  } else {
    if (NULL == it->second) {
      fd = fopen(tmp, "w+b");
    } else {
      fd = it->second;
      pthread_mutex_unlock(&g_files_map_mutex);
      return fd;
    }
  }

  if (NULL == fd) {
    RDP_LOG_ERROR << "fopen " << tmp << " failed: " << rdp_comm::GetLastErrorMsg(errno).c_str();
    pthread_mutex_unlock(&g_files_map_mutex);
    return NULL;
  }

  DLOG(INFO) << "File No: " << fileno(fd) << ", File Name: " << tmp;
  g_files_map[file_name] = fd;

  pthread_mutex_unlock(&g_files_map_mutex);

  return fd;
}

bool StatWrite(const string data, FILE *fd) {
  assert(NULL != fd);
  if (1 != fwrite(data.c_str(), (int)data.length(), 1, fd)) {
    RDP_LOG_ERROR << "fwrite failed: " << rdp_comm::GetLastErrorMsg(errno).c_str();
    return false;
  }
  DLOG(INFO) << "Writed Stat File No: " << fileno(fd) << ", Data: " << data;
  fflush(fd);
  // DLOG(INFO) << "File No: " << fileno(fd) << ", Data: " << data;

  return true;
}

void StatClose(const string file_name) {
  pthread_mutex_lock(&g_files_map_mutex);

  map<string, FILE *>::iterator it = g_files_map.find(file_name);
  if (it == g_files_map.end()) {
    pthread_mutex_unlock(&g_files_map_mutex);
    return;
  }

  FILE *fd = it->second;
  if (NULL != fd) {
    fclose(fd);
  }

  g_files_map.erase(file_name);
  pthread_mutex_unlock(&g_files_map_mutex);
}

void StatCloseAll(void) {
  pthread_mutex_lock(&g_files_map_mutex);
  map<string, FILE *>::iterator it = g_files_map.begin();
  for (; it != g_files_map.end(); ++it) {
    FILE *fd = it->second;
    if (NULL != fd) {
      fclose(fd);
    }
  }
  g_files_map.clear();
  pthread_mutex_unlock(&g_files_map_mutex);
}

uint32_t StatGetIntervalMs(void) {
  static uint32_t interval_ms = 0;
  AppConfig *conf = g_syncer_app->app_conf_;
  assert(NULL != conf);
  if (interval_ms == 0) {
    interval_ms = conf->GetValue<uint32_t>("stat.interval_ms", 60*1000);
  }
  return interval_ms ;
}

}  // namespace syncer

