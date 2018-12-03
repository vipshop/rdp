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

#include "util_sub_process.h"
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>

using namespace std;

namespace syncer {

bool StringWriter::Write(const char *buf, int &len) { 
  str_.append(buf, len);
  return true;
}


FileWriter::FileWriter(const string& filename) 
  : filename_(filename), fp_(NULL) {

}

FileWriter::~FileWriter() {
  if (fp_) {
    fclose(fp_);
  }
}

bool FileWriter::Write(const char *buf, int &len) { 
  if (!fp_) {
    fp_ = fopen(filename_.c_str(), "w+b");
    if (!fp_) {
      perror("fopen");
      return false;
    }
  }
  if (1 != fwrite(buf, len, 1, fp_)) {
    perror("fwrite");
    return false;
  }

  return true;
}


SubProcess::SubProcess()
  : cpid_(-1), args_(NULL), 
  stdout_fd_(-1), stderr_fd_(-1),
  stdout_(NULL), stderr_(NULL) {
}

SubProcess::~SubProcess() {
  if (stdout_fd_ > 0) {
    close(stdout_fd_);
  }
  if (stderr_fd_ > 0) {
    close(stderr_fd_);
  }
  if (args_) {
    free(args_);
  }
}

void SubProcess::SetCommand(const string &program, int argc, ...) {
  program_ = program;
  args_ = (char**)malloc(sizeof(char*) * (argc+2));
  args_[0] = (char*)program.c_str();
  va_list args;
  va_start(args, argc);
  int i = 0;
  for (; i<argc; i++) {
    args_[i+1] = va_arg(args, char*);
  }
  args_[i+1] = NULL;
  va_end(args);
}

bool SubProcess::Create() {
  pid_t cpid;
  int stdout_pipe[2];
  int stderr_pipe[2];

  if (stdout_  && pipe(stdout_pipe) == -1) {
    perror("pipe");
    return false;
  }
  if (stderr_  && pipe(stderr_pipe) == -1) {
    perror("pipe");
    return false;
  }


  cpid = fork(); 
  if (cpid == -1) {
    perror("fork");
    return false;
  }

  if (cpid == 0) { // Child process
    if (stdout_) {
      close(stdout_pipe[0]);
      if (dup2(stdout_pipe[1], 1) == -1) {
        perror("dup2");
        abort();
      }
    } 
    if (stderr_) {
      close(stderr_pipe[0]);
      if (dup2(stderr_pipe[1], 2) == -1) {
        perror("dup2");
        abort();
      }
    }

    // TODO: Close other fd
    

    execvp(program_.c_str(), args_);
    perror("execvp");
    abort();

  }

  // Father process
  cpid_ = cpid;
  if (stdout_) {
    close(stdout_pipe[1]);
    stdout_fd_ = stdout_pipe[0];

    int old_flag;
    if ((old_flag = fcntl(stdout_fd_, F_GETFL,0)) < 0 || 
        fcntl(stdout_fd_, F_SETFL,old_flag|O_NONBLOCK) < 0 ) {
      perror("fcntl");
      abort();
    }
  } 
  if (stderr_) {
    close(stderr_pipe[1]);
    stderr_fd_ = stderr_pipe[0];

    int old_flag;
    if ((old_flag = fcntl(stderr_fd_, F_GETFL,0)) < 0 || 
        fcntl(stderr_fd_, F_SETFL,old_flag|O_NONBLOCK) < 0 ) {
      perror("fcntl");
      abort();
    }
  }

  return true;
}

bool SubProcess::Wait(int *ret) {
  const int BUFFER_SIZE = 4096;
  char buf[BUFFER_SIZE];

  if (cpid_<=0) {
    return false;
  }

  while (stderr_ || stdout_) {
    bool no_out = false;
    bool no_err = false;

    if (stdout_) {
      int n = read(stdout_fd_, buf, sizeof(buf)) ;
      if (n > 0) {
        if (stdout_ && !stdout_->Write(buf, n)) {
          return false;
        }
      } else if (n == 0) {
        stdout_ = NULL;
      } else if (errno == EINTR || errno == EAGAIN) {
        // No data
        no_out = true;
      } else {
          perror("read stdout");
          return false;
      } 
    }

    if (stderr_) {
      int n = read(stderr_fd_, buf, sizeof(buf)) ;
      if (n > 0) {
        if (stderr_ && !stderr_->Write(buf, n)) {
          return false;
        }
      } else if (n == 0) {
        stderr_ = NULL;
      } else if (errno == EINTR || errno == EAGAIN) {
        // No data
        no_err = true;
      } else {
        perror("read stderr");
        return false;
      }
    }
    if (no_out && no_err) {
      // Sleep 100ms
      usleep(100*1000);
    }
  } //while

  int status;
  int pid = waitpid(cpid_, &status, 0);

  if (pid == -1) {
    perror("waitpid");
    return false;
  }

  if(WIFEXITED(status)) {
    *ret = WEXITSTATUS(status);
  } else if(WIFSIGNALED(status)) {
    *ret = WTERMSIG(status);
  } else {
    *ret = -1;
  }

  return true;

}

bool SubProcess::Output(int *status, string& output) {
  StringWriter sw;
  SetStdout(&sw);

  if (!Create()) {
    return false;
  }

  if (!Wait(status)) {
    return false;
  }

  output = sw.AsString();

  return true;

}

}

