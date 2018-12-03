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

#ifndef __UTIL_SUB_PROCESS_H__
#define __UTIL_SUB_PROCESS_H__

#include <stdio.h>
#include <string>

namespace syncer {

// Writer is an interface which can be used to write bytes into it
class Writer
{
 public:
  virtual bool Write(const char *buf, int &len) = 0;
};


// StringWriter can be used to store bytes, into a string
class StringWriter : public Writer
{
 public:
  virtual bool Write(const char *buf, int &len);
  
  const std::string AsString() const { return str_; } 

 private:
  std::string str_;
};


// FileWriter can be used to store bytes, into a file
class FileWriter : public Writer
{
 public:
  FileWriter(const std::string &filename_);
  ~FileWriter();

  virtual bool Write(const char *buf, int &len);

 private:
  std::string filename_;
  FILE *fp_;
};


// SubProcess can be used to fork and exec a command,
// and you can redirect the 'stdout' of the sub process,
// into a Writer, like FileWriter 
class SubProcess
{
 public:
  SubProcess(); 
  ~SubProcess();

  bool Create();
  bool Wait(int *status);
  bool Output(int *status, std::string& output);
  
  void SetCommand(const std::string &program, int argc, ...);
  void SetStdout(Writer *out) { stdout_ = out; } 
  void SetStderr(Writer *err) { stderr_ = err; } 

 private:
  int cpid_;

  std::string program_;
  char **args_;

  int stdout_fd_;
  int stderr_fd_;

  // Stdout of this sub process
  Writer *stdout_;
  Writer *stderr_;
};

}
#endif
