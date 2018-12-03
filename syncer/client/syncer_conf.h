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

#ifndef _SYNCER_CONF_H_
#define _SYNCER_CONF_H_

#include <getopt.h>
#include <iostream>
#include <list>
#include <map>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <assert.h>

namespace syncer {

class AppConfig {
  typedef std::map<std::string, std::string> CfgMap;

public:
  explicit AppConfig(const std::string &file_name);
  ~AppConfig();

public:
  // Dump to stdout
  void Dump(void);

  // Query config by key
  template <typename T> T GetValue(const std::string &key) {
    CfgMap::iterator iter = cfg_map_.find(key);
    if (cfg_map_.end() == iter) {
      std::cerr << "WARNING: '" << key << "' was not defined in " << file_name_
                << "! Value is undefined!" << std::endl;
      assert(0);
    }

    std::string v = iter->second;
    std::stringstream ss;
    ss << v;
    T value;
    ss >> value;
    return value;
  }

  // Query config by key, return default value if not configured
  template <typename T> T GetValue(const std::string &key, T default_value) {
    CfgMap::iterator iter = cfg_map_.find(key);
    if (cfg_map_.end() == iter) {
      std::cerr << "WARNING: '" << key << "' was not defined in " << file_name_
                << "! Apply default value!" << std::endl;
      return default_value;
    }

    return GetValue<T>(key);
  }

private:
  // Read config section from file
  bool Read(void);

private:
  CfgMap cfg_map_;
  std::string file_name_;
};

} // namespace syncer

#endif // _SYNCER_CONF_H_

