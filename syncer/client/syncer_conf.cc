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

#include "syncer_conf.h"
#include "syncer_utils.h"
#include <fstream>

namespace syncer {


AppConfig::AppConfig(const std::string &file_name) : file_name_(file_name) {
  if (!Read()){
    std::cerr<<"Error reading file:"<<file_name<<std::endl;
    assert(0);
  }
}

AppConfig::~AppConfig() {}

// Read config section from file
bool AppConfig::Read(void) {
	std::fstream f;
  f.open(file_name_.c_str(), std::fstream::in);
  if (!f.is_open()) {
    return false;
  }

	std::string line;
  int lnr = -1;
  while (std::getline(f, line)) {
    lnr++;
    // Skip Comments and empty lines
    if (!line.length())
      continue;
    if (line[0] == '#')
      continue;
    if (line[0] == ';')
      continue;

    std::istringstream is_line(line);
    std::string key;
    if (std::getline(is_line, key, '=')) {
      std::string value;
      if (std::getline(is_line, value)) {
        key = Trim(key);
        value = Trim(value);
        Str2Lower(key);

        if (cfg_map_.find(key) != cfg_map_.end()) {
          std::cerr << "WARNING: Statement '" << line << "' in file " << file_name_
               << ":" << lnr << " redefines a value!" << std::endl;
        }
				//std::cout << "Key: " << key << " , Value: " << value << std::endl;
        cfg_map_[key] = value;
      }
    } else {
      std::cerr << "WARNING: Invalid line: " << line << std::endl;
    }
  }

  f.close();
  return true;
}

// Dump to stdout
void AppConfig::Dump(void) {
  for (CfgMap::iterator iter = cfg_map_.begin();
       iter != cfg_map_.end(); ++iter) {
    std::cout << "key: '" << iter->first << "' \t value: '" << iter->second
              << "'" << std::endl;
  }
}

} // namespace syncer

