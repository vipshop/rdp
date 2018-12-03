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
#include <iostream>
#include "../comm_incl.h"
#include "rdp_environment.h"

DEFINE_string(cfg, "./test.cfg", "Configure File.");

using namespace rdp_comm;

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(string(argv[0]).append(".log").c_str());

  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  g_env = new RdpEnvironment(FLAGS_cfg);
  testing::AddGlobalTestEnvironment(g_env);

  FLAGS_stderrthreshold = google::INFO;
  FLAGS_colorlogtostderr = true;
  FLAGS_log_dir = "./";

  return RUN_ALL_TESTS();
}
