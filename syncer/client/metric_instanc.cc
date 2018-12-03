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

#include "rdp_syncer_metric.h"

namespace syncer {

void CreateMetricInstance(void) {
  g_syncer_metric = new RDPSyncerMetric();
  assert(g_syncer_metric);
}

void CloseMetricInstance(void) {
  #define CLEANUP_OBJ(X) if (X) { delete X; X = NULL; } 
  CLEANUP_OBJ(g_syncer_metric);
}

}  // namespace syncer
