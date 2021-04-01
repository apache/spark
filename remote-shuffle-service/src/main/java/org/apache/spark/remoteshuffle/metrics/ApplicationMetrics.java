/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.metrics;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;

import java.util.HashMap;
import java.util.Map;

public class ApplicationMetrics extends MetricGroup<ApplicationMetricsKey> {

  private final Counter numApplications;

  public ApplicationMetrics(ApplicationMetricsKey key) {
    super(key);

    this.numApplications = scope.counter("numApplications");
  }

  public Counter getNumApplications() {
    return numApplications;
  }

  @Override
  protected Scope createScope(ApplicationMetricsKey key) {
    Map<String, String> tags = new HashMap<>();
    tags.put(M3Stats.TAG_NAME_USER, key.getUser());
    tags.put(M3Stats.TAG_NAME_ATTEMPT_ID, key.getAttemptId());
    return M3Stats.createSubScope(tags);
  }
}
