/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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

public class NotifyClientMetrics extends MetricGroup<NotifyClientMetricsKey> {

  private final Counter numClients;

  public NotifyClientMetrics(NotifyClientMetricsKey key) {
    super(key);

    this.numClients = scope.counter("numClients");
  }

  public Counter getNumClients() {
    return numClients;
  }

  @Override
  protected Scope createScope(NotifyClientMetricsKey key) {
    Map<String, String> tags = new HashMap<>();
    tags.put(M3Stats.TAG_NAME_SOURCE, key.getSource());
    tags.put(M3Stats.TAG_NAME_USER, key.getUser());
    return M3Stats.createSubScope(tags);
  }
}
