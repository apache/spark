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
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;

import java.util.HashMap;
import java.util.Map;

public class ReadClientMetrics extends MetricGroup<ReadClientMetricsKey> {

  private final Counter numIgnoredBlocks;
  private final Counter numReadBytes;
  private final Timer readConnectLatency;
  private final Timer reducerWaitTime;

  private final Gauge bufferSize;

  public ReadClientMetrics(ReadClientMetricsKey key) {
    super(key);

    // The name like "numClients" was used when there were a lot of metric series which caused M3 issue, e.g.
    // not able to load the values in dashboard. Use new names ending with a number suffix to create new metrics in M3.
    this.numIgnoredBlocks = scope.counter("numIgnoredBlocks4");
    this.numReadBytes = scope.counter("numReadBytes4");
    this.readConnectLatency = scope.timer("readConnectLatency4");
    this.reducerWaitTime = scope.timer("reducerWaitTime4");
    this.bufferSize = scope.gauge("bufferSize4");
  }

  public Counter getNumIgnoredBlocks() {
    return numIgnoredBlocks;
  }

  public Counter getNumReadBytes() {
    return numReadBytes;
  }

  public Timer getReadConnectLatency() {
    return readConnectLatency;
  }

  public Timer getReducerWaitTime() {
    return reducerWaitTime;
  }

  public Gauge getBufferSize() {
    return bufferSize;
  }

  @Override
  protected Scope createScope(ReadClientMetricsKey key) {
    Map<String, String> tags = new HashMap<>();
    tags.put(M3Stats.TAG_NAME_SOURCE, key.getSource());
    tags.put(M3Stats.TAG_NAME_USER, key.getUser());
    return M3Stats.createSubScope(tags);
  }
}
