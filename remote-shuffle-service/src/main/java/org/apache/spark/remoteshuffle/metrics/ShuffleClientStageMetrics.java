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
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.ValueBuckets;

import java.util.HashMap;
import java.util.Map;

public class ShuffleClientStageMetrics extends MetricGroup<ShuffleClientStageMetricsKey> {

  private final Counter numRegisterShuffle;
  private final Histogram numMappers;
  private final Histogram numReducers;

  public ShuffleClientStageMetrics(ShuffleClientStageMetricsKey key) {
    super(key);

    this.numRegisterShuffle = scope.counter("numRegisterShuffle");
    this.numMappers = scope.histogram("numMappers", ValueBuckets.linear(0, 100, 100));
    this.numReducers = scope.histogram("numReducers", ValueBuckets.linear(0, 100, 100));
  }

  public Counter getNumRegisterShuffle() {
    return numRegisterShuffle;
  }

  public Histogram getNumMappers() {
    return numMappers;
  }

  public Histogram getNumReducers() {
    return numReducers;
  }

  @Override
  protected Scope createScope(ShuffleClientStageMetricsKey key) {
    Map<String, String> tags = new HashMap<>();
    tags.put(M3Stats.TAG_NAME_USER, key.getUser());
    tags.put(M3Stats.TAG_NAME_QUEUE, key.getQueue());
    return M3Stats.createSubScope(tags);
  }
}
