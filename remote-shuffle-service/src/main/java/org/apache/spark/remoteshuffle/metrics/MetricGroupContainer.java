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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MetricGroupContainer<K, M extends MetricGroup<K>> {
  private final Function<? super K, ? extends M> createFunction;

  private final ConcurrentHashMap<K, M> metricGroups = new ConcurrentHashMap<>();

  public MetricGroupContainer(Function<? super K, ? extends M> createFunction) {
    this.createFunction = createFunction;
  }

  public M getMetricGroup(K key) {
    return metricGroups.computeIfAbsent(key, createFunction);
  }

  public void removeMetricGroup(K key) {
    M metricGroup = metricGroups.remove(key);
    if (metricGroup != null) {
      metricGroup.close();
    }
  }
}
