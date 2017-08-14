/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.lang.reflect.Method;
import java.util.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * A Netty memory metrics class to collect metrics from Netty PooledByteBufAllocator.
 */
public class NettyMemoryMetrics implements MetricSet {

  private final PooledByteBufAllocator pooledAllocator;

  private final Map<String, Metric> allMetrics;

  private final String metricPrefix;

  public NettyMemoryMetrics(PooledByteBufAllocator pooledAllocator, String metricPrefix) {
    this.pooledAllocator = pooledAllocator;
    this.allMetrics = new HashMap<>();
    this.metricPrefix = metricPrefix;

    // using reflection to register all the metrics of this allocator.
    registerMetrics(this.pooledAllocator);

    // register the String Gauge to dump all the details of this allocator.
    allMetrics.put(MetricRegistry.name(metricPrefix, "stats"),
      (Gauge<String>) () -> this.pooledAllocator.dumpStats());
  }

  private void registerMetrics(PooledByteBufAllocator allocator) {
    int directArenaIndex = 0;
    for (PoolArenaMetric metric : allocator.directArenas()) {
      registerArenaMetric(metric, "directArena" + directArenaIndex);
      directArenaIndex++;
    }

    int heapArenaIndex = 0;
    for (PoolArenaMetric metric : allocator.heapArenas()) {
      registerArenaMetric(metric, "heapArena" + heapArenaIndex);
      heapArenaIndex++;
    }

    Set<String> uniqueKeySet = new HashSet<>();
    for (String key : allMetrics.keySet()) {
      String metricName = key.substring(key.lastIndexOf(".") + 1);
      uniqueKeySet.add(metricName);
    }

    // Aggregate metrics of different arenas to sum together as the metrics for this allocator.
    if (pooledAllocator.numDirectArenas() > 0) {
      registerAggregatedArenaMetric("directArena", pooledAllocator.numDirectArenas(), uniqueKeySet);
    }

    if (pooledAllocator.numHeapArenas() > 0) {
      registerAggregatedArenaMetric("heapArena", pooledAllocator.numHeapArenas(), uniqueKeySet);
    }
  }

  private void registerArenaMetric(PoolArenaMetric arenaMetric, String arenaName) {
    for (Method m : PoolArenaMetric.class.getDeclaredMethods()) {
      Class<?> returnType = m.getReturnType();
      String metricName = MetricRegistry.name(metricPrefix, arenaName, m.getName());
      if (returnType.equals(int.class)) {
        allMetrics.put(metricName, (Gauge<Integer>) () -> {
          try {
            return (Integer) m.invoke(arenaMetric);
          } catch (Exception e) {
            return -1; // Swallow the exceptions.
          }
        });

      } else if (returnType.equals(long.class)) {
        allMetrics.put(metricName, (Gauge<Long>) () -> {
          try {
            return (Long) m.invoke(arenaMetric);
          } catch (Exception e) {
            return -1L; // Swallow the exceptions.
          }
        });

      } else {
        // Neglect all other unknown metrics: tinySubpages, smallSubpages chunkList details,
        // these are too verbose to report.
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void registerAggregatedArenaMetric(String arenaType, int numArenas, Set<String> keySet) {
    for (String key : keySet) {
      String metricName = MetricRegistry.name(metricPrefix, arenaType, key);
      Class<?> returnType;
      try {
        returnType = PoolArenaMetric.class.getMethod(key).getReturnType();
      } catch (Exception e) {
        continue; // Bypass current metric if exception met.
      }

      if (returnType.equals(int.class)) {
        Gauge<Integer> gauge = () -> {
          int ret = 0;
          for (int i = 0; i < numArenas; i++) {
            ret += ((Gauge<Integer>) (allMetrics.get(MetricRegistry.name(metricPrefix,
              arenaType + i, key)))).getValue();
          }
          return ret;
        };

        allMetrics.put(metricName, gauge);

      } else if (returnType.equals(long.class)) {
        Gauge<Long> gauge = () -> {
          long ret = 0;
          for (int i = 0; i < numArenas; i++) {
            ret += ((Gauge<Long>) (allMetrics.get(MetricRegistry.name(metricPrefix,
              arenaType + i, key)))).getValue();
          }
          return ret;
        };

        allMetrics.put(metricName, gauge);

      } else {
        // Assume there's no other types of metrics
      }
    }
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return Collections.unmodifiableMap(allMetrics);
  }
}
