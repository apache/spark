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
import java.lang.reflect.Modifier;
import java.util.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;

/**
 * A Netty memory metrics class to collect metrics from Netty PooledByteBufAllocator.
 */
public class NettyMemoryMetrics implements MetricSet {

  private final PooledByteBufAllocator pooledAllocator;

  private final boolean verboseMetricsEnabled;

  private final Map<String, Metric> allMetrics;

  private final String metricPrefix;

  @VisibleForTesting
  static final Set<String> VERBOSE_METRICS = new HashSet<>();
  static {
    VERBOSE_METRICS.addAll(Arrays.asList(
      "numAllocations",
      "numTinyAllocations",
      "numSmallAllocations",
      "numNormalAllocations",
      "numHugeAllocations",
      "numDeallocations",
      "numTinyDeallocations",
      "numSmallDeallocations",
      "numNormalDeallocations",
      "numHugeDeallocations",
      "numActiveAllocations",
      "numActiveTinyAllocations",
      "numActiveSmallAllocations",
      "numActiveNormalAllocations",
      "numActiveHugeAllocations",
      "numActiveBytes"));
  }

  public NettyMemoryMetrics(PooledByteBufAllocator pooledAllocator,
      String metricPrefix,
      TransportConf conf) {
    this.pooledAllocator = pooledAllocator;
    this.allMetrics = new HashMap<>();
    this.metricPrefix = metricPrefix;
    this.verboseMetricsEnabled = conf.verboseMetrics();

    registerMetrics(this.pooledAllocator);
  }

  private void registerMetrics(PooledByteBufAllocator allocator) {
    PooledByteBufAllocatorMetric pooledAllocatorMetric = allocator.metric();

    // Register general metrics.
    allMetrics.put(MetricRegistry.name(metricPrefix, "usedHeapMemory"),
      (Gauge<Long>) () -> pooledAllocatorMetric.usedHeapMemory());
    allMetrics.put(MetricRegistry.name(metricPrefix, "usedDirectMemory"),
      (Gauge<Long>) () -> pooledAllocatorMetric.usedDirectMemory());

    if (verboseMetricsEnabled) {
      int directArenaIndex = 0;
      for (PoolArenaMetric metric : pooledAllocatorMetric.directArenas()) {
        registerArenaMetric(metric, "directArena" + directArenaIndex);
        directArenaIndex++;
      }

      int heapArenaIndex = 0;
      for (PoolArenaMetric metric : pooledAllocatorMetric.heapArenas()) {
        registerArenaMetric(metric, "heapArena" + heapArenaIndex);
        heapArenaIndex++;
      }
    }
  }

  private void registerArenaMetric(PoolArenaMetric arenaMetric, String arenaName) {
    for (String methodName : VERBOSE_METRICS) {
      Method m;
      try {
        m = PoolArenaMetric.class.getMethod(methodName);
      } catch (Exception e) {
        // Failed to find metric related method, ignore this metric.
        continue;
      }

      if (!Modifier.isPublic(m.getModifiers())) {
        // Ignore non-public methods.
        continue;
      }

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
      }
    }
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return Collections.unmodifiableMap(allMetrics);
  }
}
