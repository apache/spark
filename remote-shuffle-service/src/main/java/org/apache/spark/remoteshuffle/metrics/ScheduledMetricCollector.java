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

import com.uber.m3.tally.Gauge;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry;
import org.apache.spark.remoteshuffle.util.FileUtils;
import org.apache.spark.remoteshuffle.util.NetworkUtils;
import org.apache.spark.remoteshuffle.util.ServerHostAndPort;
import org.apache.spark.remoteshuffle.util.SystemUtils;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledMetricCollector {
  private static final Logger logger = LoggerFactory.getLogger(ScheduledMetricCollector.class);

  private final ServiceRegistry serviceRegistry;

  private static final Gauge activeNodeCount = M3Stats.getDefaultScope().gauge("activeNodeCount");
  private static final Gauge numFileDescriptors =
      M3Stats.getDefaultScope().gauge("numFileDescriptors");
  private static final Gauge nettyPooledHeapMemory =
      M3Stats.getDefaultScope().gauge("nettyPooledHeapMemory");
  private static final Gauge nettyPooledDirectMemory =
      M3Stats.getDefaultScope().gauge("nettyPooledDirectMemory");
  private static final Gauge jvmUsedHeapMemory =
      M3Stats.getDefaultScope().gauge("jvmUsedHeapMemory");
  private static final Gauge unreachableHosts =
      M3Stats.getDefaultScope().gauge("unreachableHosts");
  private static final Gauge unreachableHostsCheckLatency =
      M3Stats.getDefaultScope().gauge("unreachableHostsCheckLatency");
  private static final Gauge fileStoreUsableSpace =
      M3Stats.getDefaultScope().gauge("fileStoreUsableSpace");

  private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

  private final int metricCollectingIntervalSeconds = 60;

  public ScheduledMetricCollector(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public void scheduleCollectingMetrics(ScheduledExecutorService scheduledExecutorService,
                                        String dataCenter, String cluster) {
    Random random = new Random();
    int scheduleInitialDelay = random.nextInt(metricCollectingIntervalSeconds);
    scheduledExecutorService.scheduleAtFixedRate(() -> collectMetrics(dataCenter, cluster),
        scheduleInitialDelay,
        metricCollectingIntervalSeconds,
        TimeUnit.SECONDS);
  }

  public void collectMetrics(String dataCenter, String cluster) {
    try {
      List<ServerDetail> nodes = serviceRegistry
          .getServers(dataCenter, cluster, Integer.MAX_VALUE, Collections.emptyList());
      int count = nodes == null ? 0 : nodes.size();
      activeNodeCount.update(count);

      numFileDescriptors.update(SystemUtils.getFileDescriptorCount());

      PooledByteBufAllocatorMetric pooledByteBufAllocatorMetric =
          PooledByteBufAllocator.DEFAULT.metric();
      nettyPooledHeapMemory.update(pooledByteBufAllocatorMetric.usedHeapMemory());
      nettyPooledDirectMemory.update(pooledByteBufAllocatorMetric.usedDirectMemory());

      MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
      jvmUsedHeapMemory.update(memoryUsage.getUsed());

      // check whether current host is first server.
      // if current host is first server, check whether other servers are reachable.
      // this is to make sure there is only one server to do such checking to avoid servers ping each other too much
      if (nodes != null && !nodes.isEmpty()) {
        String firstServerConnectionString = nodes.stream().min(new Comparator<ServerDetail>() {
          @Override
          public int compare(ServerDetail o1, ServerDetail o2) {
            return StringUtils.compare(o1.getConnectionString(), o2.getConnectionString());
          }
        })
            .get()
            .getConnectionString();
        String localHostName = NetworkUtils.getLocalHostName();
        boolean isMyselfFirstServer =
            firstServerConnectionString.toLowerCase().contains(localHostName.toLowerCase());
        if (isMyselfFirstServer) {
          long startTime = System.currentTimeMillis();
          List<String> unreachableHostList = new ArrayList<>(nodes.size());
          for (ServerDetail node : nodes) {
            if (node.getConnectionString().equals(firstServerConnectionString)) {
              continue;
            }
            ServerHostAndPort hostAndPort =
                ServerHostAndPort.fromString(node.getConnectionString());
            boolean isReachable = NetworkUtils
                .isReachable(hostAndPort.getHost(), NetworkUtils.DEFAULT_REACHABLE_TIMEOUT);
            if (!isReachable) {
              unreachableHostList.add(hostAndPort.getHost());
            }
          }
          unreachableHosts.update(unreachableHostList.size());
          if (!unreachableHostList.isEmpty()) {
            logger.warn(String.format("Detected unreachable hosts: %s",
                StringUtils.join(unreachableHostList, ",")));
          }
          unreachableHostsCheckLatency.update(System.currentTimeMillis() - startTime);
        }
      }

      // check disk storage available space
      long usableSpace = FileUtils.getFileStoreUsableSpace();
      fileStoreUsableSpace.update(usableSpace);
    } catch (Throwable ex) {
      M3Stats.addException(ex, this.getClass().getSimpleName());
      logger.warn("Failed to collect metrics", ex);
    }
  }
}
