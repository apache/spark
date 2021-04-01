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

package org.apache.spark.remoteshuffle.common;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import com.uber.m3.tally.Gauge;
import org.apache.spark.remoteshuffle.exceptions.RssException;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;

public class MemoryMonitor {
  private static final Logger logger = LoggerFactory.getLogger(MemoryMonitor.class);

  private static final Gauge heapMemoryPercentage =
      M3Stats.getDefaultScope().gauge("heapMemoryPercentage");
  private static final Gauge gcTime = M3Stats.getDefaultScope().gauge("gcTime");
  private static final Gauge majorGCTime = M3Stats.getDefaultScope().gauge("majorGCTime");

  public void addLowMemoryListener(int lowMemoryPercentage, LowMemoryListener lowMemoryListener) {
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    NotificationListener listener = createListener(lowMemoryPercentage, lowMemoryListener);
    for (GarbageCollectorMXBean entry : gcBeans) {
      NotificationEmitter emitter = (NotificationEmitter) entry;
      emitter.addNotificationListener(listener, null, null);
      logger.info(
          String.format("Added notification listener for garbage collector: %s", entry.getName()));
    }
  }

  private NotificationListener createListener(int lowMemoryPercentage,
                                              LowMemoryListener lowMemoryListener) {
    return new NotificationListener() {
      @Override
      public void handleNotification(Notification notification, Object handback) {
        if (notification.getType().equals(
            GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
          GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo
              .from((CompositeData) notification.getUserData());
          String gcAction = info.getGcAction();
          GcInfo gcInfo = info.getGcInfo();
          logger.info(String.format("GcAction: %s, start: %s, duration: %s",
              gcAction, gcInfo.getStartTime(), gcInfo.getDuration()));
          gcTime.update(gcInfo.getDuration());
          if (gcAction.toLowerCase().contains("major")) {
            majorGCTime.update(gcInfo.getDuration());
            // only check when major (full) GC
            MemoryMXBean memoryMXBean;
            try {
              memoryMXBean = ManagementFactory.getMemoryMXBean();
            } catch (Throwable ex) {
              M3Stats.addException(ex, this.getClass().getSimpleName());
              throw new RssException("Failed to run getMemoryMXBean", ex);
            }

            MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
            long usedPercentage = memoryUsage.getUsed() * 100 / memoryUsage.getMax();
            logger.info(String.format("Heap memory, used: %s (%s%%), max: %s, committed: %s",
                memoryUsage.getUsed(), usedPercentage, memoryUsage.getMax(),
                memoryUsage.getCommitted()));
            heapMemoryPercentage.update(usedPercentage);

            if (usedPercentage > lowMemoryPercentage) {
              logger.info(String.format(
                  "Triggering low memory listener due to used memory percentage %s larger than threshold %s",
                  usedPercentage, lowMemoryPercentage));
              lowMemoryListener.run();
            }
          }
        }
      }
    };
  }
}
