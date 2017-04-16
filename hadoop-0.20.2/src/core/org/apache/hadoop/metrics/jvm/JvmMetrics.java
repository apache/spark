/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.metrics.jvm;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;

import static java.lang.Thread.State.*;
import java.lang.management.GarbageCollectorMXBean;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Singleton class which reports Java Virtual Machine metrics to the metrics API.  
 * Any application can create an instance of this class in order to emit
 * Java VM metrics.  
 */
public class JvmMetrics implements Updater {
    
    private static final float M = 1024*1024;
    private static JvmMetrics theInstance = null;
    private static Log log = LogFactory.getLog(JvmMetrics.class);
    
    private MetricsRecord metrics;
    
    // garbage collection counters
    private long gcCount = 0;
    private long gcTimeMillis = 0;
    
    // logging event counters
    private long fatalCount = 0;
    private long errorCount = 0;
    private long warnCount  = 0;
    private long infoCount  = 0;
    
    public synchronized static JvmMetrics init(String processName, String sessionId) {
      return init(processName, sessionId, "metrics");
    }
    
    public synchronized static JvmMetrics init(String processName, String sessionId,
      String recordName) {
        if (theInstance != null) {
            log.info("Cannot initialize JVM Metrics with processName=" + 
                     processName + ", sessionId=" + sessionId + 
                     " - already initialized");
        }
        else {
            log.info("Initializing JVM Metrics with processName=" 
                    + processName + ", sessionId=" + sessionId);
            theInstance = new JvmMetrics(processName, sessionId, recordName);
        }
        return theInstance;
    }
    
    /** Creates a new instance of JvmMetrics */
    private JvmMetrics(String processName, String sessionId,
      String recordName) {
        MetricsContext context = MetricsUtil.getContext("jvm");
        metrics = MetricsUtil.createRecord(context, recordName);
        metrics.setTag("processName", processName);
        metrics.setTag("sessionId", sessionId);
        context.registerUpdater(this);
    }
    
    /**
     * This will be called periodically (with the period being configuration
     * dependent).
     */
    public void doUpdates(MetricsContext context) {
        doMemoryUpdates();
        doGarbageCollectionUpdates();
        doThreadUpdates();
        doEventCountUpdates();
        metrics.update();
    }
    
    private void doMemoryUpdates() {
        MemoryMXBean memoryMXBean =
               ManagementFactory.getMemoryMXBean();
        MemoryUsage memNonHeap =
                memoryMXBean.getNonHeapMemoryUsage();
        MemoryUsage memHeap =
                memoryMXBean.getHeapMemoryUsage();
        Runtime runtime = Runtime.getRuntime();

        metrics.setMetric("memNonHeapUsedM", memNonHeap.getUsed()/M);
        metrics.setMetric("memNonHeapCommittedM", memNonHeap.getCommitted()/M);
        metrics.setMetric("memHeapUsedM", memHeap.getUsed()/M);
        metrics.setMetric("memHeapCommittedM", memHeap.getCommitted()/M);
        metrics.setMetric("maxMemoryM", runtime.maxMemory()/M);
    }
    
    private void doGarbageCollectionUpdates() {
        List<GarbageCollectorMXBean> gcBeans =
                ManagementFactory.getGarbageCollectorMXBeans();
        long count = 0;
        long timeMillis = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            count += gcBean.getCollectionCount();
            timeMillis += gcBean.getCollectionTime();
        }
        metrics.incrMetric("gcCount", (int)(count - gcCount));
        metrics.incrMetric("gcTimeMillis", (int)(timeMillis - gcTimeMillis));
        
        gcCount = count;
        gcTimeMillis = timeMillis;
    }
    
    private void doThreadUpdates() {
        ThreadMXBean threadMXBean =
                ManagementFactory.getThreadMXBean();
        long threadIds[] = 
                threadMXBean.getAllThreadIds();
        ThreadInfo[] threadInfos =
                threadMXBean.getThreadInfo(threadIds, 0);
        
        int threadsNew = 0;
        int threadsRunnable = 0;
        int threadsBlocked = 0;
        int threadsWaiting = 0;
        int threadsTimedWaiting = 0;
        int threadsTerminated = 0;
        
        for (ThreadInfo threadInfo : threadInfos) {
            // threadInfo is null if the thread is not alive or doesn't exist
            if (threadInfo == null) continue;
            Thread.State state = threadInfo.getThreadState();
            if (state == NEW) {
                threadsNew++;
            } 
            else if (state == RUNNABLE) {
                threadsRunnable++;
            }
            else if (state == BLOCKED) {
                threadsBlocked++;
            }
            else if (state == WAITING) {
                threadsWaiting++;
            } 
            else if (state == TIMED_WAITING) {
                threadsTimedWaiting++;
            }
            else if (state == TERMINATED) {
                threadsTerminated++;
            }
        }
        metrics.setMetric("threadsNew", threadsNew);
        metrics.setMetric("threadsRunnable", threadsRunnable);
        metrics.setMetric("threadsBlocked", threadsBlocked);
        metrics.setMetric("threadsWaiting", threadsWaiting);
        metrics.setMetric("threadsTimedWaiting", threadsTimedWaiting);
        metrics.setMetric("threadsTerminated", threadsTerminated);
    }
    
    private void doEventCountUpdates() {
        long newFatal = EventCounter.getFatal();
        long newError = EventCounter.getError();
        long newWarn  = EventCounter.getWarn();
        long newInfo  = EventCounter.getInfo();
        
        metrics.incrMetric("logFatal", (int)(newFatal - fatalCount));
        metrics.incrMetric("logError", (int)(newError - errorCount));
        metrics.incrMetric("logWarn",  (int)(newWarn - warnCount));
        metrics.incrMetric("logInfo",  (int)(newInfo - infoCount));
        
        fatalCount = newFatal;
        errorCount = newError;
        warnCount  = newWarn;
        infoCount  = newInfo;
    }
}
