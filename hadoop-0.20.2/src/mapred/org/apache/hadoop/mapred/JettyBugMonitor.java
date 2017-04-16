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
package org.apache.hadoop.mapred;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.nio.SelectChannelConnector;

/**
 * Class that monitors for a certain class of Jetty bug known to
 * affect TaskTrackers. In this type of bug, the Jetty selector
 * thread starts spinning and using ~100% CPU while no actual
 * HTTP content is being served. Given that this bug has been
 * active in Jetty/JDK for a long time with no resolution in site,
 * this class provides a temporary workaround.
 * 
 * Upon detecting the selector thread spinning, it simply exits the
 * JVM with a Fatal message.
 */
class JettyBugMonitor extends Thread {
  private final static Log LOG = LogFactory.getLog(
      JettyBugMonitor.class);

  private static final ThreadMXBean threadBean =
    ManagementFactory.getThreadMXBean();

  private static final String CHECK_ENABLED_KEY =
    "mapred.tasktracker.jetty.cpu.check.enabled";
  private static final boolean CHECK_ENABLED_DEFAULT = true;
  
  static final String CHECK_INTERVAL_KEY =
    "mapred.tasktracker.jetty.cpu.check.interval";
  private static final long CHECK_INTERVAL_DEFAULT = 15*1000;
  private long checkInterval; 
  
  private static final String WARN_THRESHOLD_KEY =
    "mapred.tasktracker.jetty.cpu.threshold.warn";
  private static final float WARN_THRESHOLD_DEFAULT = 0.50f;
  private float warnThreshold;

  private static final String FATAL_THRESHOLD_KEY =
    "mapred.tasktracker.jetty.cpu.threshold.fatal";
  private static final float FATAL_THRESHOLD_DEFAULT = 0.90f;
  private float fatalThreshold;
  
  private boolean stopping = false;

  /**
   * Create the monitoring thread.
   * @return null if thread CPU monitoring is not supported
   */
  public static JettyBugMonitor create(Configuration conf) {
    if (!conf.getBoolean(CHECK_ENABLED_KEY, CHECK_ENABLED_DEFAULT))  {
      return null;
    }
    
    if (!threadBean.isThreadCpuTimeSupported()) {
      LOG.warn("Not starting monitor for Jetty bug since thread CPU time " +
          "measurement is not supported by this JVM");
      return null;
    }
    return new JettyBugMonitor(conf);
  }
  
  JettyBugMonitor(Configuration conf) {
    setName("Monitor for Jetty bugs");
    setDaemon(true);
    
    this.warnThreshold = conf.getFloat(
        WARN_THRESHOLD_KEY, WARN_THRESHOLD_DEFAULT);
    this.fatalThreshold = conf.getFloat(
        FATAL_THRESHOLD_KEY, FATAL_THRESHOLD_DEFAULT);
    this.checkInterval = conf.getLong(
        CHECK_INTERVAL_KEY, CHECK_INTERVAL_DEFAULT);
  }
  
  @Override
  public void run() {
    try {
      doRun();
    } catch (InterruptedException ie) {
      if (!stopping) {
        LOG.warn("Jetty monitor unexpectedly interrupted", ie);
      }
    } catch (Throwable t) {
      LOG.error("Jetty bug monitor failed", t);
    }
    LOG.debug("JettyBugMonitor shutting down");
  }
  
  private void doRun() throws InterruptedException {
    List<Long> tids = waitForJettyThreads();
    if (tids.isEmpty()) {
      LOG.warn("Could not locate Jetty selector threads");
      return;
    }
    while (true) {
      try {
        monitorThreads(tids);
      } catch (ThreadNotRunningException tnre) {
        return;
      }
    }
  }
  
  /**
   * Monitor the given list of threads, summing their CPU usage.
   * If the usage exceeds the configured threshold, aborts the JVM.
   * @param tids thread ids to monitor
   * @throws InterruptedException if interrupted
   * @throws ThreadNotRunningException if one of the threads is no longer
   *         running
   */
  private void monitorThreads(List<Long> tids)
      throws InterruptedException, ThreadNotRunningException {
    
    long timeBefore = System.nanoTime();
    long usageBefore = getCpuUsageNanos(tids);
    while (true) {
      Thread.sleep(checkInterval);
      long usageAfter = getCpuUsageNanos(tids);
      long timeAfter = System.nanoTime();

      long delta = usageAfter - usageBefore;
      double percentCpu = (double)delta / (timeAfter - timeBefore);
      
      String msg = String.format("Jetty CPU usage: %.1f%%", percentCpu * 100);
      if (percentCpu > fatalThreshold) {
        LOG.fatal(
            "************************************************************\n" +
            msg + ". This is greater than the fatal threshold " +
            FATAL_THRESHOLD_KEY + ". Aborting JVM.\n" +
            "************************************************************");
        doAbort();
      } else if (percentCpu > warnThreshold) {
        LOG.warn(msg);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(msg);
      }

      usageBefore = usageAfter;
      timeBefore = timeAfter;
    }
  }
  
  protected void doAbort() {
    Runtime.getRuntime().exit(1);
  }

  /**
   * Wait for jetty selector threads to start.
   * @return the list of thread IDs
   * @throws InterruptedException if interrupted
   */
  protected List<Long> waitForJettyThreads() throws InterruptedException {
    List<Long> tids = new ArrayList<Long>();
    int i = 0;
    while (tids.isEmpty() & i++ < 30) {
      Thread.sleep(1000);
      tids = getJettyThreadIds();
    }
    return tids;
  }

  private static long getCpuUsageNanos(List<Long> tids)
      throws ThreadNotRunningException {
    long total = 0;
    for (long tid : tids) {
      long time = threadBean.getThreadCpuTime(tid);
      if (time == -1) {
        LOG.warn("Unable to monitor CPU usage for thread: " + tid);
        throw new ThreadNotRunningException();
      }
      total += time;
    }
    return total;
  }

  static List<Long> getJettyThreadIds() {
    List<Long> tids = new ArrayList<Long>();
    long[] threadIds = threadBean.getAllThreadIds();
    for (long tid : threadIds) {
      if (isJettySelectorThread(tid)) {
        tids.add(tid);
      }
    }
    return tids;
  }

  /**
   * @return true if the given thread ID appears to be a Jetty selector thread
   * based on its stack trace
   */
  private static boolean isJettySelectorThread(long tid) {
    ThreadInfo info = threadBean.getThreadInfo(tid, 20);
    for (StackTraceElement stack : info.getStackTrace()) {
      // compare class names instead of classses, since
      // jetty uses a different classloader
      if (SelectChannelConnector.class.getName().equals(
          stack.getClassName())) {
        LOG.debug("Thread #" + tid + " (" + info.getThreadName() + ") " +
            "is a Jetty selector thread.");
        return true;
      }
    }
    LOG.debug("Thread #" + tid + " (" + info.getThreadName() + ") " +
      "is not a jetty thread");
    return false;
  }
  
  private static class ThreadNotRunningException extends Exception {
    private static final long serialVersionUID = 1L;
  }

  public void shutdown() {
    this.stopping  = true;
    this.interrupt();
  }
}
