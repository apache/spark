/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A stress test for the fair scheduler.
 * <p>
 * Unlike {@link TestFairSchedulerSystem}, this test is designed to be run
 * against a cluster. Here's an example
 * configuration for the cluster for the purposes of testing. The update and
 * preemption intervals are smaller than normal.
 * <p>
 * <pre>
 * &lt;property&gt;
 *   &lt;name&gt;mapred.jobtracker.taskScheduler&lt;/name&gt;
 *   &lt;value&gt;org.apache.hadoop.mapred.FairScheduler&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;mapred.fairscheduler.allocation.file&lt;/name&gt;
 *   &lt;value&gt;/path/to/allocations.xml&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;mapred.fairscheduler.preemption&lt;/name&gt;
 *   &lt;value&gt;true&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;mapred.fairscheduler.update.interval&lt;/name&gt;
 *   &lt;value&gt;10&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;mapred.fairscheduler.preemption.interval&lt;/name&gt;
 *   &lt;value&gt;10&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;mapred.fairscheduler.eventlog.enabled&lt;/name&gt;
 *   &lt;value&gt;true&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * Here's an <i>allocations.xml</i> file which sets the minimum share preemption
 * timeout to a very low value (1 second) to exercise the scheduler:
 * <p>
 * <pre>
 * &lt;?xml version="1.0"?&gt;
 * &lt;allocations&gt;
 *   &lt;pool name="pool0"&gt;
 *     &lt;minMaps&gt;1&lt;/minMaps&gt;
 *     &lt;minReduces&gt;1&lt;/minReduces&gt;
 *   &lt;/pool&gt;
 *   &lt;defaultMinSharePreemptionTimeout&gt;1&lt;/defaultMinSharePreemptionTimeout&gt;
 * &lt;/allocations&gt;
 * </pre>
 * <p>
 * The following system properties can be set to override the defaults:
 * <ul>
 * <li><code>test.fairscheduler.numThreads</code></li>
 * <li><code>test.fairscheduler.numJobs</code></li>
 * <li><code>test.fairscheduler.numPools</code></li>
 * <li><code>test.fairscheduler.sleepTime</code></li>
 * <li><code>test.fairscheduler.jobTrackerInfoPort</code></li>
 * </ul>
 */
public class FairSchedulerStressTest extends FairSchedulerSystemTestBase {
  private static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";

  static final int DEFAULT_NUM_THREADS = 10;
  static final int DEFAULT_NUM_JOBS = 20;
  static final int DEFAULT_NUM_POOLS = 2;
  static final int DEFAULT_SLEEP_TIME = 1000;
  static final int DEFAULT_JOB_TRACKER_INFO_PORT = 50030;

  static Random RAND = new Random();

  static JobConf conf;
  
  private static int numThreads;
  private static int numJobs;
  private static int numPools;
  private static int sleepTime;
  private static int jobTrackerInfoPort;
  
  @BeforeClass
  public static void setUp() throws Exception {
    
    String namenode = System
        .getProperty(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    if (namenode == null) {
      fail(String.format("System property %s must be specified.",
          CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    }
    String jobtracker = System.getProperty(MAPRED_JOB_TRACKER);
    if (jobtracker == null) {
      fail(String.format("System property %s must be specified.",
          MAPRED_JOB_TRACKER));
    }
    conf = new JobConf();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, namenode);
    conf.set(MAPRED_JOB_TRACKER, jobtracker);
    
    numThreads = getIntProperty("numThreads", DEFAULT_NUM_THREADS);
    numJobs = getIntProperty("numJobs", DEFAULT_NUM_JOBS);
    numPools = getIntProperty("numPools", DEFAULT_NUM_POOLS);
    sleepTime = getIntProperty("sleepTime", DEFAULT_SLEEP_TIME);
    jobTrackerInfoPort = getIntProperty("jobTrackerInfoPort",
        DEFAULT_JOB_TRACKER_INFO_PORT);
  }
  
  private static int getIntProperty(String suffix, int defaultValue) {
    String name = "test.fairscheduler." + suffix;
    if (System.getProperty(name) == null) {
      return defaultValue;
    }
    return Integer.parseInt(System.getProperty(name));
  }
  
  /**
   * Submit some concurrent sleep jobs, and visit the scheduler servlet
   * while they're running.
   */
  @Test
  public void testFairSchedulerSystem() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    List<Future<Void>> futures = new ArrayList<Future<Void>>(numJobs);
    for (int i = 0; i < numJobs; i++) {
      futures.add(exec.submit(new Callable<Void>() {
            public Void call() throws Exception {
              JobConf jobConf = new JobConf(conf);
              jobConf.set("mapred.fairscheduler.pool",
                  "pool" + RAND.nextInt(numPools));
              runSleepJob(jobConf, sleepTime);
              return null;
            }
          }));
    }

    JobClient jc = new JobClient(conf);

    // Wait for the tasks to finish, and visit the scheduler servlet
    // every few seconds while waiting.
    for (Future<Void> future : futures) {
      while (true) {
        try {
          future.get(3, TimeUnit.SECONDS);
          break;
        } catch (TimeoutException te) {
          // It's OK
        }
        checkServlet(true, false, jobTrackerInfoPort);
        checkServlet(false, false, jobTrackerInfoPort);
      }
    }
  }

}