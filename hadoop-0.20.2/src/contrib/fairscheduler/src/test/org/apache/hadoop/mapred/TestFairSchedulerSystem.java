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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.AssertionFailedError;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * System tests for the fair scheduler. These run slower than the
 * mock-based tests in TestFairScheduler but have a better chance
 * of catching synchronization bugs with the real JT.
 *
 * This test suite will often be run inside JCarder in order to catch
 * deadlock bugs which have plagued the scheduler in the past - hence
 * it is a bit of a "grab-bag" of system tests, since it's important
 * that they all run as part of the same JVM instantiation.
 */
public class TestFairSchedulerSystem extends FairSchedulerSystemTestBase {
  static final int NUM_THREADS=2;
  static final int SLEEP_TIME=1;

  static MiniMRCluster mr;
  static JobConf conf;
  
  @BeforeClass
  public static void setUp() throws Exception {
    conf = new JobConf();
    final int taskTrackers = 1;

    // Bump up the frequency of preemption updates to test against
    // deadlocks, etc.
    conf.set("mapred.jobtracker.taskScheduler", FairScheduler.class.getCanonicalName());
    conf.set("mapred.fairscheduler.update.interval", "0");
    conf.set("mapred.fairscheduler.preemption.interval", "0");
    conf.set("mapred.fairscheduler.preemption", "true");
    conf.set("mapred.fairscheduler.eventlog.enabled", "true");
    conf.set("mapred.fairscheduler.poolnameproperty", "group.name");
    mr = new MiniMRCluster(taskTrackers, "file:///", 1, null, null, conf);
  }
  
  protected int getJobTrackerInfoPort() {
    return mr.getJobTrackerRunner().getJobTrackerInfoPort();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (mr != null) {
      mr.shutdown();
    }
  }
  
  /**
   * Submit some concurrent sleep jobs, and visit the scheduler servlet
   * while they're running.
   */
  @Test
  public void testFairSchedulerSystem() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Void>> futures = new ArrayList<Future<Void>>(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(exec.submit(new Callable<Void>() {
            public Void call() throws Exception {
              JobConf jobConf = mr.createJobConf();
              runSleepJob(jobConf, SLEEP_TIME);
              return null;
            }
          }));
    }

    JobClient jc = new JobClient(mr.createJobConf(null));

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
        checkServlet(true, true, getJobTrackerInfoPort());
        checkServlet(false, true, getJobTrackerInfoPort());

        JobStatus jobs[] = jc.getAllJobs();
        if (jobs == null) {
          System.err.println("No jobs running, not checking tasklog servlet");
          continue;
        }
        for (JobStatus j : jobs) {
          System.err.println("Checking task graph for " + j.getJobID());
          checkTaskGraphServlet(j.getJobID(), getJobTrackerInfoPort());
        }
      }
    }
  }
  
  
}
