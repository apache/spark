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

package org.apache.hadoop.tools.rumen;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.TaskType;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestZombieJob {
  final double epsilon = 0.01;
  private final int[] attemptTimesPercentiles = new int[] { 10, 50, 90 };
  private long[] succeededCDF = new long[] { 5268, 5268, 5268, 5268, 5268 };
  private long[] failedCDF = new long[] { 18592, 18592, 18592, 18592, 18592 };
  private double[] expectedPs = new double[] { 0.000001, 0.18707660239708182,
      0.0013027618551328818, 2.605523710265763E-4 };

  private final long[] mapTaskCounts = new long[] { 7838525L, 342277L, 100228L,
      1564L, 1234L };
  private final long[] reduceTaskCounts = new long[] { 4405338L, 139391L,
      1514383L, 139391, 1234L };

  List<LoggedJob> loggedJobs = new ArrayList<LoggedJob>();
  List<JobStory> jobStories = new ArrayList<JobStory>();

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputDir = new Path(
        System.getProperty("test.tools.input.dir", "")).makeQualified(lfs);
    final Path rootInputFile = new Path(rootInputDir, "rumen/zombie");

    ZombieJobProducer parser = new ZombieJobProducer(new Path(rootInputFile,
        "input-trace.json"), new ZombieCluster(new Path(rootInputFile,
        "input-topology.json"), null, conf), conf);

    JobStory job = null;
    for (int i = 0; i < 4; i++) {
      job = parser.getNextJob();
      ZombieJob zJob = (ZombieJob) job;
      LoggedJob loggedJob = zJob.getLoggedJob();
      System.out.println(i + ":" + job.getNumberMaps() + "m, "
          + job.getNumberReduces() + "r");
      System.out
          .println(loggedJob.getOutcome() + ", " + loggedJob.getJobtype());

      System.out.println("Input Splits -- " + job.getInputSplits().length
          + ", " + job.getNumberMaps());

      System.out.println("Successful Map CDF -------");
      for (LoggedDiscreteCDF cdf : loggedJob.getSuccessfulMapAttemptCDFs()) {
        System.out.println(cdf.getNumberValues() + ": " + cdf.getMinimum()
            + "--" + cdf.getMaximum());
        for (LoggedSingleRelativeRanking ranking : cdf.getRankings()) {
          System.out.println("   " + ranking.getRelativeRanking() + ":"
              + ranking.getDatum());
        }
      }
      System.out.println("Failed Map CDF -----------");
      for (LoggedDiscreteCDF cdf : loggedJob.getFailedMapAttemptCDFs()) {
        System.out.println(cdf.getNumberValues() + ": " + cdf.getMinimum()
            + "--" + cdf.getMaximum());
        for (LoggedSingleRelativeRanking ranking : cdf.getRankings()) {
          System.out.println("   " + ranking.getRelativeRanking() + ":"
              + ranking.getDatum());
        }
      }
      System.out.println("Successful Reduce CDF ----");
      LoggedDiscreteCDF cdf = loggedJob.getSuccessfulReduceAttemptCDF();
      System.out.println(cdf.getNumberValues() + ": " + cdf.getMinimum() + "--"
          + cdf.getMaximum());
      for (LoggedSingleRelativeRanking ranking : cdf.getRankings()) {
        System.out.println("   " + ranking.getRelativeRanking() + ":"
            + ranking.getDatum());
      }
      System.out.println("Failed Reduce CDF --------");
      cdf = loggedJob.getFailedReduceAttemptCDF();
      System.out.println(cdf.getNumberValues() + ": " + cdf.getMinimum() + "--"
          + cdf.getMaximum());
      for (LoggedSingleRelativeRanking ranking : cdf.getRankings()) {
        System.out.println("   " + ranking.getRelativeRanking() + ":"
            + ranking.getDatum());
      }
      System.out.print("map attempts to success -- ");
      for (double p : loggedJob.getMapperTriesToSucceed()) {
        System.out.print(p + ", ");
      }
      System.out.println();
      System.out.println("===============");

      loggedJobs.add(loggedJob);
      jobStories.add(job);
    }
  }

  @Test
  public void testFirstJob() {
    // 20th job seems reasonable: "totalMaps":329,"totalReduces":101
    // successful map: 80 node-local, 196 rack-local, 53 rack-remote, 2 unknown
    // failed map: 0-0-0-1
    // successful reduce: 99 failed reduce: 13
    // map attempts to success -- 0.9969879518072289, 0.0030120481927710845,
    JobStory job = jobStories.get(0);
    assertEquals(1, job.getNumberMaps());
    assertEquals(1, job.getNumberReduces());

    // get splits

    TaskAttemptInfo taInfo = null;
    long expectedRuntime = 2423;
    // get a succeeded map task attempt, expect the exact same task attempt
    taInfo = job.getMapTaskAttemptInfoAdjusted(14, 0, 1);
    assertEquals(expectedRuntime, taInfo.getRuntime());
    assertEquals(State.SUCCEEDED, taInfo.getRunState());

    // get a succeeded map attempt, but reschedule with different locality.
    taInfo = job.getMapTaskAttemptInfoAdjusted(14, 0, 2);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());
    taInfo = job.getMapTaskAttemptInfoAdjusted(14, 0, 0);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());

    expectedRuntime = 97502;
    // get a succeeded reduce task attempt, expect the exact same task attempt
    taInfo = job.getTaskAttemptInfo(TaskType.REDUCE, 14, 0);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());

    // get a failed reduce task attempt, expect the exact same task attempt
    taInfo = job.getTaskAttemptInfo(TaskType.REDUCE, 14, 0);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());

    // get a non-exist reduce task attempt, expect a made-up task attempt
    // TODO fill in test case
  }

  @Test
  public void testSecondJob() {
    // 7th job has many failed tasks.
    // 3204 m, 0 r
    // successful maps 497-586-23-1, failed maps 0-0-0-2714
    // map attempts to success -- 0.8113600833767587, 0.18707660239708182,
    // 0.0013027618551328818, 2.605523710265763E-4,
    JobStory job = jobStories.get(1);
    assertEquals(20, job.getNumberMaps());
    assertEquals(1, job.getNumberReduces());

    TaskAttemptInfo taInfo = null;
    // get a succeeded map task attempt
    taInfo = job.getMapTaskAttemptInfoAdjusted(17, 1, 1);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());

    // get a succeeded map task attempt, with different locality
    taInfo = job.getMapTaskAttemptInfoAdjusted(17, 1, 2);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());
    taInfo = job.getMapTaskAttemptInfoAdjusted(17, 1, 0);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());

    // get a failed map task attempt
    taInfo = job.getMapTaskAttemptInfoAdjusted(14, 0, 1);
    assertEquals(1927, taInfo.getRuntime());
    assertEquals(State.SUCCEEDED, taInfo.getRunState());

    // get a failed map task attempt, with different locality
    // TODO: this test does not make sense here, because I don't have
    // available data set.
  }

  @Test
  public void testFourthJob() {
    // 7th job has many failed tasks.
    // 3204 m, 0 r
    // successful maps 497-586-23-1, failed maps 0-0-0-2714
    // map attempts to success -- 0.8113600833767587, 0.18707660239708182,
    // 0.0013027618551328818, 2.605523710265763E-4,
    JobStory job = jobStories.get(3);
    assertEquals(131, job.getNumberMaps());
    assertEquals(47, job.getNumberReduces());

    TaskAttemptInfo taInfo = null;
    // get a succeeded map task attempt
    long runtime = 5268;
    taInfo = job.getMapTaskAttemptInfoAdjusted(113, 1, 1);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());
    assertEquals(runtime, taInfo.getRuntime());

    // get a succeeded map task attempt, with different locality
    taInfo = job.getMapTaskAttemptInfoAdjusted(113, 1, 2);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());
    assertEquals(runtime, taInfo.getRuntime() / 2);
    taInfo = job.getMapTaskAttemptInfoAdjusted(113, 1, 0);
    assertEquals(State.SUCCEEDED, taInfo.getRunState());
    assertEquals((long) (runtime / 1.5), taInfo.getRuntime());

    // get a failed map task attempt
    taInfo = job.getMapTaskAttemptInfoAdjusted(113, 0, 1);
    assertEquals(18592, taInfo.getRuntime());
    assertEquals(State.FAILED, taInfo.getRunState());
  }

  @Test
  public void testRecordIOInfo() {
    JobStory job = jobStories.get(3);

    TaskInfo mapTask = job.getTaskInfo(TaskType.MAP, 113);

    TaskInfo reduceTask = job.getTaskInfo(TaskType.REDUCE, 0);

    assertEquals(mapTaskCounts[0], mapTask.getInputBytes());
    assertEquals(mapTaskCounts[1], mapTask.getInputRecords());
    assertEquals(mapTaskCounts[2], mapTask.getOutputBytes());
    assertEquals(mapTaskCounts[3], mapTask.getOutputRecords());
    assertEquals(mapTaskCounts[4], mapTask.getTaskMemory());

    assertEquals(reduceTaskCounts[0], reduceTask.getInputBytes());
    assertEquals(reduceTaskCounts[1], reduceTask.getInputRecords());
    assertEquals(reduceTaskCounts[2], reduceTask.getOutputBytes());
    assertEquals(reduceTaskCounts[3], reduceTask.getOutputRecords());
    assertEquals(reduceTaskCounts[4], reduceTask.getTaskMemory());
  }

  @Test
  public void testMakeUpInfo() {
    // get many non-exist tasks
    // total 3204 map tasks, 3300 is a non-exist task.
    checkMakeUpTask(jobStories.get(3), 113, 1);
  }

  private void checkMakeUpTask(JobStory job, int taskNumber, int locality) {
    TaskAttemptInfo taInfo = null;

    Histogram sampleSucceeded = new Histogram();
    Histogram sampleFailed = new Histogram();
    List<Integer> sampleAttempts = new ArrayList<Integer>();
    for (int i = 0; i < 100000; i++) {
      int attemptId = 0;
      while (true) {
        taInfo = job.getMapTaskAttemptInfoAdjusted(taskNumber, attemptId, 1);
        if (taInfo.getRunState() == State.SUCCEEDED) {
          sampleSucceeded.enter(taInfo.getRuntime());
          break;
        }
        sampleFailed.enter(taInfo.getRuntime());
        attemptId++;
      }
      sampleAttempts.add(attemptId);
    }

    // check state distribution
    int[] countTries = new int[] { 0, 0, 0, 0 };
    for (int attempts : sampleAttempts) {
      assertTrue(attempts < 4);
      countTries[attempts]++;
    }
    /*
     * System.out.print("Generated map attempts to success -- "); for (int
     * count: countTries) { System.out.print((double)count/sampleAttempts.size()
     * + ", "); } System.out.println(); System.out.println("===============");
     */
    for (int i = 0; i < 4; i++) {
      int count = countTries[i];
      double p = (double) count / sampleAttempts.size();
      assertTrue(expectedPs[i] - p < epsilon);
    }

    // check succeeded attempts runtime distribution
    long[] expectedCDF = succeededCDF;
    LoggedDiscreteCDF cdf = new LoggedDiscreteCDF();
    cdf.setCDF(sampleSucceeded, attemptTimesPercentiles, 100);
    /*
     * System.out.println("generated succeeded map runtime distribution");
     * System.out.println(cdf.getNumberValues() + ": " + cdf.getMinimum() + "--"
     * + cdf.getMaximum()); for (LoggedSingleRelativeRanking ranking:
     * cdf.getRankings()) { System.out.println("   " +
     * ranking.getRelativeRanking() + ":" + ranking.getDatum()); }
     */
    assertRuntimeEqual(cdf.getMinimum(), expectedCDF[0]);
    assertRuntimeEqual(cdf.getMaximum(), expectedCDF[4]);
    for (int i = 0; i < 3; i++) {
      LoggedSingleRelativeRanking ranking = cdf.getRankings().get(i);
      assertRuntimeEqual(expectedCDF[i + 1], ranking.getDatum());
    }

    // check failed attempts runtime distribution
    expectedCDF = failedCDF;
    cdf = new LoggedDiscreteCDF();
    cdf.setCDF(sampleFailed, attemptTimesPercentiles, 100);

    System.out.println("generated failed map runtime distribution");
    System.out.println(cdf.getNumberValues() + ": " + cdf.getMinimum() + "--"
        + cdf.getMaximum());
    for (LoggedSingleRelativeRanking ranking : cdf.getRankings()) {
      System.out.println("   " + ranking.getRelativeRanking() + ":"
          + ranking.getDatum());
    }
    assertRuntimeEqual(cdf.getMinimum(), expectedCDF[0]);
    assertRuntimeEqual(cdf.getMaximum(), expectedCDF[4]);
    for (int i = 0; i < 3; i++) {
      LoggedSingleRelativeRanking ranking = cdf.getRankings().get(i);
      assertRuntimeEqual(expectedCDF[i + 1], ranking.getDatum());
    }
  }

  private void assertRuntimeEqual(long expected, long generated) {
    if (expected == 0) {
      assertTrue(generated > -1000 && generated < 1000);
    } else {
      long epsilon = Math.max(expected / 10, 5000);
      assertTrue(expected - generated > -epsilon);
      assertTrue(expected - generated < epsilon);
    }
  }

}
