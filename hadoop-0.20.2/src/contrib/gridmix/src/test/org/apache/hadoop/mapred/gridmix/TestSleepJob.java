/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class TestSleepJob {

  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  {
    ((Log4JLogger) LogFactory.getLog("org.apache.hadoop.mapred.gridmix"))
      .getLogger().setLevel(Level.DEBUG);
  }

  static GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.REPLAY;
  private static final int NJOBS = 2;
  private static final long GENDATA = 50; // in megabytes


  @BeforeClass
  public static void init() throws IOException {
    GridmixTestUtils.initCluster();
  }

  @AfterClass
  public static void shutDown() throws IOException {
    GridmixTestUtils.shutdownCluster();
  }

  static class TestMonitor extends JobMonitor {
    private final BlockingQueue<Job> retiredJobs;
    private final int expected;

    public TestMonitor(int expected, Statistics stats) {
      super(stats);
      this.expected = expected;
      retiredJobs = new LinkedBlockingQueue<Job>();
    }

    @Override
    protected void onSuccess(Job job) {
      System.out.println(" Job Sucess " + job);
      retiredJobs.add(job);
    }

    @Override
    protected void onFailure(Job job) {
      fail("Job failure: " + job);
    }

    public void verify(ArrayList<JobStory> submitted) throws Exception  {
      assertEquals("Bad job count", expected, retiredJobs.size());
    }
  }


  static class DebugGridmix extends Gridmix {

    private JobFactory factory;
    private TestMonitor monitor;

    @Override
    protected JobMonitor createJobMonitor(Statistics stats) {
      monitor = new TestMonitor(NJOBS + 1, stats);
      return monitor;
    }

    @Override
    protected JobFactory createJobFactory(
      JobSubmitter submitter, String traceIn, Path scratchDir,
      Configuration conf, CountDownLatch startFlag, UserResolver userResolver)
      throws IOException {
      factory = DebugJobFactory.getFactory(
        submitter, scratchDir, NJOBS, conf, startFlag, userResolver);
      return factory;
    }

    public void checkMonitor() throws Exception {
      monitor.verify(((DebugJobFactory.Debuggable) factory).getSubmitted());
    }
  }


  @Test
  public void testReplaySubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.REPLAY;
    System.out.println(" Replay started at " + System.currentTimeMillis());
    doSubmission();
    System.out.println(" Replay ended at " + System.currentTimeMillis());
  }

  @Test
  public void testRandomLocationSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.STRESS;
    System.out.println(" Random locations started at " + System.currentTimeMillis());
    doSubmission("-D"+JobCreator.SLEEPJOB_RANDOM_LOCATIONS+"=3");
    System.out.println(" Random locations ended at " + System.currentTimeMillis());
  }
  
  @Test
  public void testMapTasksOnlySubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.STRESS;
    System.out.println(" Map tasks only at " + System.currentTimeMillis());
    doSubmission("-D"+SleepJob.SLEEPJOB_MAPTASK_ONLY+"=true");
    System.out.println(" Map tasks only ended at " + System.currentTimeMillis());
  }

  @Test
  public void testLimitTaskSleepTimeSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.STRESS;
    System.out.println(" Limit sleep time only at " + System.currentTimeMillis());
    doSubmission("-D" + SleepJob.GRIDMIX_SLEEP_MAX_MAP_TIME + "=100", "-D"
        + SleepJob.GRIDMIX_SLEEP_MAX_REDUCE_TIME + "=200");
    System.out.println(" Limit sleep time ended at " + System.currentTimeMillis());
  }

  @Test
  public void testStressSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.STRESS;
    System.out.println(" Stress started at " + System.currentTimeMillis());
    doSubmission();
    System.out.println(" Stress ended at " + System.currentTimeMillis());
  }

  @Test
  public void testSerialSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.SERIAL;
    System.out.println("Serial started at " + System.currentTimeMillis());
    doSubmission();
    System.out.println("Serial ended at " + System.currentTimeMillis());
  }
  
  @Test
  public void testRandomLocation() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    // testRandomLocation(0, 10, ugi);
    testRandomLocation(1, 10, ugi);
    testRandomLocation(2, 10, ugi);
  }
  
  private void testRandomLocation(int locations, int njobs, UserGroupInformation ugi) throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(JobCreator.SLEEPJOB_RANDOM_LOCATIONS, locations);
    DebugJobProducer jobProducer = new DebugJobProducer(njobs, conf);
    JobConf jconf = GridmixTestUtils.mrCluster.createJobConf(new JobConf(conf));
    JobStory story;
    int seq=1;
    while ((story = jobProducer.getNextJob()) != null) {
      GridmixJob gridmixJob = JobCreator.SLEEPJOB.createGridmixJob(jconf, 0,
          story, new Path("ignored"), ugi, seq++);
      gridmixJob.buildSplits(null);
      List<InputSplit> splits = new SleepJob.SleepInputFormat()
          .getSplits(gridmixJob.getJob());
      for (InputSplit split : splits) {
        assertEquals(locations, split.getLocations().length);
      }
    }
  }
  
  @Test
  public void testMapTasksOnlySleepJobs()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(SleepJob.SLEEPJOB_MAPTASK_ONLY, true);
    DebugJobProducer jobProducer = new DebugJobProducer(5, conf);
    JobConf jconf = GridmixTestUtils.mrCluster.createJobConf(new JobConf(conf));
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    JobStory story;
    int seq = 1;
    while ((story = jobProducer.getNextJob()) != null) {
      GridmixJob gridmixJob = JobCreator.SLEEPJOB.createGridmixJob(jconf, 0,
          story, new Path("ignored"), ugi, seq++);
      gridmixJob.buildSplits(null);
      Job job = gridmixJob.call();
      assertEquals(0, job.getNumReduceTasks());
    }
  }

  private void doSubmission(String...optional) throws Exception {
    final Path in = new Path("foo").makeQualified(GridmixTestUtils.dfs);
    final Path out = GridmixTestUtils.DEST.makeQualified(GridmixTestUtils.dfs);
    final Path root = new Path("/user");
    Configuration conf = null;
    try {
      // required options
      final String[] required = {"-D" + FilePool.GRIDMIX_MIN_FILE + "=0",
        "-D" + Gridmix.GRIDMIX_OUT_DIR + "=" + out,
        "-D" + Gridmix.GRIDMIX_USR_RSV + "=" + EchoUserResolver.class.getName(),
        "-D" + JobCreator.GRIDMIX_JOB_TYPE + "=" + JobCreator.SLEEPJOB.name(),
        "-D" + SleepJob.GRIDMIX_SLEEP_INTERVAL +"=" +"10"
      };
      // mandatory arguments
      final String[] mandatory = {
          "-generate",String.valueOf(GENDATA) + "m", in.toString(), "-"
          // ignored by DebugGridmix
      };
      
      ArrayList<String> argv = new ArrayList<String>(required.length+optional.length+mandatory.length);
      for (String s : required) {
        argv.add(s);
      }
      for (String s : optional) {
        argv.add(s);
      }
      for (String s : mandatory) {
        argv.add(s);
      }

      DebugGridmix client = new DebugGridmix();
      conf = new Configuration();
      conf.setEnum(GridmixJobSubmissionPolicy.JOB_SUBMISSION_POLICY, policy);
      conf = GridmixTestUtils.mrCluster.createJobConf(new JobConf(conf));
//    GridmixTestUtils.createHomeAndStagingDirectory((JobConf)conf);
      // allow synthetic users to create home directories
      GridmixTestUtils.dfs.mkdirs(root, new FsPermission((short) 0777));
      GridmixTestUtils.dfs.setPermission(root, new FsPermission((short) 0777));
      String[] args = argv.toArray(new String[argv.size()]);
      System.out.println("Command line arguments:");
      for (int i=0; i<args.length; ++i) {
        System.out.printf("    [%d] %s\n", i, args[i]);
      }
      int res = ToolRunner.run(conf, client, args);
      assertEquals("Client exited with nonzero status", 0, res);
      client.checkMonitor();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      in.getFileSystem(conf).delete(in, true);
      out.getFileSystem(conf).delete(out, true);
      root.getFileSystem(conf).delete(root, true);
    }
  }

}
