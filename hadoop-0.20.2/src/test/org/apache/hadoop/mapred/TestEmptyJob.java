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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.InetAddress;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * A JUnit test to test Map-Reduce empty jobs.
 */
public class TestEmptyJob extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestEmptyJob.class.getName());

  private static String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).toURI()
          .toString().replace(' ', '+');

  MiniMRCluster mr = null;

  /** Committer with cleanup waiting on a signal
   */
  static class CommitterWithDelayCleanup extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      Configuration conf = context.getConfiguration();
      Path share = new Path(conf.get("share"));
      FileSystem fs = FileSystem.get(conf);

      
      while (true) {
        if (fs.exists(share)) {
          break;
        }
        UtilsForTests.waitFor(100);
      }
      super.commitJob(context);
    }
  }

  /**
   * Simple method running a MapReduce job with no input data. Used to test that
   * such a job is successful.
   * 
   * @param fileSys
   * @param numMaps
   * @param numReduces
   * @return true if the MR job is successful, otherwise false
   * @throws IOException
   */
  private boolean launchEmptyJob(URI fileSys, int numMaps, int numReduces)
      throws IOException {
    // create an empty input dir
    final Path inDir = new Path(TEST_ROOT_DIR, "testing/empty/input");
    final Path outDir = new Path(TEST_ROOT_DIR, "testing/empty/output");
    final Path inDir2 = new Path(TEST_ROOT_DIR, "testing/dummy/input");
    final Path outDir2 = new Path(TEST_ROOT_DIR, "testing/dummy/output");
    final Path share = new Path(TEST_ROOT_DIR, "share");

    JobConf conf = mr.createJobConf();
    FileSystem fs = FileSystem.get(fileSys, conf);
    fs.delete(new Path(TEST_ROOT_DIR), true);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      LOG.warn("Can't create " + inDir);
      return false;
    }

    // use WordCount example
    FileSystem.setDefaultUri(conf, fileSys);
    conf.setJobName("empty");
    // use an InputFormat which returns no split
    conf.setInputFormat(EmptyInputFormat.class);
    conf.setOutputCommitter(CommitterWithDelayCleanup.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    conf.set("share", share.toString());

    // run job and wait for completion
    JobClient jc = new JobClient(conf);
    RunningJob runningJob = jc.submitJob(conf);
    JobInProgress job = mr.getJobTrackerRunner().getJobTracker().getJob(runningJob.getID());

    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      assertTrue(job.getJobSubmitHostAddress().equalsIgnoreCase(
          ip.getHostAddress()));
      assertTrue(job.getJobSubmitHostName().equalsIgnoreCase(ip.getHostName()));
    }

    while (true) {
      if (job.isCleanupLaunched()) {
        LOG.info("Waiting for cleanup to be launched for job " 
                 + runningJob.getID());
        break;
      }
      UtilsForTests.waitFor(100);
    }
    
    // submit another job so that the map load increases and scheduling happens
    LOG.info("Launching dummy job ");
    RunningJob dJob = null;
    try {
      JobConf dConf = new JobConf(conf);
      dConf.setOutputCommitter(FileOutputCommitter.class);
      dJob = UtilsForTests.runJob(dConf, inDir2, outDir2, 2, 0);
    } catch (Exception e) {
      LOG.info("Exception ", e);
      throw new IOException(e);
    }
    
    while (true) {
      LOG.info("Waiting for job " + dJob.getID() + " to complete");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      if (dJob.isComplete()) {
        break;
      }
    }
    
    // check if the second job is successful
    assertTrue(dJob.isSuccessful());

    // signal the cleanup
    fs.create(share).close();
    
    while (true) {
      LOG.info("Waiting for job " + runningJob.getID() + " to complete");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      if (runningJob.isComplete()) {
        break;
      }
    }

    assertTrue(runningJob.isComplete());
    assertTrue(runningJob.isSuccessful());
    JobID jobID = runningJob.getID();

    TaskReport[] jobSetupTasks = jc.getSetupTaskReports(jobID);
    assertTrue("Number of job-setup tips is not 2!", jobSetupTasks.length == 2);
    assertTrue("Setup progress is " + runningJob.setupProgress()
        + " and not 1.0", runningJob.setupProgress() == 1.0);
    assertTrue("Setup task is not finished!", mr.getJobTrackerRunner()
        .getJobTracker().getJob(jobID).isSetupFinished());

    assertTrue("Number of maps is not zero!", jc.getMapTaskReports(runningJob
        .getID()).length == 0);
    assertTrue(
        "Map progress is " + runningJob.mapProgress() + " and not 1.0!",
        runningJob.mapProgress() == 1.0);

    assertTrue("Reduce progress is " + runningJob.reduceProgress()
        + " and not 1.0!", runningJob.reduceProgress() == 1.0);
    assertTrue("Number of reduces is not " + numReduces, jc
        .getReduceTaskReports(runningJob.getID()).length == numReduces);

    TaskReport[] jobCleanupTasks = jc.getCleanupTaskReports(jobID);
    assertTrue("Number of job-cleanup tips is not 2!",
        jobCleanupTasks.length == 2);
    assertTrue("Cleanup progress is " + runningJob.cleanupProgress()
        + " and not 1.0", runningJob.cleanupProgress() == 1.0);

    assertTrue("Job output directory doesn't exit!", fs.exists(outDir));
    FileStatus[] list = fs.listStatus(outDir, 
        new Utils.OutputFileUtils.OutputFilesFilter());
    assertTrue("Number of part-files is " + list.length + " and not "
        + numReduces, list.length == numReduces);

    // cleanup
    fs.delete(outDir, true);

    // return job result
    LOG.info("job is complete: " + runningJob.isSuccessful());
    return (runningJob.isSuccessful());
  }

  /**
   * Test that a job with no input data (and thus with no input split and no map
   * task to execute) is successful.
   * 
   * @throws IOException
   */
  public void testEmptyJob()
      throws IOException {
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 2;
      JobConf conf = new JobConf();
      fileSys = FileSystem.get(conf);

      conf.set("mapred.job.tracker.handler.count", "1");
      conf.set("mapred.job.tracker", "127.0.0.1:0");
      conf.set("mapred.job.tracker.http.address", "127.0.0.1:0");
      conf.set("mapred.task.tracker.http.address", "127.0.0.1:0");

      mr =
          new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1,
              null, null, conf);

      assertTrue(launchEmptyJob(fileSys.getUri(), 3, 1));
      assertTrue(launchEmptyJob(fileSys.getUri(), 0, 0));
    } finally {
      if (fileSys != null) {
        fileSys.close();
      }
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}
