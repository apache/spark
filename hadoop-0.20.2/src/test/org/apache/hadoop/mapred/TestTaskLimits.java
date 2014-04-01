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

import junit.framework.TestCase;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.examples.PiEstimator;
import org.apache.hadoop.fs.FileSystem;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

/**
 * A JUnit test to test configired task limits.
 */
public class TestTaskLimits extends TestCase {

  {     
    ((Log4JLogger)JobInProgress.LOG).getLogger().setLevel(Level.ALL);
  }     

  private static final Log LOG =
    LogFactory.getLog(TestMiniMRWithDFS.class.getName());
  
  static final int NUM_MAPS = 5;
  static final int NUM_SAMPLES = 100;
  
  public static class TestResult {
    public String output;
    public RunningJob job;
    TestResult(RunningJob job, String output) {
      this.job = job;
      this.output = output;
    }
  }
  
  static void runPI(MiniMRCluster mr, JobConf jobconf) throws IOException {
    LOG.info("runPI");
    double estimate = PiEstimator.estimate(NUM_MAPS, NUM_SAMPLES, jobconf).doubleValue();
    double error = Math.abs(Math.PI - estimate);
    System.out.println("PI estimation " + error);
  }

  /**
   * check with a reduce limit of 10 bytes for input to reduce.
   * This should fail since input to reduce estimate is greater
   * than that!
   * @return true on failing the job, false
   * @throws IOException
   */
  private boolean runReduceLimitCheck() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    boolean success = false;
    try {
      final int taskTrackers = 2;
   
      Configuration conf = new Configuration();
      conf.setInt("mapred.jobtracker.maxtasks.per.job", -1);
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      JobConf jconf = new JobConf(conf);
      mr = new MiniMRCluster(0, 0, taskTrackers, fileSys.getUri().toString(), 1,
                             null, null, null, jconf);
      
      JobConf jc = mr.createJobConf();
      jc.setLong("mapreduce.reduce.input.limit", 10L);
      try {
        runPI(mr, jc);
        success = false;
      } catch (IOException e) {
        success = true;
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
    return success;
  }
  
  /**
   * Run the pi test with a specifix value of 
   * mapred.jobtracker.maxtasks.per.job. Returns true if the job succeeded.
   */
  private boolean runOneTest(int maxTasks) throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    boolean success = false;
    try {
      final int taskTrackers = 2;
   
      Configuration conf = new Configuration();
      conf.setInt("mapred.jobtracker.maxtasks.per.job", maxTasks);
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      JobConf jconf = new JobConf(conf);
      mr = new MiniMRCluster(0, 0, taskTrackers, fileSys.getUri().toString(), 1,
                             null, null, null, jconf);
      
      JobConf jc = mr.createJobConf();
      try {
        runPI(mr, jc);
        success = true;
      } catch (IOException e) {
        success = false;
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
    return success;
  }

  public void testTaskLimits() throws IOException {

    System.out.println("Job 1 running with max set to 2");
    boolean status = runOneTest(2);
    assertTrue(status == false);
    System.out.println("Job 1 failed as expected.");

    // verify that checking this limit works well. The job
    // needs 5 mappers and we set the limit to 7.
    System.out.println("Job 2 running with max set to 7.");
    status = runOneTest(7);
    assertTrue(status == true);
    System.out.println("Job 2 succeeded as expected.");

    System.out.println("Job 3 running with max disabled.");
    status = runOneTest(-1);
    assertTrue(status == true);
    System.out.println("Job 3 succeeded as expected.");
    status = runReduceLimitCheck();
    assertTrue(status == true);
    System.out.println("Success: Reduce limit as expected");
  }
}
