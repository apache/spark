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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;

public class TestMapredHeartbeat extends TestCase {
  public void testJobDirCleanup() throws IOException {
    MiniMRCluster mr = null;
    try {
      // test the default heartbeat interval
      int taskTrackers = 2;
      JobConf conf = new JobConf();
      mr = new MiniMRCluster(taskTrackers, "file:///", 3, 
          null, null, conf);
      JobClient jc = new JobClient(mr.createJobConf());
      while(jc.getClusterStatus().getTaskTrackers() != taskTrackers) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(MRConstants.HEARTBEAT_INTERVAL_MIN_DEFAULT, 
        mr.getJobTrackerRunner().getJobTracker().getNextHeartbeatInterval());
      mr.shutdown(); 
      
      // test configured heartbeat interval
      taskTrackers = 5;
      conf.setInt(JobTracker.JT_HEARTBEATS_IN_SECOND, 1);
      mr = new MiniMRCluster(taskTrackers, "file:///", 3, 
          null, null, conf);
      jc = new JobClient(mr.createJobConf());
      while(jc.getClusterStatus().getTaskTrackers() != taskTrackers) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(taskTrackers * 1000, 
        mr.getJobTrackerRunner().getJobTracker().getNextHeartbeatInterval());
      mr.shutdown(); 
      
      // test configured heartbeat interval is capped with min value
      taskTrackers = 5;
      conf.setInt(JobTracker.JT_HEARTBEATS_IN_SECOND, 100);
      mr = new MiniMRCluster(taskTrackers, "file:///", 3, 
          null, null, conf);
      jc = new JobClient(mr.createJobConf());
      while(jc.getClusterStatus().getTaskTrackers() != taskTrackers) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(MRConstants.HEARTBEAT_INTERVAL_MIN_DEFAULT, 
        mr.getJobTrackerRunner().getJobTracker().getNextHeartbeatInterval());
    } finally {
      if (mr != null) { mr.shutdown(); }
    }
  }
  
  public void testOutOfBandHeartbeats() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 4, true, null);
      
      int taskTrackers = 1;
      JobConf jobConf = new JobConf();
      jobConf.setFloat(JobTracker.JT_HEARTBEATS_SCALING_FACTOR, 30.0f);
      jobConf.setBoolean(TaskTracker.TT_OUTOFBAND_HEARBEAT, true);
      mr = new MiniMRCluster(taskTrackers, 
                             dfs.getFileSystem().getUri().toString(), 3, 
                             null, null, jobConf);
      long start = System.currentTimeMillis();
      TestMiniMRDFSSort.runRandomWriter(mr.createJobConf(), new Path("rw"));
      long end = System.currentTimeMillis();
      
      final int expectedRuntimeSecs = 120;
      final int runTimeSecs = (int)((end-start) / 1000); 
      System.err.println("Runtime is " + runTimeSecs);
      assertEquals("Actual runtime " + runTimeSecs + "s not less than expected " +
                   "runtime of " + expectedRuntimeSecs + "s!", 
                   true, (runTimeSecs <= 120));
    } finally {
      if (mr != null) { mr.shutdown(); }
      if (dfs != null) { dfs.shutdown(); }
    }
  }

}


