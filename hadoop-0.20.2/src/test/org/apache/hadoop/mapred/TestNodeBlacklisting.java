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
import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * Test node blacklisting. This testcase tests
 *   - node blacklisting along with node refresh 
 */
public class TestNodeBlacklisting extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestNodeBlacklisting.class);
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data", "/tmp"), "node-bklisting");

  // Mapper that fails once for the first time
  static class FailOnceMapper extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {

    private boolean shouldFail = false;
    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {

      if (shouldFail) {
        throw new RuntimeException("failing map");
      }
    }
     
    @Override
    public void configure(JobConf conf) {
      TaskAttemptID id = TaskAttemptID.forName(conf.get("mapred.task.id"));
      shouldFail = id.getId() == 0 && id.getTaskID().getId() == 0; 
    }
  }
   
  /**
   * Check refreshNodes for decommissioning blacklisted nodes. 
   */
  public void testBlacklistedNodeDecommissioning() throws Exception {
    LOG.info("Testing blacklisted node decommissioning");
    MiniMRCluster mr = null;
    JobTracker jt = null;
     
    try {
      // start mini mr
      JobConf jtConf = new JobConf();
      jtConf.set("mapred.max.tracker.blacklists", "1");
      mr = new MiniMRCluster(0, 0, 2, "file:///", 1, null, null, null, jtConf);
      jt = mr.getJobTrackerRunner().getJobTracker();

      assertEquals("Trackers not up", 2, jt.taskTrackers().size());
      // validate the total tracker count
      assertEquals("Active tracker count mismatch", 
                   2, jt.getClusterStatus(false).getTaskTrackers());
      // validate blacklisted count
      assertEquals("Blacklisted tracker count mismatch", 
                   0, jt.getClusterStatus(false).getBlacklistedTrackers());
      
      // run a failing job to blacklist the tracker
      JobConf jConf = mr.createJobConf();
      jConf.set("mapred.max.tracker.failures", "1");
      jConf.setJobName("test-job-fail-once");
      jConf.setMapperClass(FailOnceMapper.class);
      jConf.setReducerClass(IdentityReducer.class);
      jConf.setNumMapTasks(1);
      jConf.setNumReduceTasks(0);

      RunningJob job = 
        UtilsForTests.runJob(jConf, new Path(TEST_DIR, "in"), 
                             new Path(TEST_DIR, "out"));
      job.waitForCompletion();

      // validate the total tracker count
      assertEquals("Active tracker count mismatch", 
                   1, jt.getClusterStatus(false).getTaskTrackers());
      // validate blacklisted count
      assertEquals("Blacklisted tracker count mismatch", 
                   1, jt.getClusterStatus(false).getBlacklistedTrackers());

      // find the blacklisted tracker
      String trackerName = null;
      for (TaskTrackerStatus status : jt.taskTrackers()) {
        if (jt.isBlacklisted(status.getTrackerName())) {
          trackerName = status.getTrackerName();
          break;
        }
      }
      // get the hostname
      String hostToDecommission = 
        JobInProgress.convertTrackerNameToHostName(trackerName);
      LOG.info("Decommissioning tracker " + hostToDecommission);

      // decommission the node
      HashSet<String> decom = new HashSet<String>(1);
      decom.add(hostToDecommission);
      jt.decommissionNodes(decom);

      // validate
      // check the cluster status and tracker size
      assertEquals("Tracker is not lost upon host decommissioning", 
                   1, jt.getClusterStatus(false).getTaskTrackers());
      assertEquals("Blacklisted tracker count incorrect in cluster status "
                   + "after decommissioning", 
                   0, jt.getClusterStatus(false).getBlacklistedTrackers());
      assertEquals("Tracker is not lost upon host decommissioning", 
                   1, jt.taskTrackers().size());
    } finally {
      if (mr != null) {
        mr.shutdown();
        mr = null;
        jt = null;
        FileUtil.fullyDelete(new File(TEST_DIR.toString()));
      }
    }
  }
}
