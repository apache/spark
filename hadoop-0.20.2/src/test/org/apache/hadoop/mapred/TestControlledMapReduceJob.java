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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ControlledMapReduceJob.ControlledMapReduceJobRunner;

/**
 * Test to verify the controlled behavior of a ControlledMapReduceJob.
 * 
 */
public class TestControlledMapReduceJob extends ClusterMapReduceTestCase {
  static final Log LOG = LogFactory.getLog(TestControlledMapReduceJob.class);

  /**
   * Starts a job with 5 maps and 5 reduces. Then controls the finishing of
   * tasks. Signals finishing tasks in batches and then verifies their
   * completion.
   * 
   * @throws Exception
   */
  public void testControlledMapReduceJob()
      throws Exception {

    Properties props = new Properties();
    props.setProperty("mapred.tasktracker.map.tasks.maximum", "2");
    props.setProperty("mapred.tasktracker.reduce.tasks.maximum", "2");
    startCluster(true, props);
    LOG.info("Started the cluster");

    ControlledMapReduceJobRunner jobRunner =
        ControlledMapReduceJobRunner
            .getControlledMapReduceJobRunner(createJobConf(), 7, 6);
    jobRunner.start();
    ControlledMapReduceJob controlledJob = jobRunner.getJob();
    JobInProgress jip =
        getMRCluster().getJobTrackerRunner().getJobTracker().getJob(
            jobRunner.getJobID());

    ControlledMapReduceJob.waitTillNTasksStartRunning(jip, true, 4);
    LOG.info("Finishing 3 maps");
    controlledJob.finishNTasks(true, 3);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip, true, 3);

    ControlledMapReduceJob.waitTillNTasksStartRunning(jip, true, 4);
    LOG.info("Finishing 4 more maps");
    controlledJob.finishNTasks(true, 4);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip, true, 7);

    ControlledMapReduceJob.waitTillNTasksStartRunning(jip, false, 4);
    LOG.info("Finishing 2 reduces");
    controlledJob.finishNTasks(false, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip, false, 2);

    ControlledMapReduceJob.waitTillNTasksStartRunning(jip, false, 4);
    LOG.info("Finishing 4 more reduces");
    controlledJob.finishNTasks(false, 4);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip, false, 6);

    jobRunner.join();
  }
}
