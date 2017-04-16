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
import org.apache.hadoop.mapred.ControlledMapReduceJob.ControlledMapReduceJobRunner;
import org.junit.*;

/**UNTIL MAPREDUCE-873 is backported, we will not run recovery manager tests
 */
@Ignore
public class TestJobTrackerRestartWithCS extends ClusterWithCapacityScheduler {

  /**
   * Test single queue.
   *
   * <p>
   *
   * Submit a job with more M/R tasks than total capacity. Full queue capacity
   * should be utilized and remaining M/R tasks should wait for slots to be
   * available.
   *
   * @throws Exception
   */
  public void testJobTrackerRestartWithCS()
          throws Exception {
    try {
      Properties schedulerProps = new Properties();
      schedulerProps.put(
              "mapred.capacity-scheduler.queue.default.guaranteed-capacity", "100");
      Properties clusterProps = new Properties();
      clusterProps.put("mapred.tasktracker.map.tasks.maximum", String.valueOf(2));
      clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String.valueOf(0));

      // cluster capacity 2 maps, 0 reduces
      startCluster(1, clusterProps, schedulerProps);

      ControlledMapReduceJobRunner jobRunner =
              ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
              getJobConf(), 4, 0);
      jobRunner.start();
      ControlledMapReduceJob controlledJob = jobRunner.getJob();
      JobID myJobID = jobRunner.getJobID();
      JobInProgress myJob = getJobTracker().getJob(myJobID);
      ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 2);

      LOG.info("Trying to finish 2 maps");
      controlledJob.finishNTasks(true, 2);
      ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 2);
      assertTrue("Number of maps finished", myJob.finishedMaps() == 2);

      JobClient jobClient = new JobClient(getMrCluster().createJobConf());
      getMrCluster().stopJobTracker();

      getMrCluster().getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover",
              true);
      getMrCluster().startJobTracker();

      UtilsForTests.waitForJobTracker(jobClient);
      ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 1);

      controlledJob.finishNTasks(true, 2);
      ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 2);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tearDown();
    }
  }
}
