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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.examples.SleepJob;
import org.junit.Test;

public class TestTaskTrackerInstrumentation {

  @Test
  public void testStartup() throws IOException {
    MiniMRCluster mr = null;
    try {
      JobConf jtConf = new JobConf();
      // Set a bad class.
      jtConf.set("mapred.tasktracker.instrumentation",
          "org.nowhere.FUBAR");
      mr = new MiniMRCluster(1, "file:///", 1, null, null, jtConf);
      // Assert that the TT fell back to default class.
      assertEquals(TaskTrackerMetricsInst.class,
          mr.getTaskTrackerRunner(0).getTaskTracker()
          .getTaskTrackerInstrumentation().getClass());
    } finally {
      mr.shutdown();
    }
  }

  @Test
  public void testSlots() throws IOException {
    MiniMRCluster mr = null;
    try {
      JobConf jtConf = new JobConf();
      jtConf.set("mapred.tasktracker.instrumentation",
          MyTaskTrackerMetricsInst.class.getName());
      mr = new MiniMRCluster(1, "file:///", 1, null, null, jtConf);
      MyTaskTrackerMetricsInst instr = (MyTaskTrackerMetricsInst)
        mr.getTaskTrackerRunner(0).getTaskTracker()
        .getTaskTrackerInstrumentation();

      JobConf conf = mr.createJobConf();
      SleepJob job = new SleepJob();
      job.setConf(conf);
      int numMapTasks = 3;
      int numReduceTasks = 2;
      job.run(numMapTasks, numReduceTasks, 1, 1, 1, 1);

      synchronized (instr) {
        // 5 regular tasks + 2 setup/cleanup tasks.
        assertEquals(7, instr.complete);
        assertEquals(7, instr.end);
        assertEquals(7, instr.launch);
      }
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  static class MyTaskTrackerMetricsInst extends TaskTrackerInstrumentation  {
    public int complete = 0;
    public int launch = 0;
    public int end = 0;

    public MyTaskTrackerMetricsInst(TaskTracker tracker) {
      super(tracker);
    }

    @Override
    public void completeTask(TaskAttemptID t) {
      this.complete++;
    }

    @Override
    public void reportTaskLaunch(TaskAttemptID t, File stdout, File stderr) {
      this.launch++;
    }

    @Override
    public void reportTaskEnd(TaskAttemptID t) {
      this.end++;
    }
  }
}
