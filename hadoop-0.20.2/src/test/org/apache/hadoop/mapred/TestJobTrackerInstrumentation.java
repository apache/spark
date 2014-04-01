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

import org.apache.hadoop.examples.SleepJob;

@SuppressWarnings("deprecation")
public class TestJobTrackerInstrumentation extends TestCase {

  public void testSlots() throws IOException {
    MiniMRCluster mr = null;
    try {
      JobConf jtConf = new JobConf();
      jtConf.set("mapred.jobtracker.instrumentation", 
          MyJobTrackerMetricsInst.class.getName());
      mr = new MiniMRCluster(2, "file:///", 3, null, null, jtConf);
      MyJobTrackerMetricsInst instr = (MyJobTrackerMetricsInst) 
        mr.getJobTrackerRunner().getJobTracker().getInstrumentation();

      JobConf conf = mr.createJobConf();
      SleepJob job = new SleepJob();
      job.setConf(conf);
      int numMapTasks = 3;
      int numReduceTasks = 2;
      job.run(numMapTasks, numReduceTasks, 10000, 1, 10000, 1);
      
      synchronized (instr) {
        //after the job completes, incr and decr should be equal
        assertEquals(instr.incrOccupiedMapSlots, 
            instr.decrOccupiedMapSlots);
        assertEquals(instr.incrOccupiedReduceSlots, 
            instr.decrOccupiedReduceSlots);
        assertEquals(instr.incrRunningMaps,
            instr.decrRunningMaps);
        assertEquals(instr.incrRunningReduces,
            instr.decrRunningReduces);
        assertEquals(instr.incrReservedMapSlots,
            instr.decrReservedMapSlots);
        assertEquals(instr.incrReservedReduceSlots,
            instr.decrReservedReduceSlots);
        
        //validate that atleast once the callbacks happened
        assertTrue(instr.incrOccupiedMapSlots > 0);
        assertTrue(instr.incrOccupiedReduceSlots > 0);
        assertTrue(instr.incrRunningMaps > 0);
        assertTrue(instr.incrRunningReduces > 0);
      }
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  static class MyJobTrackerMetricsInst extends JobTrackerInstrumentation  {
    public MyJobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
      super(tracker, conf);
    }

    private int incrReservedMapSlots = 0;
    private int decrReservedMapSlots = 0;
    private int incrReservedReduceSlots = 0;
    private int decrReservedReduceSlots = 0;
    private int incrOccupiedMapSlots = 0;
    private int decrOccupiedMapSlots = 0;
    private int incrOccupiedReduceSlots = 0;
    private int decrOccupiedReduceSlots = 0;
    private int incrRunningMaps = 0;
    private int decrRunningMaps = 0;
    private int incrRunningReduces = 0;
    private int decrRunningReduces = 0;

    @Override
    public synchronized void addReservedMapSlots(int slots)
    { 
      incrReservedMapSlots += slots;
    }

    @Override
    public synchronized void decReservedMapSlots(int slots)
    {
      decrReservedMapSlots += slots;
    }

    @Override
    public synchronized void addReservedReduceSlots(int slots)
    {
      incrReservedReduceSlots += slots;
    }

    @Override
    public synchronized void decReservedReduceSlots(int slots)
    {
      decrReservedReduceSlots += slots;
    }

    @Override
    public synchronized void addOccupiedMapSlots(int slots)
    {
      incrOccupiedMapSlots += slots;
    }

    @Override
    public synchronized void decOccupiedMapSlots(int slots)
    {
      decrOccupiedMapSlots += slots;
    }

    @Override
    public synchronized void addOccupiedReduceSlots(int slots)
    {
      incrOccupiedReduceSlots += slots;
    }

    @Override
    public synchronized void decOccupiedReduceSlots(int slots)
    {
      decrOccupiedReduceSlots += slots;
    }
    
    @Override
    public synchronized void addRunningMaps(int task)
    {
      incrRunningMaps += task;
    }

    @Override
    public synchronized void decRunningMaps(int task) 
    {
      decrRunningMaps += task;
    }

    @Override
    public synchronized void addRunningReduces(int task)
    {
      incrRunningReduces += task;
    }

    @Override
    public synchronized void decRunningReduces(int task)
    {
      decrRunningReduces += task;
    }
  }
}
