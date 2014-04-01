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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskStatus.State;

import junit.framework.TestCase;

/**
 * Exercise the canAssignMap and canAssignReduce methods in 
 * CapBasedLoadManager.
 */
public class TestCapBasedLoadManager extends TestCase {
  
  /**
   * Returns a running MapTaskStatus.
   */
  private TaskStatus getRunningMapTaskStatus() {
    TaskStatus ts = new MapTaskStatus();
    ts.setRunState(State.RUNNING);
    return ts;
  }

  /**
   * Returns a running ReduceTaskStatus.
   */
  private TaskStatus getRunningReduceTaskStatus() {
    TaskStatus ts = new ReduceTaskStatus();
    ts.setRunState(State.RUNNING);
    return ts;
  }
  
  /**
   * Returns a TaskTrackerStatus with the specified statistics. 
   * @param mapCap        The capacity of map tasks 
   * @param reduceCap     The capacity of reduce tasks
   * @param runningMap    The number of running map tasks
   * @param runningReduce The number of running reduce tasks
   */
  private TaskTrackerStatus getTaskTrackerStatus(int mapCap, int reduceCap, 
      int runningMap, int runningReduce) {
    List<TaskStatus> ts = new ArrayList<TaskStatus>();
    for (int i = 0; i < runningMap; i++) {
      ts.add(getRunningMapTaskStatus());
    }
    for (int i = 0; i < runningReduce; i++) {
      ts.add(getRunningReduceTaskStatus());
    }
    TaskTrackerStatus tracker = new TaskTrackerStatus("tracker", 
      "tracker_host", 1234, ts, 0, 0, mapCap, reduceCap);
    return tracker;
  }

  /**
   * A single test of canAssignMap.
   */
  private void oneTestCanAssignMap(float maxDiff, int mapCap, int runningMap,
      int totalMapSlots, int totalRunnableMap, int expectedAssigned) {
    
    CapBasedLoadManager manager = new CapBasedLoadManager();
    Configuration conf = new Configuration();
    conf.setFloat("mapred.fairscheduler.load.max.diff", maxDiff);
    manager.setConf(conf);
    
    TaskTrackerStatus ts = getTaskTrackerStatus(mapCap, 1, runningMap, 1);
    
    int numAssigned = 0;
    while (manager.canAssignMap(ts, totalRunnableMap, totalMapSlots, numAssigned)) {
      numAssigned++;
    }
      
    assertEquals( "When maxDiff=" + maxDiff + ", with totalRunnableMap=" 
        + totalRunnableMap + " and totalMapSlots=" + totalMapSlots
        + ", a tracker with runningMap=" + runningMap + " and mapCap="
        + mapCap + " should be able to assign " + expectedAssigned + " maps",
        expectedAssigned, numAssigned);
  }
  
  
  /** 
   * Test canAssignMap method.
   */
  public void testCanAssignMap() {
    oneTestCanAssignMap(0.0f, 5, 0, 50, 1, 1);
    oneTestCanAssignMap(0.0f, 5, 1, 50, 10, 0);
    // 20% load + 20% diff = 40% of available slots, but rounds
    // up with floating point error: so we get 3/5 slots on TT.
    // 1 already taken, so assigns 2 more
    oneTestCanAssignMap(0.2f, 5, 1, 50, 10, 2);
    oneTestCanAssignMap(0.0f, 5, 1, 50, 11, 1);
    oneTestCanAssignMap(0.0f, 5, 2, 50, 11, 0);
    oneTestCanAssignMap(0.3f, 5, 2, 50, 6, 1);
    oneTestCanAssignMap(1.0f, 5, 5, 50, 50, 0);
  }
  
  
  /**
   * A single test of canAssignReduce.
   */
  private void oneTestCanAssignReduce(float maxDiff, int reduceCap,
      int runningReduce, int totalReduceSlots, int totalRunnableReduce,
      int expectedAssigned) {
    
    CapBasedLoadManager manager = new CapBasedLoadManager();
    Configuration conf = new Configuration();
    conf.setFloat("mapred.fairscheduler.load.max.diff", maxDiff);
    manager.setConf(conf);
    
    TaskTrackerStatus ts = getTaskTrackerStatus(1, reduceCap, 1,
        runningReduce);
    
    int numAssigned = 0;
    while (manager.canAssignReduce(ts, totalRunnableReduce, totalReduceSlots, numAssigned)) {
      numAssigned++;
    }
      
    assertEquals( "When maxDiff=" + maxDiff + ", with totalRunnableReduce=" 
        + totalRunnableReduce + " and totalReduceSlots=" + totalReduceSlots
        + ", a tracker with runningReduce=" + runningReduce + " and reduceCap="
        + reduceCap + " should be able to assign " + expectedAssigned + " reduces",
        expectedAssigned, numAssigned);
  }
    
  /** 
   * Test canAssignReduce method.
   */
  public void testCanAssignReduce() {
    oneTestCanAssignReduce(0.0f, 5, 0, 50, 1, 1);
    oneTestCanAssignReduce(0.0f, 5, 1, 50, 10, 0);
    // 20% load + 20% diff = 40% of available slots, but rounds
    // up with floating point error: so we get 3/5 slots on TT.
    // 1 already taken, so assigns 2 more
    oneTestCanAssignReduce(0.2f, 5, 1, 50, 10, 2);
    oneTestCanAssignReduce(0.0f, 5, 1, 50, 11, 1);
    oneTestCanAssignReduce(0.0f, 5, 2, 50, 11, 0);
    oneTestCanAssignReduce(0.3f, 5, 2, 50, 6, 1);
    oneTestCanAssignReduce(1.0f, 5, 5, 50, 50, 0);
  }
  
}
