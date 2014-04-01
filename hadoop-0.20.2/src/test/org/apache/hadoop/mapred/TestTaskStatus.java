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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class TestTaskStatus extends TestCase {
  
  private static final Log LOG = LogFactory.getLog(TestTaskStatus.class);

  public void testMapTaskStatusStartAndFinishTimes() {
    checkTaskStatues(true);
  }

  public void testReduceTaskStatusStartAndFinishTimes() {
    checkTaskStatues(false);
  }

  /**
   * Private utility method which ensures uniform testing of newly created
   * TaskStatus object.
   * 
   * @param isMap
   *          true to test map task status, false for reduce.
   */
  private void checkTaskStatues(boolean isMap) {

    TaskStatus status = null;
    if (isMap) {
      status = new MapTaskStatus();
    } else {
      status = new ReduceTaskStatus();
    }
    long currentTime = System.currentTimeMillis();
    // first try to set the finish time before
    // start time is set.
    status.setFinishTime(currentTime);
    assertEquals("Finish time of the task status set without start time", 0,
        status.getFinishTime());
    // Now set the start time to right time.
    status.setStartTime(currentTime);
    assertEquals("Start time of the task status not set correctly.",
        currentTime, status.getStartTime());
    // try setting wrong start time to task status.
    long wrongTime = -1;
    status.setStartTime(wrongTime);
    assertEquals(
        "Start time of the task status is set to wrong negative value",
        currentTime, status.getStartTime());
    // finally try setting wrong finish time i.e. negative value.
    status.setFinishTime(wrongTime);
    assertEquals("Finish time of task status is set to wrong negative value",
        0, status.getFinishTime());
    status.setFinishTime(currentTime);
    assertEquals("Finish time of the task status not set correctly.",
        currentTime, status.getFinishTime());
    
    // test with null task-diagnostics
    TaskStatus ts = ((TaskStatus)status.clone());
    ts.setDiagnosticInfo(null);
    ts.setDiagnosticInfo("");
    ts.setStateString(null);
    ts.setStateString("");
    ((TaskStatus)status.clone()).statusUpdate(ts);
    
    // test with null state-string
    ((TaskStatus)status.clone()).statusUpdate(0, null, null);
    ((TaskStatus)status.clone()).statusUpdate(0, "", null);
    ((TaskStatus)status.clone()).statusUpdate(null, 0, "", null, 1);
  }
  
  /**
   * Test the {@link TaskStatus} against large sized task-diagnostic-info and 
   * state-string. Does the following
   *  - create Map/Reduce TaskStatus such that the task-diagnostic-info and 
   *    state-string are small strings and check their contents
   *  - append them with small string and check their contents
   *  - append them with large string and check their size
   *  - update the status using statusUpdate() calls and check the size/contents
   *  - create Map/Reduce TaskStatus with large string and check their size
   */
  @Test
  public void testTaskDiagnosticsAndStateString() {
    // check the default case
    String test = "hi";
    final int maxSize = 16;
    TaskStatus status = new TaskStatus(null, 0, 0, null, test, test, null, null, 
                                       null) {
      @Override
      protected int getMaxStringSize() {
        return maxSize;
      }

      @Override
      public void addFetchFailedMap(TaskAttemptID mapTaskId) {
      }

      @Override
      public boolean getIsMap() {
        return false;
      }
    };
    assertEquals("Small diagnostic info test failed", 
                 status.getDiagnosticInfo(), test);
    assertEquals("Small state string test failed", status.getStateString(), 
                 test);
    
    // now append some small string and check
    String newDInfo = test.concat(test);
    status.setDiagnosticInfo(test);
    status.setStateString(newDInfo);
    assertEquals("Small diagnostic info append failed", 
                 newDInfo, status.getDiagnosticInfo());
    assertEquals("Small state-string append failed", 
                 newDInfo, status.getStateString());
    
    // update the status with small state strings
    TaskStatus newStatus = (TaskStatus)status.clone();
    String newSInfo = "hi1";
    newStatus.setStateString(newSInfo);
    status.statusUpdate(newStatus);
    newDInfo = newDInfo.concat(newStatus.getDiagnosticInfo());
    
    assertEquals("Status-update on diagnostic-info failed", 
                 newDInfo, status.getDiagnosticInfo());
    assertEquals("Status-update on state-string failed", 
                 newSInfo, status.getStateString());
    
    newSInfo = "hi2";
    status.statusUpdate(0, newSInfo, null);
    assertEquals("Status-update on state-string failed", 
                 newSInfo, status.getStateString());
    
    newSInfo = "hi3";
    status.statusUpdate(null, 0, newSInfo, null, 0);
    assertEquals("Status-update on state-string failed", 
                 newSInfo, status.getStateString());
    
    
    // now append each with large string
    String large = "hihihihihihihihihihi"; // 20 chars
    status.setDiagnosticInfo(large);
    status.setStateString(large);
    assertEquals("Large diagnostic info append test failed", 
                 maxSize, status.getDiagnosticInfo().length());
    assertEquals("Large state-string append test failed",
                 maxSize, status.getStateString().length());
    
    // update a large status with large strings
    newStatus.setDiagnosticInfo(large + "0");
    newStatus.setStateString(large + "1");
    status.statusUpdate(newStatus);
    assertEquals("Status-update on diagnostic info failed",
                 maxSize, status.getDiagnosticInfo().length());
    assertEquals("Status-update on state-string failed", 
                 maxSize, status.getStateString().length());
    
    status.statusUpdate(0, large + "2", null);
    assertEquals("Status-update on state-string failed", 
                 maxSize, status.getStateString().length());
    
    status.statusUpdate(null, 0, large + "3", null, 0);
    assertEquals("Status-update on state-string failed", 
                 maxSize, status.getStateString().length());
    
    // test passing large string in constructor
    status = new TaskStatus(null, 0, 0, null, large, large, null, null, 
        null) {
      @Override
      protected int getMaxStringSize() {
        return maxSize;
      }

      @Override
      public void addFetchFailedMap(TaskAttemptID mapTaskId) {
      }

      @Override
      public boolean getIsMap() {
        return false;
      }
    };
    assertEquals("Large diagnostic info test failed", 
                maxSize, status.getDiagnosticInfo().length());
    assertEquals("Large state-string test failed", 
                 maxSize, status.getStateString().length());
  }
}
