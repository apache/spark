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

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.KerberosInfo;

/** 
 * Protocol that a TaskTracker and the central JobTracker use to communicate.
 * The JobTracker is the Server, which implements this protocol.
 */ 
@KerberosInfo(
    serverPrincipal = JobTracker.JT_USER_NAME,
    clientPrincipal = TaskTracker.TT_USER_NAME)
interface InterTrackerProtocol extends VersionedProtocol {
  /**
   * version 3 introduced to replace 
   * emitHearbeat/pollForNewTask/pollForTaskWithClosedJob with
   * {@link #heartbeat(TaskTrackerStatus, boolean, boolean, boolean, short)}
   * version 4 changed TaskReport for HADOOP-549.
   * version 5 introduced that removes locateMapOutputs and instead uses
   * getTaskCompletionEvents to figure finished maps and fetch the outputs
   * version 6 adds maxTasks to TaskTrackerStatus for HADOOP-1245
   * version 7 replaces maxTasks by maxMapTasks and maxReduceTasks in 
   * TaskTrackerStatus for HADOOP-1274
   * Version 8: HeartbeatResponse is added with the next heartbeat interval.
   * version 9 changes the counter representation for HADOOP-2248
   * version 10 changes the TaskStatus representation for HADOOP-2208
   * version 11 changes string to JobID in getTaskCompletionEvents().
   * version 12 changes the counters representation for HADOOP-1915
   * version 13 added call getBuildVersion() for HADOOP-236
   * Version 14: replaced getFilesystemName with getSystemDir for HADOOP-3135
   * Version 15: Changed format of Task and TaskStatus for HADOOP-153
   * Version 16: adds ResourceStatus to TaskTrackerStatus for HADOOP-3759
   * Version 17: Changed format of Task and TaskStatus for HADOOP-3150
   * Version 18: Changed status message due to changes in TaskStatus
   * Version 19: Changed heartbeat to piggyback JobTracker restart information
                 so that the TaskTracker can synchronize itself.
   * Version 20: Changed status message due to changes in TaskStatus
   *             (HADOOP-4232)
   * Version 21: Changed information reported in TaskTrackerStatus'
   *             ResourceStatus and the corresponding accessor methods
   *             (HADOOP-4035)
   * Version 22: Replaced parameter 'initialContact' with 'restarted' 
   *             in heartbeat method (HADOOP-4305) 
   * Version 23: Added parameter 'initialContact' again in heartbeat method
   *            (HADOOP-4869) 
   * Version 24: Changed format of Task and TaskStatus for HADOOP-4759 
   * Version 25: JobIDs are passed in response to JobTracker restart 
   * Version 26: Added numRequiredSlots to TaskStatus for MAPREDUCE-516
   * Version 27: Adding node health status to TaskStatus for MAPREDUCE-211
   * Version 28: Adding user name to the serialized Task for use by TT.
   * Version 29: Adding available memory and CPU usage information on TT to
   *             TaskTrackerStatus for MAPREDUCE-1218
   * Version 30: Adding disk failure to TaskTrackerStatus for MAPREDUCE-3015
   * Version 31: Adding version methods for HADOOP-8209
   */
  public static final long versionID = 31L;
  
  public final static int TRACKERS_OK = 0;
  public final static int UNKNOWN_TASKTRACKER = 1;

  /**
   * Called regularly by the {@link TaskTracker} to update the status of its 
   * tasks within the job tracker. {@link JobTracker} responds with a 
   * {@link HeartbeatResponse} that directs the 
   * {@link TaskTracker} to undertake a series of 'actions' 
   * (see {@link org.apache.hadoop.mapred.TaskTrackerAction.ActionType}).  
   * 
   * {@link TaskTracker} must also indicate whether this is the first 
   * interaction (since state refresh) and acknowledge the last response
   * it recieved from the {@link JobTracker} 
   * 
   * @param status the status update
   * @param restarted <code>true</code> if the process has just started or 
   *                   restarted, <code>false</code> otherwise
   * @param initialContact <code>true</code> if this is first interaction since
   *                       'refresh', <code>false</code> otherwise.
   * @param acceptNewTasks <code>true</code> if the {@link TaskTracker} is
   *                       ready to accept new tasks to run.                 
   * @param responseId the last responseId successfully acted upon by the
   *                   {@link TaskTracker}.
   * @return a {@link org.apache.hadoop.mapred.HeartbeatResponse} with 
   *         fresh instructions.
   */
  HeartbeatResponse heartbeat(TaskTrackerStatus status, 
                              boolean restarted, 
                              boolean initialContact,
                              boolean acceptNewTasks,
                              short responseId)
    throws IOException;
  
  /**
   * The task tracker calls this once, to discern where it can find
   * files referred to by the JobTracker
   */
  public String getFilesystemName() throws IOException;

  /**
   * Report a problem to the job tracker.
   * @param taskTracker the name of the task tracker
   * @param errorClass the kind of error (eg. the class that was thrown)
   * @param errorMessage the human readable error message
   * @throws IOException if there was a problem in communication or on the
   *                     remote side
   */
  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) throws IOException;
  /**
   * Get task completion events for the jobid, starting from fromEventId. 
   * Returns empty aray if no events are available. 
   * @param jobid job id 
   * @param fromEventId event id to start from. 
   * @param maxEvents the max number of events we want to look at
   * @return array of task completion events. 
   * @throws IOException
   */
  TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid, int fromEventId
      , int maxEvents) throws IOException;

  /**
   * Grab the jobtracker system directory path where job-specific files are to be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public String getSystemDir();
  
  /**
   * Returns the VersionInfo build version of the JobTracker 
   */
  public String getBuildVersion() throws IOException;

  /**
   * Returns the VersionInfo version of the JobTracker
   */
  public String getVIVersion() throws IOException;
}
