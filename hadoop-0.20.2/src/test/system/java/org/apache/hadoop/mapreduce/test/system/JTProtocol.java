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

package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapred.StatisticsCollectionHandler;

/**
 * Client side API's exposed from JobTracker.
 */
public interface JTProtocol extends DaemonProtocol {
  long versionID = 1L;

  /**
   * Get the information pertaining to given job.<br/>
   * The returned JobInfo object can be null when the
   * specified job by the job id is retired from the 
   * JobTracker memory which happens after job is 
   * completed. <br/>
   * 
   * @param id
   *          of the job for which information is required.
   * @return information of regarding job null if job is 
   *         retired from JobTracker memory.
   * @throws IOException
   */
  public JobInfo getJobInfo(JobID jobID) throws IOException;

  /**
   * Gets the information pertaining to a task. <br/>
   * The returned TaskInfo object can be null when the 
   * specified task specified by the task id is retired
   * from the JobTracker memory which happens after the
   * job is completed. <br/>
   * @param id
   *          of the task for which information is required.
   * @return information of regarding the task null if the 
   *          task is retired from JobTracker memory.
   * @throws IOException
   */
  public TaskInfo getTaskInfo(TaskID taskID) throws IOException;

  /**
   * Gets the information pertaining to a given TaskTracker. <br/>
   * The returned TTInfo class can be null if the given TaskTracker
   * information is removed from JobTracker memory which is done
   * when the TaskTracker is marked lost by the JobTracker. <br/>
   * @param name
   *          of the tracker.
   * @return information regarding the tracker null if the TaskTracker
   *          is marked lost by the JobTracker.
   * @throws IOException
   */
  public TTInfo getTTInfo(String trackerName) throws IOException;

  /**
   * Gets a list of all available jobs with JobTracker.<br/>
   * 
   * @return list of all jobs.
   * @throws IOException
   */
  public JobInfo[] getAllJobInfo() throws IOException;

  /**
   * Gets a list of tasks pertaining to a job. <br/>
   * 
   * @param id
   *          of the job.
   * 
   * @return list of all tasks for the job.
   * @throws IOException
   */
  public TaskInfo[] getTaskInfo(JobID jobID) throws IOException;

  /**
   * Gets a list of TaskTrackers which have reported to the JobTracker. <br/>
   * 
   * @return list of all TaskTracker.
   * @throws IOException
   */
  public TTInfo[] getAllTTInfo() throws IOException;

  /**
   * Checks if a given job is retired from the JobTrackers Memory. <br/>
   * 
   * @param id
   *          of the job
   * @return true if job is retired.
   * @throws IOException
   */
  boolean isJobRetired(JobID jobID) throws IOException;

  /**
   * Gets the location of the history file for a retired job. <br/>
   * 
   * @param id
   *          of the job
   * @return location of history file
   * @throws IOException
   */
  String getJobHistoryLocationForRetiredJob(JobID jobID) throws IOException;
  
  /**
   * This directly calls the JobTracker public with no modifications
   * @param trackerID uniquely indentifies the task tracker
   * @return
   * @throws IOException is thrown in case of RPC error
   */
  public boolean isBlackListed(String trackerID) throws IOException;

  /**
   * Get the job summary details from the jobtracker log files.
   * @param jobID - job id
   * @param filePattern - jobtracker log file pattern.
   * @return String - the job summary details
   * @throws IOException if any I/O error occurs.
   */
  public String getJobSummaryFromLog(JobID jobId, String filePattern)
    throws IOException;

  /**
   * Get the job summary information of given job id.
   * @param jobID - job id
   * @return String - the job summary details as map.
   * @throws IOException if any I/O error occurs.
   */
   public String getJobSummaryInfo(JobID jobId) throws IOException;


   /**
    * This gets the value of one task tracker window in the tasktracker page.
    *
    * @param TaskTrackerStatus,
    * timePeriod and totalTasksOrSucceededTasks, which are requried to
    * identify the window
    * @return value of one task in a single Job tracker window
    */
   public int getTaskTrackerLevelStatistics(TaskTrackerStatus ttStatus,
       String timePeriod, String totalTasksOrSucceededTasks)
       throws IOException;

   /**
    * This gets the value of all task trackers windows in the tasktracker page.
    *
    * @param none,
    * return a object which returns all the tasktracker info
    */
   public StatisticsCollectionHandler getInfoFromAllClientsForAllTaskType()
     throws Exception;

   /**
    * Get Information for Time Period and TaskType box
    * from all tasktrackers
    * @param
    * timePeriod and totalTasksOrSucceededTasks, which are requried to
    * identify the window
    * @return The total number of tasks info for a particular column in
    * tasktracker page.
    */
   public int getInfoFromAllClients(String timePeriod,
       String totalTasksOrSucceededTasks) throws IOException;

   /**
    * This gets the value of all task trackers windows in the tasktracker page.
    */
   public int getTaskTrackerHeartbeatInterval() throws Exception;
   

   
   /**
    * The method with access the history data in JobTracker, it will only do a 
    * read on the data structure none is returned, this is used to verify a
    * bug with simultaneously accessing history data
    * @param jobId
    * @throws Exception
    */
   public void accessHistoryData(JobID jobId) throws Exception;


  /**
   * Finds out if the given Task tracker client is decommissioned or not
   */
  public boolean isNodeDecommissioned(String ttClientHostName) 
      throws IOException;

}
