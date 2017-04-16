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

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;

/**
 * Job state information as seen by the JobTracker.
 */
public interface JobInfo extends Writable {
  /**
   * Gets the JobId of the job.<br/>
   * 
   * @return id of the job.
   */
  JobID getID();

  /**
   * Gets the current status of the job.<br/>
   * 
   * @return status.
   */
  JobStatus getStatus();

  /**
   * Gets the history location of the job.<br/>
   * 
   * @return the path to the history file.
   */
  String getHistoryUrl();

  /**
   * Gets the number of maps which are currently running for the job. <br/>
   * 
   * @return number of running for the job.
   */
  int runningMaps();

  /**
   * Gets the number of reduces currently running for the job. <br/>
   * 
   * @return number of reduces running for the job.
   */
  int runningReduces();

  /**
   * Gets the number of maps to be scheduled for the job. <br/>
   * 
   * @return number of waiting maps.
   */
  int waitingMaps();

  /**
   * Gets the number of reduces to be scheduled for the job. <br/>
   * 
   * @return number of waiting reduces.
   */
  int waitingReduces();
  
  /**
   * Gets the number of maps that are finished. <br/>
   * @return the number of finished maps.
   */
  int finishedMaps();
  
  /**
   * Gets the number of map tasks that are to be spawned for the job <br/>
   * @return
   */
  int numMaps();
  
  /**
   * Gets the number of reduce tasks that are to be spawned for the job <br/>
   * @return
   */
  int numReduces();
  
  /**
   * Gets the number of reduces that are finished. <br/>
   * @return the number of finished reduces.
   */
  int finishedReduces();

  /**
   * Gets if cleanup for the job has been launched.<br/>
   * 
   * @return true if cleanup task has been launched.
   */
  boolean isCleanupLaunched();

  /**
   * Gets if the setup for the job has been launched.<br/>
   * 
   * @return true if setup task has been launched.
   */
  boolean isSetupLaunched();

  /**
   * Gets if the setup for the job has been completed.<br/>
   * 
   * @return true if the setup task for the job has completed.
   */
  boolean isSetupFinished();

  /**
   * Gets list of blacklisted trackers for the particular job. <br/>
   * 
   * @return list of blacklisted tracker name.
   */
  List<String> getBlackListedTrackers();
  
  /**
   * Get the launch time of a job.
   * @return long - launch time for a job.
   */
  long getLaunchTime();
  /**
   * Get the finish time of a job
   * @return long - finish time for a job
   */
  long getFinishTime();
  /**
   * Get the number of slots per map.
   * @return int - number of slots per map.
   */
  int getNumSlotsPerMap();
  /**
   * Get the number of slots per reduce.
   * @return int - number of slots per reduce.
   */
  int getNumSlotsPerReduce();
}
