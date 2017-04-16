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
package org.apache.hadoop.tools.rumen;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;

/**
 * {@link JobStory} represents the runtime information available for a
 * completed Map-Reduce job.
 */
public interface JobStory {
  
  /**
   * Get the {@link JobConf} for the job.
   * @return the <code>JobConf</code> for the job
   */
  public JobConf getJobConf();
  
  /**
   * Get the job name.
   * @return the job name
   */
  public String getName();
  
  /**
   * Get the job ID
   * @return the job ID
   */
  public JobID getJobID();
  
  /**
   * Get the user who ran the job.
   * @return the user who ran the job
   */
  public String getUser();
  
  /**
   * Get the job submission time.
   * @return the job submission time
   */
  public long getSubmissionTime();
  
  /**
   * Get the number of maps in the {@link JobStory}.
   * @return the number of maps in the <code>Job</code>
   */
  public int getNumberMaps();
  
  /**
   * Get the number of reduce in the {@link JobStory}.
   * @return the number of reduces in the <code>Job</code>
   */
  public int getNumberReduces();

  /**
   * Get the input splits for the job.
   * @return the input splits for the job
   */
  public InputSplit[] getInputSplits();
  
  /**
   * Get {@link TaskInfo} for a given task.
   * @param taskType {@link TaskType} of the task
   * @param taskNumber Partition number of the task
   * @return the <code>TaskInfo</code> for the given task
   */
  public TaskInfo getTaskInfo(TaskType taskType, int taskNumber);
  
  /**
   * Get {@link TaskAttemptInfo} for a given task-attempt, without regard to
   * impact of locality (e.g. not needed to make scheduling decisions).
   * @param taskType {@link TaskType} of the task-attempt
   * @param taskNumber Partition number of the task-attempt
   * @param taskAttemptNumber Attempt number of the task
   * @return the <code>TaskAttemptInfo</code> for the given task-attempt
   */
  public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType, 
                                            int taskNumber, 
                                            int taskAttemptNumber);
  
  /**
   * Get {@link TaskAttemptInfo} for a given task-attempt, considering impact
   * of locality.
   * @param taskNumber Partition number of the task-attempt
   * @param taskAttemptNumber Attempt number of the task
   * @param locality Data locality of the task as scheduled in simulation
   * @return the <code>TaskAttemptInfo</code> for the given task-attempt
   */
  public TaskAttemptInfo
    getMapTaskAttemptInfoAdjusted(int taskNumber,
                                  int taskAttemptNumber,
                                  int locality);
  
  /**
   * Get the outcome of the job execution.
   * @return The outcome of the job execution.
   */
  public Values getOutcome();
  
  /**
   * Get the queue where the job is submitted.
   * @return the queue where the job is submitted.
   */
  public String getQueueName();
}
