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
package org.apache.hadoop.mapreduce.server.tasktracker.userlogs;

import org.apache.hadoop.mapreduce.JobID;

/**
 * This is an {@link UserLogEvent} sent when the job completes
 */
public class JobCompletedEvent extends UserLogEvent {
  private JobID jobid;
  private long jobCompletionTime;
  private int retainHours;

  /**
   * Create the event for job completion.
   * 
   * @param jobid
   *          The completed {@link JobID} .
   * @param jobCompletionTime
   *          The job completion time.
   * @param retainHours
   *          The number of hours for which the job logs should be retained
   */
  public JobCompletedEvent(JobID jobid, long jobCompletionTime,
      int retainHours) {
    super(EventType.JOB_COMPLETED);
    this.jobid = jobid;
    this.jobCompletionTime = jobCompletionTime;
    this.retainHours = retainHours;
  }

  /**
   * Get the job id.
   * 
   * @return object of {@link JobID}
   */
  public JobID getJobID() {
    return jobid;
  }

  /**
   * Get the job completion time-stamp in milli-seconds.
   * 
   * @return job completion time.
   */
  public long getJobCompletionTime() {
    return jobCompletionTime;
  }

  /**
   * Get the number of hours for which job logs should be retained.
   * 
   * @return retainHours
   */
  public int getRetainHours() {
    return retainHours;
  }
}
