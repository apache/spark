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

/**
 * {@link JobStatusChangeEvent} tracks the change in job's status. Job's 
 * status can change w.r.t 
 *   - run state i.e PREP, RUNNING, FAILED, KILLED, SUCCEEDED
 *   - start time
 *   - priority
 * Note that job times can change as the job can get restarted.
 */
class JobStatusChangeEvent extends JobChangeEvent {
  // Events in job status that can lead to a job-status change
  static enum EventType {RUN_STATE_CHANGED, START_TIME_CHANGED, PRIORITY_CHANGED}
  
  private JobStatus oldStatus;
  private JobStatus newStatus;
  private EventType eventType;
   
  JobStatusChangeEvent(JobInProgress jip, EventType eventType, 
                       JobStatus oldStatus, JobStatus newStatus) {
    super(jip);
    this.oldStatus = oldStatus;
    this.newStatus = newStatus;
    this.eventType = eventType;
  }
  
  /**
   * Create a {@link JobStatusChangeEvent} indicating the state has changed. 
   * Note that here we assume that the state change doesnt care about the old
   * state.
   */
  JobStatusChangeEvent(JobInProgress jip, EventType eventType, JobStatus status)
  {
    this(jip, eventType, status, status);
  }
  
  /**
   * Returns a event-type that caused the state change
   */
  EventType getEventType() {
    return eventType;
  }
  
  /**
   * Get the old job status
   */
  JobStatus getOldStatus() {
    return oldStatus;
  }
  
  /**
   * Get the new job status as a result of the events
   */
  JobStatus getNewStatus() {
    return newStatus;
  }
}