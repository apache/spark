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

import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapreduce.JobACL;

/**
 * Generic operation that maps to the dependent set of ACLs that drive the
 * authorization of the operation.
 */
public enum Operation {
  VIEW_JOB_COUNTERS(QueueACL.ADMINISTER_JOBS, JobACL.VIEW_JOB),
  VIEW_JOB_DETAILS(QueueACL.ADMINISTER_JOBS, JobACL.VIEW_JOB),
  VIEW_TASK_LOGS(QueueACL.ADMINISTER_JOBS, JobACL.VIEW_JOB),
  KILL_JOB(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  FAIL_TASK(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  KILL_TASK(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  SET_JOB_PRIORITY(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  SUBMIT_JOB(QueueACL.SUBMIT_JOB, null);
  
  public QueueACL qACLNeeded;
  public JobACL jobACLNeeded;
  
  Operation(QueueACL qACL, JobACL jobACL) {
    this.qACLNeeded = qACL;
    this.jobACLNeeded = jobACL;
  }
}