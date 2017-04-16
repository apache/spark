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
package org.apache.hadoop.mapreduce;

/**
 * Job related ACLs
 */
public enum JobACL {

  /**
   * ACL for 'viewing' job. Dictates who can 'view' some or all of the job
   * related details.
   */
  VIEW_JOB(JobContext.JOB_ACL_VIEW_JOB),

  /**
   * ACL for 'modifying' job. Dictates who can 'modify' the job for e.g., by
   * killing the job, killing/failing a task of the job or setting priority of
   * the job.
   */
  MODIFY_JOB(JobContext.JOB_ACL_MODIFY_JOB);

  String aclName;

  JobACL(String name) {
    this.aclName = name;
  }

  /**
   * Get the name of the ACL. Here it is same as the name of the configuration
   * property for specifying the ACL for the job.
   * 
   * @return aclName
   */
  public String getAclName() {
    return aclName;
  }
}
