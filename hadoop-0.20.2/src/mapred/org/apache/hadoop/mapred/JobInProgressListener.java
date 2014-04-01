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

/**
 * A listener for changes in a {@link JobInProgress job}'s lifecycle in the
 * {@link JobTracker}.
 */
abstract class JobInProgressListener {

  /**
   * Invoked when a new job has been added to the {@link JobTracker}.
   * @param job The added job.
   * @throws IOException 
   */
  public abstract void jobAdded(JobInProgress job) throws IOException;

  /**
   * Invoked when a job has been removed from the {@link JobTracker}.
   * @param job The removed job.
   */
  public abstract void jobRemoved(JobInProgress job);
  
  /**
   * Invoked when a job has been updated in the {@link JobTracker}.
   * This change in the job is tracker using {@link JobChangeEvent}.
   * @param event the event that tracks the change
   */
  public abstract void jobUpdated(JobChangeEvent event);
}
