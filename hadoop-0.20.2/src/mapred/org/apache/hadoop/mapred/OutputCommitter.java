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
 * <code>OutputCommitter</code> describes the commit of task output for a 
 * Map-Reduce job.
 *
 * <p>The Map-Reduce framework relies on the <code>OutputCommitter</code> of 
 * the job to:<p>
 * <ol>
 *   <li>
 *   Setup the job during initialization. For example, create the temporary 
 *   output directory for the job during the initialization of the job.
 *   </li>
 *   <li>
 *   Cleanup the job after the job completion. For example, remove the
 *   temporary output directory after the job completion. 
 *   </li>
 *   <li>
 *   Setup the task temporary output.
 *   </li> 
 *   <li>
 *   Check whether a task needs a commit. This is to avoid the commit
 *   procedure if a task does not need commit.
 *   </li>
 *   <li>
 *   Commit of the task output.
 *   </li>  
 *   <li>
 *   Discard the task commit.
 *   </li>
 * </ol>
 * 
 * @see FileOutputCommitter 
 * @see JobContext
 * @see TaskAttemptContext 
 */
public abstract class OutputCommitter 
                extends org.apache.hadoop.mapreduce.OutputCommitter {
  /**
   * For the framework to setup the job output during initialization
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException if temporary output could not be created
   */
  public abstract void setupJob(JobContext jobContext) throws IOException;

  /**
   * For cleaning up the job's output after job completion
   * @deprecated use {@link #commitJob(JobContext)} or
   *                 {@link #abortJob(JobContext, int)} instead
   */
  @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException { }

  /**
   * For committing job's output after successful job completion. Note that this
   * is invoked for jobs with final run state as {@link JobStatus#SUCCEEDED}.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException 
   */
  public void commitJob(JobContext jobContext) throws IOException { 
    cleanupJob(jobContext);
  }

  /**
   * For cleaning up the job's output after job failure.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @param status Final run state of the job, should be 
   * {@link JobStatus#KILLED} or {@link JobStatus#FAILED}
   * @throws IOException
   */
  public void abortJob(JobContext jobContext, int status) 
  throws IOException {
    cleanupJob(jobContext);
  }
  
  /**
   * Sets up output for the task.
   * 
   * @param taskContext Context of the task whose output is being written.
   * @throws IOException
   */
  public abstract void setupTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * Check whether task needs a commit
   * 
   * @param taskContext
   * @return true/false
   * @throws IOException
   */
  public abstract boolean needsTaskCommit(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * To promote the task's temporary output to final output location
   * 
   * The task's output is moved to the job's output directory.
   * 
   * @param taskContext Context of the task whose output is being written.
   * @throws IOException if commit is not 
   */
  public abstract void commitTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * Discard the task output
   * 
   * @param taskContext
   * @throws IOException
   */
  public abstract void abortTask(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final void setupJob(org.apache.hadoop.mapreduce.JobContext jobContext
                             ) throws IOException {
    setupJob((JobContext) jobContext);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  @Deprecated
  public final void cleanupJob(org.apache.hadoop.mapreduce.JobContext context
                               ) throws IOException {
    cleanupJob((JobContext) context);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final void commitJob(org.apache.hadoop.mapreduce.JobContext context
                               ) throws IOException {
    commitJob((JobContext) context);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final void abortJob(org.apache.hadoop.mapreduce.JobContext context, 
                          org.apache.hadoop.mapreduce.JobStatus.State runState) 
  throws IOException {
    int state = JobStatus.getOldNewJobRunState(runState);
    if (state != JobStatus.FAILED && state != JobStatus.KILLED) {
      throw new IOException ("Invalid job run state : " + runState.name());
    }
    abortJob((JobContext) context, state);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final 
  void setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                 ) throws IOException {
    setupTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final boolean 
    needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                    ) throws IOException {
    return needsTaskCommit((TaskAttemptContext) taskContext);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final 
  void commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                  ) throws IOException {
    commitTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final 
  void abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                 ) throws IOException {
    abortTask((TaskAttemptContext) taskContext);
  }
}
