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
import org.apache.hadoop.mapred.JvmTask;
import org.apache.hadoop.mapreduce.security.token.JobTokenSelector;
import org.apache.hadoop.security.token.TokenInfo;

/** Protocol that task child process uses to contact its parent process.  The
 * parent is a daemon which which polls the central master for a new map or
 * reduce task and runs it as a child process.  All communication between child
 * and parent is via this protocol. */
@TokenInfo(JobTokenSelector.class)
public interface TaskUmbilicalProtocol extends VersionedProtocol {

  /** 
   * Changed the version to 2, since we have a new method getMapOutputs 
   * Changed version to 3 to have progress() return a boolean
   * Changed the version to 4, since we have replaced 
   *         TaskUmbilicalProtocol.progress(String, float, String, 
   *         org.apache.hadoop.mapred.TaskStatus.Phase, Counters) 
   *         with statusUpdate(String, TaskStatus)
   * 
   * Version 5 changed counters representation for HADOOP-2248
   * Version 6 changes the TaskStatus representation for HADOOP-2208
   * Version 7 changes the done api (via HADOOP-3140). It now expects whether
   *           or not the task's output needs to be promoted.
   * Version 8 changes {job|tip|task}id's to use their corresponding 
   * objects rather than strings.
   * Version 9 changes the counter representation for HADOOP-1915
   * Version 10 changed the TaskStatus format and added reportNextRecordRange
   *            for HADOOP-153
   * Version 11 Adds RPCs for task commit as part of HADOOP-3150
   * Version 12 getMapCompletionEvents() now also indicates if the events are 
   *            stale or not. Hence the return type is a class that 
   *            encapsulates the events and whether to reset events index.
   * Version 13 changed the getTask method signature for HADOOP-249
   * Version 14 changed the getTask method signature for HADOOP-4232
   * Version 15 Adds FAILED_UNCLEAN and KILLED_UNCLEAN states for HADOOP-4759
   * Version 16 Added fatalError for child to communicate fatal errors to TT
   *            Added numRequiredSlots to TaskStatus for MAPREDUCE-516
   * Version 17 Change in signature of getTask() for HADOOP-5488
   * Version 18 Added jvmContext to most method signatures for MAPREDUCE-2429
   * */

  public static final long versionID = 18L;
  
  /**
   * Called when a child task process starts, to get its task.
   * @param context the JvmContext of the JVM w.r.t the TaskTracker that
   *        launched it
   * @return Task object
   * @throws IOException 
   */
  JvmTask getTask(JvmContext context) throws IOException;

  /**
   * Report child's progress to parent.
   * 
   * @param taskId task-id of the child
   * @param taskStatus status of the child
   * @param jvmContext context the jvmContext running the task.
   * @throws IOException
   * @throws InterruptedException
   * @return True if the task is known
   */
  boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus,
      JvmContext jvmContext) throws IOException, InterruptedException;
  
  /** Report error messages back to parent.  Calls should be sparing, since all
   *  such messages are held in the job tracker.
   *  @param taskid the id of the task involved
   *  @param trace the text to report
   *  @param jvmContext context the jvmContext running the task.
   */
  void reportDiagnosticInfo(TaskAttemptID taskid, String trace,
      JvmContext jvmContext) throws IOException;
  
  /**
   * Report the record range which is going to process next by the Task.
   * @param taskid the id of the task involved
   * @param range the range of record sequence nos
   * @param jvmContext context the jvmContext running the task.
   * @throws IOException
   */
  void reportNextRecordRange(TaskAttemptID taskid, SortedRanges.Range range,
      JvmContext jvmContext) throws IOException;

  /** Periodically called by child to check if parent is still alive.
   * @param taskid the id of the task involved
   * @param jvmContext context the jvmContext running the task.
   * @return True if the task is known
   */
  boolean ping(TaskAttemptID taskid, JvmContext jvmContext) throws IOException;

  /** Report that the task is successfully completed.  Failure is assumed if
   * the task process exits without calling this.
   * @param taskid task's id
   * @param jvmContext context the jvmContext running the task.
   */
  void done(TaskAttemptID taskid, JvmContext jvmContext) throws IOException;
  
  /** 
   * Report that the task is complete, but its commit is pending.
   * 
   * @param taskId task's id
   * @param taskStatus status of the child
   * @param jvmContext context the jvmContext running the task.
   * @throws IOException
   */
  void commitPending(TaskAttemptID taskId, TaskStatus taskStatus,
      JvmContext jvmContext) throws IOException, InterruptedException;  

  /**
   * Polling to know whether the task can go-ahead with commit 
   * @param taskid
   * @param jvmContext context the jvmContext running the task.
   * @return true/false 
   * @throws IOException
   */
  boolean canCommit(TaskAttemptID taskid, JvmContext jvmContext) throws IOException;

  /** Report that a reduce-task couldn't shuffle map-outputs. */
  void shuffleError(TaskAttemptID taskId, String message, JvmContext jvmContext)
      throws IOException;
  
  /** Report that the task encounted a local filesystem error.*/
  void fsError(TaskAttemptID taskId, String message, JvmContext jvmContext)
      throws IOException;

  /** Report that the task encounted a fatal error.*/
  void fatalError(TaskAttemptID taskId, String message, JvmContext jvmContext)
      throws IOException;
  
  /** Called by a reduce task to get the map output locations for finished maps.
   * Returns an update centered around the map-task-completion-events. 
   * The update also piggybacks the information whether the events copy at the 
   * task-tracker has changed or not. This will trigger some action at the 
   * child-process.
   *
   * @param jobId the reducer job id
   * @param fromIndex the index starting from which the locations should be 
   * fetched
   * @param maxLocs the max number of locations to fetch
   * @param id The attempt id of the task that is trying to communicate
   * @return A {@link MapTaskCompletionEventsUpdate} 
   */
  MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
                                                       int fromIndex, 
                                                       int maxLocs,
                                                       TaskAttemptID id,
                                                       JvmContext jvmContext) 
  throws IOException;

  /**
   * The job initializer needs to report the sizes of the archive
   * objects in the private distributed cache.
   * @param jobId the job to update
   * @param sizes the array of sizes that were computed
   * @throws IOException
   */
  void updatePrivateDistributedCacheSizes(org.apache.hadoop.mapreduce.JobID jobId,
                                          long[] sizes) throws IOException;
}
