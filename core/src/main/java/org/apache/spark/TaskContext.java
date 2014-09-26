/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import scala.Function0;
import scala.Function1;
import scala.Unit;
import scala.collection.JavaConversions;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.TaskCompletionListenerException;

/**
* :: DeveloperApi ::
* Contextual information about a task which can be read or mutated during execution.
*/
@DeveloperApi
public class TaskContext implements Serializable {

  private int stageId;
  private int partitionId;
  private long attemptId;
  private boolean runningLocally;
  private TaskMetrics taskMetrics;

  /**
   * :: DeveloperApi ::
   * Contextual information about a task which can be read or mutated during execution.
   *
   * @param stageId stage id
   * @param partitionId index of the partition
   * @param attemptId the number of attempts to execute this task
   * @param runningLocally whether the task is running locally in the driver JVM
   * @param taskMetrics performance metrics of the task
   */
  @DeveloperApi
  public TaskContext(Integer stageId, Integer partitionId, Long attemptId, Boolean runningLocally,
                     TaskMetrics taskMetrics) {
    this.attemptId = attemptId;
    this.partitionId = partitionId;
    this.runningLocally = runningLocally;
    this.stageId = stageId;
    this.taskMetrics = taskMetrics;
  }


  /**
   * :: DeveloperApi ::
   * Contextual information about a task which can be read or mutated during execution.
   *
   * @param stageId stage id
   * @param partitionId index of the partition
   * @param attemptId the number of attempts to execute this task
   * @param runningLocally whether the task is running locally in the driver JVM
   */
  @DeveloperApi
  public TaskContext(Integer stageId, Integer partitionId, Long attemptId,
                     Boolean runningLocally) {
    this.attemptId = attemptId;
    this.partitionId = partitionId;
    this.runningLocally = runningLocally;
    this.stageId = stageId;
    this.taskMetrics = TaskMetrics.empty();
  }


  /**
   * :: DeveloperApi ::
   * Contextual information about a task which can be read or mutated during execution.
   *
   * @param stageId stage id
   * @param partitionId index of the partition
   * @param attemptId the number of attempts to execute this task
   */
  @DeveloperApi
  public TaskContext(Integer stageId, Integer partitionId, Long attemptId) {
    this.attemptId = attemptId;
    this.partitionId = partitionId;
    this.runningLocally = false;
    this.stageId = stageId;
    this.taskMetrics = TaskMetrics.empty();
  }

  private static ThreadLocal<TaskContext> taskContext =
    new ThreadLocal<TaskContext>();

  /**
  * :: Internal API ::
  * This is spark internal API, not intended to be called from user programs.
  */
  public static void setTaskContext(TaskContext tc) {
    taskContext.set(tc);
  }

  public static TaskContext get() {
    return taskContext.get();
  }

  /** 
  * :: Internal API ::
  */
  public static void remove() {
    taskContext.remove();
  }

  // List of callback functions to execute when the task completes.
  private transient List<TaskCompletionListener> onCompleteCallbacks =
    new ArrayList<TaskCompletionListener>();

  // Whether the corresponding task has been killed.
  private volatile Boolean interrupted = false;

  // Whether the task has completed.
  private volatile Boolean completed = false;

  /**
   * Checks whether the task has completed.
   */
  public Boolean isCompleted() {
    return completed;
  }

  /**
   * Checks whether the task has been killed.
   */
  public Boolean isInterrupted() {
    return interrupted;
  }

  /**
   * Add a (Java friendly) listener to be executed on task completion.
   * This will be called in all situation - success, failure, or cancellation.
   * <p/>
   * An example use is for HadoopRDD to register a callback to close the input stream.
   */
  public TaskContext addTaskCompletionListener(TaskCompletionListener listener) {
    onCompleteCallbacks.add(listener);
    return this;
  }

  /**
   * Add a listener in the form of a Scala closure to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation.
   * <p/>
   * An example use is for HadoopRDD to register a callback to close the input stream.
   */
  public TaskContext addTaskCompletionListener(final Function1<TaskContext, Unit> f) {
    onCompleteCallbacks.add(new TaskCompletionListener() {
      @Override
      public void onTaskCompletion(TaskContext context) {
        f.apply(context);
      }
    });
    return this;
  }

  /**
   * Add a callback function to be executed on task completion. An example use
   * is for HadoopRDD to register a callback to close the input stream.
   * Will be called in any situation - success, failure, or cancellation.
   *
   * Deprecated: use addTaskCompletionListener
   * 
   * @param f Callback function.
   */
  @Deprecated
  public void addOnCompleteCallback(final Function0<Unit> f) {
    onCompleteCallbacks.add(new TaskCompletionListener() {
      @Override
      public void onTaskCompletion(TaskContext context) {
        f.apply();
      }
    });
  }

  /**
   * ::Internal API::
   * Marks the task as completed and triggers the listeners.
   */
  public void markTaskCompleted() throws TaskCompletionListenerException {
    completed = true;
    List<String> errorMsgs = new ArrayList<String>(2);
    // Process complete callbacks in the reverse order of registration
    List<TaskCompletionListener> revlist =
      new ArrayList<TaskCompletionListener>(onCompleteCallbacks);
    Collections.reverse(revlist);
    for (TaskCompletionListener tcl: revlist) {
      try {
        tcl.onTaskCompletion(this);
      } catch (Throwable e) {
        errorMsgs.add(e.getMessage());
      }
    }

    if (!errorMsgs.isEmpty()) {
      throw new TaskCompletionListenerException(JavaConversions.asScalaBuffer(errorMsgs));
    }
  }

  /**
   * ::Internal API::
   * Marks the task for interruption, i.e. cancellation.
   */
  public void markInterrupted() {
    interrupted = true;
  }

  @Deprecated
  /** Deprecated: use getStageId() */
  public int stageId() {
    return stageId;
  }

  @Deprecated
  /** Deprecated: use getPartitionId() */
  public int partitionId() {
    return partitionId;
  }

  @Deprecated
  /** Deprecated: use getAttemptId() */
  public long attemptId() {
    return attemptId;
  }

  @Deprecated
  /** Deprecated: use getRunningLocally() */
  public boolean runningLocally() {
    return runningLocally;
  }

  public boolean getRunningLocally() {
    return runningLocally;
  }

  public int getStageId() {
    return stageId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getAttemptId() {
    return attemptId;
  }  

  /** ::Internal API:: */
  public TaskMetrics taskMetrics() {
    return taskMetrics;
  }
}
