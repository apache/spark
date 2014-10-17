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

import scala.Function0;
import scala.Function1;
import scala.Unit;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.util.TaskCompletionListener;

/**
 * Contextual information about a task which can be read or mutated during
 * execution. To access the TaskContext for a running task use
 * TaskContext.get().
 */
public abstract class TaskContext implements Serializable {
  /**
   * Return the currently active TaskContext. This can be called inside of
   * user functions to access contextual information about running tasks.
   */
  public static TaskContext get() {
    return taskContext.get();
  }

  private static ThreadLocal<TaskContext> taskContext =
    new ThreadLocal<TaskContext>();

  static void setTaskContext(TaskContext tc) {
    taskContext.set(tc);
  }

  static void unset() {
    taskContext.remove();
  }

  /**
   * Whether the task has completed.
   */
  public abstract boolean isCompleted();

  /**
   * Whether the task has been killed.
   */
  public abstract boolean isInterrupted();

  /** @deprecated: use isRunningLocally() */
  @Deprecated
  public abstract boolean runningLocally();

  public abstract boolean isRunningLocally();

  /**
   * Add a (Java friendly) listener to be executed on task completion.
   * This will be called in all situation - success, failure, or cancellation.
   * <p/>
   * An example use is for HadoopRDD to register a callback to close the input stream.
   */
  public abstract TaskContext addTaskCompletionListener(TaskCompletionListener listener);

  /**
   * Add a listener in the form of a Scala closure to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation.
   * <p/>
   * An example use is for HadoopRDD to register a callback to close the input stream.
   */
  public abstract TaskContext addTaskCompletionListener(final Function1<TaskContext, Unit> f);

  /**
   * Add a callback function to be executed on task completion. An example use
   * is for HadoopRDD to register a callback to close the input stream.
   * Will be called in any situation - success, failure, or cancellation.
   *
   * @deprecated: use addTaskCompletionListener
   *
   * @param f Callback function.
   */
  @Deprecated
  public abstract void addOnCompleteCallback(final Function0<Unit> f);

  public abstract int stageId();

  public abstract int partitionId();

  public abstract long attemptId();

  /** ::DeveloperApi:: */
  @DeveloperApi
  public abstract TaskMetrics taskMetrics();
}
