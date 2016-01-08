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

package org.apache.spark.util.collection

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.collect.ConcurrentHashMultiset

import org.apache.spark.TaskContext

/**
 * Thread-safe collection for maintaining both global and per-task reference counts for objects.
 */
private[spark] class ReferenceCounter[T] {

  private type TaskAttemptId = Long

  /**
   * Total references across all tasks.
   */
  private[this] val allReferences = ConcurrentHashMultiset.create[T]()

  /**
   * Total references per task. Used to auto-release references upon task completion.
   */
  private[this] val referencesByTask = {
    // We need to explicitly box as java.lang.Long to avoid a type mismatch error:
    val loader = new CacheLoader[java.lang.Long, ConcurrentHashMultiset[T]] {
      override def load(t: java.lang.Long) = ConcurrentHashMultiset.create[T]()
    }
    CacheBuilder.newBuilder().build(loader)
  }

  /**
   * Returns the total reference count, across all tasks, for the given object.
   */
  def getReferenceCount(obj: T): Int = allReferences.count(obj)

  /**
   * Increments the given object's reference count for the current task.
   */
  def retain(obj: T): Unit = retainForTask(currentTaskAttemptId, obj)

  /**
   * Decrements the given object's reference count for the current task.
   */
  def release(obj: T): Unit = releaseForTask(currentTaskAttemptId, obj)

  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Increments the given object's reference count for the given task.
   */
  def retainForTask(taskAttemptId: TaskAttemptId, obj: T): Unit = {
    referencesByTask.get(taskAttemptId).add(obj)
    allReferences.add(obj)
  }

  /**
   * Decrements the given object's reference count for the given task.
   */
  def releaseForTask(taskAttemptId: TaskAttemptId, obj: T): Unit = {
    val countsForTask = referencesByTask.get(taskAttemptId)
    val newReferenceCountForTask: Int = countsForTask.remove(obj, 1) - 1
    val newTotalReferenceCount: Int = allReferences.remove(obj, 1) - 1
    if (newReferenceCountForTask < 0) {
      throw new IllegalStateException(
        s"Task $taskAttemptId released object $obj more times than it was retained")
    }
    if (newTotalReferenceCount < 0) {
      throw new IllegalStateException(
        s"Task $taskAttemptId released object $obj more times than it was retained")
    }
  }

  /**
   * Release all references held by the given task, clearing that task's reference bookkeeping
   * structures and updating the global reference counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   */
  def releaseAllReferencesForTask(taskAttemptId: TaskAttemptId): Unit = {
    val referenceCounts = referencesByTask.get(taskAttemptId)
    referencesByTask.invalidate(taskAttemptId)
    referenceCounts.entrySet().iterator().asScala.foreach { entry =>
      val obj = entry.getElement
      val taskRefCount = entry.getCount
      val newRefCount = allReferences.remove(obj, taskRefCount) - taskRefCount
      if (newRefCount < 0) {
        throw new IllegalStateException(
          s"Task $taskAttemptId released object $obj more times than it was retained")
      }
    }
  }

  /**
   * Return the number of map entries in this reference counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[collection] def getNumberOfMapEntries: Long = {
    allReferences.size() +
      referencesByTask.size() +
      referencesByTask.asMap().asScala.map(_._2.size()).sum
  }
}
