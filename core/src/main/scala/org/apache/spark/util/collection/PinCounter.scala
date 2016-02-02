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
 * Thread-safe collection for maintaining both global and per-task pin counts for objects.
 */
private[spark] class PinCounter[T] {

  private type TaskAttemptId = Long

  /**
   * Total pins across all tasks.
   */
  private[this] val allPins = ConcurrentHashMultiset.create[T]()

  /**
   * Total pins per task. Used to auto-release pins upon task completion.
   */
  private[this] val pinsByTask = {
    // We need to explicitly box as java.lang.Long to avoid a type mismatch error:
    val loader = new CacheLoader[java.lang.Long, ConcurrentHashMultiset[T]] {
      override def load(t: java.lang.Long) = ConcurrentHashMultiset.create[T]()
    }
    CacheBuilder.newBuilder().build(loader)
  }

  /**
   * Returns the total pin count, across all tasks, for the given object.
   */
  def getPinCount(obj: T): Int = allPins.count(obj)

  /**
   * Increments the given object's pin count for the current task.
   */
  def pin(obj: T): Unit = pinForTask(currentTaskAttemptId, obj)

  /**
   * Decrements the given object's pin count for the current task.
   */
  def unpin(obj: T): Unit = unpinForTask(currentTaskAttemptId, obj)

  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Increments the given object's pin count for the given task.
   */
  def pinForTask(taskAttemptId: TaskAttemptId, obj: T): Unit = {
    pinsByTask.get(taskAttemptId).add(obj)
    allPins.add(obj)
  }

  /**
   * Decrements the given object's pin count for the given task.
   */
  def unpinForTask(taskAttemptId: TaskAttemptId, obj: T): Unit = {
    val countsForTask = pinsByTask.get(taskAttemptId)
    val newPinCountForTask: Int = countsForTask.remove(obj, 1) - 1
    val newTotalPinCount: Int = allPins.remove(obj, 1) - 1
    if (newPinCountForTask < 0) {
      throw new IllegalStateException(
        s"Task $taskAttemptId released object $obj more times than it was retained")
    }
    if (newTotalPinCount < 0) {
      throw new IllegalStateException(
        s"Task $taskAttemptId released object $obj more times than it was retained")
    }
  }

  /**
   * Release all pins held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   *
   * @return the number of pins released
   */
  def releaseAllPinsForTask(taskAttemptId: TaskAttemptId): Int = {
    val pinCounts = pinsByTask.get(taskAttemptId)
    pinsByTask.invalidate(taskAttemptId)
    val totalPinCountForTask = pinCounts.size()
    pinCounts.entrySet().iterator().asScala.foreach { entry =>
      val obj = entry.getElement
      val taskRefCount = entry.getCount
      val newRefCount = allPins.remove(obj, taskRefCount) - taskRefCount
      if (newRefCount < 0) {
        throw new IllegalStateException(
          s"Task $taskAttemptId released object $obj more times than it was retained")
      }
    }
    totalPinCountForTask
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[collection] def getNumberOfMapEntries: Long = {
    allPins.size() +
      pinsByTask.size() +
      pinsByTask.asMap().asScala.map(_._2.size()).sum
  }
}
