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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.atomic.AtomicLong

/**
 * Tracks the current continuous processing epoch within a task. Call
 * EpochTracker.getCurrentEpoch to get the current epoch.
 */
object EpochTracker {
  // The current epoch. Note that this is a shared reference; ContinuousWriteRDD.compute() will
  // update the underlying AtomicLong as it finishes epochs. Other code should only read the value.
  private val currentEpoch: InheritableThreadLocal[AtomicLong] = {
    new InheritableThreadLocal[AtomicLong] {
      override protected def childValue(parent: AtomicLong): AtomicLong = {
        // Note: make another instance so that changes in the parent epoch aren't reflected in
        // those in the children threads. This is required at `ContinuousCoalesceRDD`.
        new AtomicLong(parent.get)
      }
      override def initialValue() = new AtomicLong(-1)
    }
  }

  /**
   * Get the current epoch for the current task, or None if the task has no current epoch.
   */
  def getCurrentEpoch: Option[Long] = {
    currentEpoch.get().get() match {
      case n if n < 0 => None
      case e => Some(e)
    }
  }

  /**
   * Increment the current epoch for this task thread. Should be called by [[ContinuousWriteRDD]]
   * between epochs.
   */
  def incrementCurrentEpoch(): Unit = {
    currentEpoch.get().incrementAndGet()
  }

  /**
   * Initialize the current epoch for this task thread. Should be called by [[ContinuousWriteRDD]]
   * at the beginning of a task.
   */
  def initializeCurrentEpoch(startEpoch: Long): Unit = {
    currentEpoch.get().set(startEpoch)
  }
}
