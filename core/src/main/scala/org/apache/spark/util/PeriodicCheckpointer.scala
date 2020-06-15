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

package org.apache.spark.util

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging


/**
 * This abstraction helps with persisting and checkpointing RDDs and types derived from RDDs
 * (such as Graphs and DataFrames).  In documentation, we use the phrase "Dataset" to refer to
 * the distributed data type (RDD, Graph, etc.).
 *
 * Specifically, this abstraction automatically handles persisting and (optionally) checkpointing,
 * as well as unpersisting and removing checkpoint files.
 *
 * Users should call update() when a new Dataset has been created,
 * before the Dataset has been materialized.  After updating [[PeriodicCheckpointer]], users are
 * responsible for materializing the Dataset to ensure that persisting and checkpointing actually
 * occur.
 *
 * When update() is called, this does the following:
 *  - Persist new Dataset (if not yet persisted), and put in queue of persisted Datasets.
 *  - Unpersist Datasets from queue until there are at most 3 persisted Datasets.
 *  - If using checkpointing and the checkpoint interval has been reached,
 *     - Checkpoint the new Dataset, and put in a queue of checkpointed Datasets.
 *     - Remove older checkpoints.
 *
 * WARNINGS:
 *  - This class should NOT be copied (since copies may conflict on which Datasets should be
 *    checkpointed).
 *  - This class removes checkpoint files once later Datasets have been checkpointed.
 *    However, references to the older Datasets will still return isCheckpointed = true.
 *
 * @param checkpointInterval  Datasets will be checkpointed at this interval.
 *                            If this interval was set as -1, then checkpointing will be disabled.
 * @param sc  SparkContext for the Datasets given to this checkpointer
 * @tparam T  Dataset type, such as RDD[Double]
 */
private[spark] abstract class PeriodicCheckpointer[T](
    val checkpointInterval: Int,
    val sc: SparkContext) extends Logging {

  /** FIFO queue of past checkpointed Datasets */
  private val checkpointQueue = mutable.Queue[T]()

  /** FIFO queue of past persisted Datasets */
  private val persistedQueue = mutable.Queue[T]()

  /** Number of times [[update()]] has been called */
  private var updateCount = 0

  /**
   * Update with a new Dataset. Handle persistence and checkpointing as needed.
   * Since this handles persistence and checkpointing, this should be called before the Dataset
   * has been materialized.
   *
   * @param newData  New Dataset created from previous Datasets in the lineage.
   */
  def update(newData: T): Unit = {
    persist(newData)
    persistedQueue.enqueue(newData)
    // We try to maintain 2 Datasets in persistedQueue to support the semantics of this class:
    // Users should call [[update()]] when a new Dataset has been created,
    // before the Dataset has been materialized.
    while (persistedQueue.size > 3) {
      val dataToUnpersist = persistedQueue.dequeue()
      unpersist(dataToUnpersist)
    }
    updateCount += 1

    // Handle checkpointing (after persisting)
    if (checkpointInterval != -1 && (updateCount % checkpointInterval) == 0
      && sc.getCheckpointDir.nonEmpty) {
      // Add new checkpoint before removing old checkpoints.
      checkpoint(newData)
      checkpointQueue.enqueue(newData)
      // Remove checkpoints before the latest one.
      var canDelete = true
      while (checkpointQueue.size > 1 && canDelete) {
        // Delete the oldest checkpoint only if the next checkpoint exists.
        if (isCheckpointed(checkpointQueue(1))) {
          removeCheckpointFile()
        } else {
          canDelete = false
        }
      }
    }
  }

  /** Checkpoint the Dataset */
  protected def checkpoint(data: T): Unit

  /** Return true iff the Dataset is checkpointed */
  protected def isCheckpointed(data: T): Boolean

  /**
   * Persist the Dataset.
   * Note: This should handle checking the current [[StorageLevel]] of the Dataset.
   */
  protected def persist(data: T): Unit

  /** Unpersist the Dataset */
  protected def unpersist(data: T): Unit

  /** Get list of checkpoint files for this given Dataset */
  protected def getCheckpointFiles(data: T): Iterable[String]

  /**
   * Call this to unpersist the Dataset.
   */
  def unpersistDataSet(): Unit = {
    while (persistedQueue.nonEmpty) {
      val dataToUnpersist = persistedQueue.dequeue()
      unpersist(dataToUnpersist)
    }
  }

  /**
   * Call this at the end to delete any remaining checkpoint files.
   */
  def deleteAllCheckpoints(): Unit = {
    while (checkpointQueue.nonEmpty) {
      removeCheckpointFile()
    }
  }

  /**
   * Call this at the end to delete any remaining checkpoint files, except for the last checkpoint.
   * Note that there may not be any checkpoints at all.
   */
  def deleteAllCheckpointsButLast(): Unit = {
    while (checkpointQueue.size > 1) {
      removeCheckpointFile()
    }
  }

  /**
   * Get all current checkpoint files.
   * This is useful in combination with [[deleteAllCheckpointsButLast()]].
   */
  def getAllCheckpointFiles: Array[String] = {
    checkpointQueue.flatMap(getCheckpointFiles).toArray
  }

  /**
   * Dequeue the oldest checkpointed Dataset, and remove its checkpoint files.
   * This prints a warning but does not fail if the files cannot be removed.
   */
  private def removeCheckpointFile(): Unit = {
    val old = checkpointQueue.dequeue()
    // Since the old checkpoint is not deleted by Spark, we manually delete it.
    getCheckpointFiles(old).foreach(
      PeriodicCheckpointer.removeCheckpointFile(_, sc.hadoopConfiguration))
  }
}

private[spark] object PeriodicCheckpointer extends Logging {

  /** Delete a checkpoint file, and log a warning if deletion fails. */
  def removeCheckpointFile(checkpointFile: String, conf: Configuration): Unit = {
    try {
      val path = new Path(checkpointFile)
      val fs = path.getFileSystem(conf)
      fs.delete(path, true)
    } catch {
      case e: Exception =>
        logWarning("PeriodicCheckpointer could not remove old checkpoint file: " +
          checkpointFile)
    }
  }
}
