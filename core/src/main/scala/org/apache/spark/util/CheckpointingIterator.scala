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

import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, ReliableCheckpointRDD}
import org.apache.spark.storage.RDDBlockId

/**
 * Wrapper around an iterator which writes checkpoint data to HDFS while running action on
 * an RDD.
 *
 * @param id the unique id for a partition of an RDD
 * @param values the data to be checkpointed
 * @param fs the FileSystem to use
 * @param tempOutputPath the temp path to write the checkpoint data
 * @param finalOutputPath the final path to move the temp file to when finishing checkpointing
 * @param context the task context
 * @param blockSize the block size for writing the checkpoint data
 */
private[spark] class CheckpointingIterator[T: ClassTag](
    id: RDDBlockId,
    values: Iterator[T],
    fs: FileSystem,
    tempOutputPath: Path,
    finalOutputPath: Path,
    context: TaskContext,
    blockSize: Int) extends Iterator[T] with Logging {

  private[this] var completed = false

  // We don't know if the task is successful. So it's possible that we still checkpoint the
  // remaining values even if the task is failed.
  // TODO optimize the failure case if we can know the task status
  context.addTaskCompletionListener { _ => complete() }

  private[this] val fileOutputStream: OutputStream = {
    val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)
    if (blockSize < 0) {
      fs.create(tempOutputPath, false, bufferSize)
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize, fs.getDefaultReplication, blockSize)
    }
  }

  private[this] val serializeStream =
    SparkEnv.get.serializer.newInstance().serializeStream(fileOutputStream)

  /**
   * Called when this iterator is on the last element by `hasNext`.
   * This method will rename temporary output path to final output path of checkpoint data.
   */
  private[this] def complete(): Unit = {
    if (completed) {
      return
    }

    if (serializeStream == null) {
      // There is some exception when creating serializeStream, we only need to clean up the
      // resources.
      cleanup()
      return
    }

    while (values.hasNext) {
      serializeStream.writeObject(values.next)
    }

    completed = true
    CheckpointingIterator.releaseLockForPartition(id)
    serializeStream.close()

    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.exists(finalOutputPath)) {
        logInfo("Deleting tempOutputPath " + tempOutputPath)
        fs.delete(tempOutputPath, false)
        throw new IOException("Checkpoint failed: failed to save output of task: "
          + context.attemptNumber + " and final output path does not exist")
      } else {
        // Some other copy of this task must've finished before us and renamed it
        logInfo("Final output path " + finalOutputPath + " already exists; not overwriting it")
        fs.delete(tempOutputPath, false)
      }
    }
  }

  private[this] def cleanup(): Unit = {
    completed = true
    CheckpointingIterator.releaseLockForPartition(id)
    if (serializeStream != null) {
      serializeStream.close()
    }
    fs.delete(tempOutputPath, false)
  }

  private[this] def cleanupOnFailure[A](body: => A): A = {
    try {
      body
    } catch {
      case e: Throwable =>
        try {
          cleanup()
        } catch {
          case NonFatal(e1) =>
            // Log `e1` since we should not override `e`
            logError(e1.getMessage, e1)
        }
        throw e
    }
  }

  override def hasNext: Boolean = cleanupOnFailure {
    val r = values.hasNext
    if (!r) {
      complete()
    }
    r
  }

  override def next(): T = cleanupOnFailure {
    val value = values.next()
    serializeStream.writeObject(value)
    value
  }
}

private[spark] object CheckpointingIterator {

  private val checkpointingRDDPartitions = new ConcurrentHashMap[RDDBlockId, RDDBlockId]()

  /**
   * Return true if the caller gets the lock to write the checkpoint file. Otherwise, the caller
   * should not do checkpointing.
   */
  private def acquireLockForPartition(id: RDDBlockId): Boolean = {
    checkpointingRDDPartitions.putIfAbsent(id, id) == null
  }

  /**
   * Release the lock to avoid memory leak.
   */
  private def releaseLockForPartition(id: RDDBlockId): Unit = {
    checkpointingRDDPartitions.remove(id)
  }

  /**
   * Create a `CheckpointingIterator` to wrap the original `Iterator` so that when the wrapper is
   * consumed, it will checkpoint the values. Even if the wrapper is not drained, we will still
   * drain the remaining values when a task is completed.
   *
   * If this method is called multiple times for the same partition of an `RDD`, only one `Iterator`
   * that gets the lock will be wrapped. For other `Iterator`s that don't get the lock or find the
   * partition has been checkpointed, we just return the original `Iterator`.
   */
  def apply[T: ClassTag](
      rdd: RDD[T],
      values: Iterator[T],
      path: String,
      broadcastedConf: Broadcast[SerializableConfiguration],
      partitionIndex: Int,
      context: TaskContext,
      blockSize: Int = -1): Iterator[T] = {
    val id = RDDBlockId(rdd.id, partitionIndex)
    if (CheckpointingIterator.acquireLockForPartition(id)) {
      try {
        val outputDir = new Path(path)
        val fs = outputDir.getFileSystem(broadcastedConf.value.value)
        val finalOutputName = ReliableCheckpointRDD.checkpointFileName(partitionIndex)
        val finalOutputPath = new Path(outputDir, finalOutputName)
        if (fs.exists(finalOutputPath)) {
          // RDD has already been checkpointed by a previous task. So we don't need to checkpoint
          // again.
          return values
        }
        val tempOutputPath =
          new Path(outputDir, "." + finalOutputName + "-attempt-" + context.attemptNumber)
        if (fs.exists(tempOutputPath)) {
          throw new IOException(s"Checkpoint failed: temporary path $tempOutputPath already exists")
        }

        new CheckpointingIterator(
          id,
          values,
          fs,
          tempOutputPath,
          finalOutputPath,
          context,
          blockSize)
      } catch {
        case e: Throwable =>
          releaseLockForPartition(id)
          throw e
      }
    } else {
      values // Iterator is being checkpointed, so just return the values
    }

  }
}
