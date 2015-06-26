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

import scala.reflect.ClassTag

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.CheckpointRDD
import org.apache.spark.serializer.SerializationStream

/**
 * Wrapper around an iterator which writes checkpoint data to HDFS while running action on
 * a RDD to support checkpointing RDD.
 */
private[spark] class CheckpointingIterator[A: ClassTag, +I <: Iterator[A]](
  sub: I,
  path: String,
  broadcastedConf: Broadcast[SerializableConfiguration],
  partitionId: Int,
  context: TaskContext,
  blockSize: Int = -1) extends Iterator[A] with Logging {

  val env = SparkEnv.get
  var fs: FileSystem = null
  val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
  var serializeStream: SerializationStream = null

  var finalOutputPath: Path = null
  var tempOutputPath: Path = null

  def init(): this.type = {
    val outputDir = new Path(path)
    fs = outputDir.getFileSystem(broadcastedConf.value.value)

    val finalOutputName = CheckpointRDD.splitIdToFile(partitionId)
    finalOutputPath = new Path(outputDir, finalOutputName)
    tempOutputPath =
      new Path(outputDir, "." + finalOutputName + "-attempt-" + context.attemptNumber)

    if (fs.exists(tempOutputPath)) {
      // There are more than one iterator of the RDD is consumed.
      // Don't checkpoint data in this iterator.
      doCheckpoint = false
      return this
    }

    val fileOutputStream = if (blockSize < 0) {
      fs.create(tempOutputPath, false, bufferSize)
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize, fs.getDefaultReplication, blockSize)
    }
    val serializer = env.serializer.newInstance()
    serializeStream = serializer.serializeStream(fileOutputStream)
    this
  }

  def completion(): Unit = {
    if (!doCheckpoint) {
      return
    }

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

  def checkpointing(item: A): Unit = {
    serializeStream.writeObject(item)
  }

  override def next(): A = {
    val item = sub.next()
    if (doCheckpoint) {
      checkpointing(item)
    }
    // If this the latest item, call hasNext will write to final output early.
    hasNext
    item
  }

  private[this] var doCheckpoint = true
  private[this] var completed = false

  override def hasNext: Boolean = {
    val r = sub.hasNext
    if (!r && !completed) {
      completed = true
      completion()
    }
    r
  }
}

private[spark] object CheckpointingIterator {
  def apply[A: ClassTag, I <: Iterator[A]](
    sub: I,
    path: String,
    broadcastedConf: Broadcast[SerializableConfiguration],
    partitionId: Int,
    context: TaskContext,
    blockSize: Int = -1) : CheckpointingIterator[A, I] = {
    new CheckpointingIterator[A, I](
      sub,
      path,
      broadcastedConf,
      partitionId,
      context,
      blockSize).init()
  }
}
