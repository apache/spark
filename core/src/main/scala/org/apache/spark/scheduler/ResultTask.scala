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

package org.apache.spark.scheduler

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDDCheckpointData
import org.apache.spark.util.{MetadataCleanerType, MetadataCleaner, TimeStampedHashMap}

private[spark] object ResultTask {

  // A simple map between the stage id to the serialized byte array of a task.
  // Served as a cache for task serialization because serialization can be
  // expensive on the master node if it needs to launch thousands of tasks.
  val serializedInfoCache = new TimeStampedHashMap[Int, Array[Byte]]

  // TODO: This object shouldn't have global variables
  val metadataCleaner = new MetadataCleaner(
    MetadataCleanerType.RESULT_TASK, serializedInfoCache.clearOldValues, new SparkConf)

  def serializeInfo(stageId: Int, rdd: RDD[_], func: (TaskContext, Iterator[_]) => _): Array[Byte] =
  {
    synchronized {
      val old = serializedInfoCache.get(stageId).orNull
      if (old != null) {
        old
      } else {
        val out = new ByteArrayOutputStream
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val objOut = ser.serializeStream(new GZIPOutputStream(out))
        objOut.writeObject(rdd)
        objOut.writeObject(func)
        objOut.close()
        val bytes = out.toByteArray
        serializedInfoCache.put(stageId, bytes)
        bytes
      }
    }
  }

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], (TaskContext, Iterator[_]) => _) =
  {
    val loader = Thread.currentThread.getContextClassLoader
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val objIn = ser.deserializeStream(in)
    val rdd = objIn.readObject().asInstanceOf[RDD[_]]
    val func = objIn.readObject().asInstanceOf[(TaskContext, Iterator[_]) => _]
    (rdd, func)
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
    }
  }
}


/**
 * A task that sends back the output to the driver application.
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param rdd input to func
 * @param func a function to apply on a partition of the RDD
 * @param _partitionId index of the number in the RDD
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    var rdd: RDD[T],
    var func: (TaskContext, Iterator[T]) => U,
    _partitionId: Int,
    @transient locs: Seq[TaskLocation],
    var outputId: Int)
  extends Task[U](stageId, _partitionId) with Externalizable {

  def this() = this(0, null, null, 0, null, 0)

  var split = if (rdd == null) null else rdd.partitions(partitionId)

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    metrics = Some(context.taskMetrics)
    try {
      func(context, rdd.iterator(split, context))
    } finally {
      context.executeOnCompleteCallbacks()
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ResultTask(" + stageId + ", " + partitionId + ")"

  override def writeExternal(out: ObjectOutput) {
    RDDCheckpointData.synchronized {
      split = rdd.partitions(partitionId)
      out.writeInt(stageId)
      val bytes = ResultTask.serializeInfo(
        stageId, rdd, func.asInstanceOf[(TaskContext, Iterator[_]) => _])
      out.writeInt(bytes.length)
      out.write(bytes)
      out.writeInt(partitionId)
      out.writeInt(outputId)
      out.writeLong(epoch)
      out.writeObject(split)
    }
  }

  override def readExternal(in: ObjectInput) {
    val stageId = in.readInt()
    val numBytes = in.readInt()
    val bytes = new Array[Byte](numBytes)
    in.readFully(bytes)
    val (rdd_, func_) = ResultTask.deserializeInfo(stageId, bytes)
    rdd = rdd_.asInstanceOf[RDD[T]]
    func = func_.asInstanceOf[(TaskContext, Iterator[T]) => U]
    partitionId = in.readInt()
    outputId = in.readInt()
    epoch = in.readLong()
    split = in.readObject().asInstanceOf[Partition]
  }
}
