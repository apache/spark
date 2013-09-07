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
import org.apache.spark.util.{MetadataCleaner, TimeStampedHashMap}

private[spark] object ResultTask {

  // A simple map between the stage id to the serialized byte array of a task.
  // Served as a cache for task serialization because serialization can be
  // expensive on the master node if it needs to launch thousands of tasks.
  val serializedInfoCache = new TimeStampedHashMap[Int, Array[Byte]]

  val metadataCleaner = new MetadataCleaner("ResultTask", serializedInfoCache.clearOldValues)

  def serializeInfo(stageId: Int, rdd: RDD[_], func: (TaskContext, Iterator[_]) => _): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId).orNull
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val ser = SparkEnv.get.closureSerializer.newInstance
        val objOut = ser.serializeStream(new GZIPOutputStream(out))
        objOut.writeObject(rdd)
        objOut.writeObject(func)
        objOut.close()
        val bytes = out.toByteArray
        serializedInfoCache.put(stageId, bytes)
        return bytes
      }
    }
  }

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], (TaskContext, Iterator[_]) => _) = {
    val loader = Thread.currentThread.getContextClassLoader
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val ser = SparkEnv.get.closureSerializer.newInstance
    val objIn = ser.deserializeStream(in)
    val rdd = objIn.readObject().asInstanceOf[RDD[_]]
    val func = objIn.readObject().asInstanceOf[(TaskContext, Iterator[_]) => _]
    return (rdd, func)
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
    }
  }
}


private[spark] class ResultTask[T, U](
    stageId: Int,
    var rdd: RDD[T],
    var func: (TaskContext, Iterator[T]) => U,
    var partition: Int,
    @transient locs: Seq[TaskLocation],
    var outputId: Int)
  extends Task[U](stageId) with Externalizable {

  def this() = this(0, null, null, 0, null, 0)

  var split = if (rdd == null) {
    null
  } else {
    rdd.partitions(partition)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def run(attemptId: Long): U = {
    val context = new TaskContext(stageId, partition, attemptId, runningLocally = false)
    metrics = Some(context.taskMetrics)
    try {
      func(context, rdd.iterator(split, context))
    } finally {
      context.executeOnCompleteCallbacks()
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"

  override def writeExternal(out: ObjectOutput) {
    RDDCheckpointData.synchronized {
      split = rdd.partitions(partition)
      out.writeInt(stageId)
      val bytes = ResultTask.serializeInfo(
        stageId, rdd, func.asInstanceOf[(TaskContext, Iterator[_]) => _])
      out.writeInt(bytes.length)
      out.write(bytes)
      out.writeInt(partition)
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
    partition = in.readInt()
    outputId = in.readInt()
    epoch = in.readLong()
    split = in.readObject().asInstanceOf[Partition]
  }
}
