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

import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.storage._
import org.apache.spark.util.{TimeStampedHashMap, MetadataCleaner}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDDCheckpointData


private[spark] object ShuffleMapTask {

  // A simple map between the stage id to the serialized byte array of a task.
  // Served as a cache for task serialization because serialization can be
  // expensive on the master node if it needs to launch thousands of tasks.
  val serializedInfoCache = new TimeStampedHashMap[Int, Array[Byte]]

  val metadataCleaner = new MetadataCleaner("ShuffleMapTask", serializedInfoCache.clearOldValues)

  def serializeInfo(stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_]): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId).orNull
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val objOut = ser.serializeStream(new GZIPOutputStream(out))
        objOut.writeObject(rdd)
        objOut.writeObject(dep)
        objOut.close()
        val bytes = out.toByteArray
        serializedInfoCache.put(stageId, bytes)
        return bytes
      }
    }
  }

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], ShuffleDependency[_,_]) = {
    synchronized {
      val loader = Thread.currentThread.getContextClassLoader
      val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
      val ser = SparkEnv.get.closureSerializer.newInstance()
      val objIn = ser.deserializeStream(in)
      val rdd = objIn.readObject().asInstanceOf[RDD[_]]
      val dep = objIn.readObject().asInstanceOf[ShuffleDependency[_,_]]
      return (rdd, dep)
    }
  }

  // Since both the JarSet and FileSet have the same format this is used for both.
  def deserializeFileSet(bytes: Array[Byte]) : HashMap[String, Long] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val objIn = new ObjectInputStream(in)
    val set = objIn.readObject().asInstanceOf[Array[(String, Long)]].toMap
    return (HashMap(set.toSeq: _*))
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
    }
  }
}

private[spark] class ShuffleMapTask(
    stageId: Int,
    var rdd: RDD[_],
    var dep: ShuffleDependency[_,_],
    var partition: Int,
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId)
  with Externalizable
  with Logging {

  protected def this() = this(0, null, null, 0, null)

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  var split = if (rdd == null) null else rdd.partitions(partition)

  override def writeExternal(out: ObjectOutput) {
    RDDCheckpointData.synchronized {
      split = rdd.partitions(partition)
      out.writeInt(stageId)
      val bytes = ShuffleMapTask.serializeInfo(stageId, rdd, dep)
      out.writeInt(bytes.length)
      out.write(bytes)
      out.writeInt(partition)
      out.writeLong(epoch)
      out.writeObject(split)
    }
  }

  override def readExternal(in: ObjectInput) {
    val stageId = in.readInt()
    val numBytes = in.readInt()
    val bytes = new Array[Byte](numBytes)
    in.readFully(bytes)
    val (rdd_, dep_) = ShuffleMapTask.deserializeInfo(stageId, bytes)
    rdd = rdd_
    dep = dep_
    partition = in.readInt()
    epoch = in.readLong()
    split = in.readObject().asInstanceOf[Partition]
  }

  override def run(attemptId: Long): MapStatus = {
    val numOutputSplits = dep.partitioner.numPartitions

    val taskContext = new TaskContext(stageId, partition, attemptId, runningLocally = false)
    metrics = Some(taskContext.taskMetrics)

    val blockManager = SparkEnv.get.blockManager
    var shuffle: ShuffleBlocks = null
    var buckets: ShuffleWriterGroup = null

    try {
      // Obtain all the block writers for shuffle blocks.
      val ser = SparkEnv.get.serializerManager.get(dep.serializerClass)
      shuffle = blockManager.shuffleBlockManager.forShuffle(dep.shuffleId, numOutputSplits, ser)
      buckets = shuffle.acquireWriters(partition)

      // Write the map output to its associated buckets.
      for (elem <- rdd.iterator(split, taskContext)) {
        val pair = elem.asInstanceOf[Product2[Any, Any]]
        val bucketId = dep.partitioner.getPartition(pair._1)
        buckets.writers(bucketId).write(pair)
      }

      // Commit the writes. Get the size of each bucket block (total block size).
      var totalBytes = 0L
      val compressedSizes: Array[Byte] = buckets.writers.map { writer: BlockObjectWriter =>
        writer.commit()
        writer.close()
        val size = writer.size()
        totalBytes += size
        MapOutputTracker.compressSize(size)
      }

      // Update shuffle metrics.
      val shuffleMetrics = new ShuffleWriteMetrics
      shuffleMetrics.shuffleBytesWritten = totalBytes
      metrics.get.shuffleWriteMetrics = Some(shuffleMetrics)

      return new MapStatus(blockManager.blockManagerId, compressedSizes)
    } catch { case e: Exception =>
      // If there is an exception from running the task, revert the partial writes
      // and throw the exception upstream to Spark.
      if (buckets != null) {
        buckets.writers.foreach(_.revertPartialWrites())
      }
      throw e
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && buckets != null) {
        shuffle.releaseWriters(buckets)
      }
      // Execute the callbacks on task completion.
      taskContext.executeOnCompleteCallbacks()
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
