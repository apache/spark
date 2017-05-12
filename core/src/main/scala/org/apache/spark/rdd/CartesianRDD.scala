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

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.{CompletionIterator, Utils}

private[spark]
class CartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

private[spark]
class CartesianRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T],
    var rdd2 : RDD[U])
  extends RDD[(T, U)](sc, Nil)
  with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
    val blockManager = SparkEnv.get.blockManager
    val currSplit = split.asInstanceOf[CartesianPartition]
    val blockId2 = RDDBlockId(rdd2.id, currSplit.s2.index)
    var cachedInLocal = false
    var holdReadLock = false

    // Try to get data from the local, otherwise it will be cached to the local.
    def getOrElseCache(
        rdd: RDD[U],
        partition: Partition,
        context: TaskContext,
        level: StorageLevel): Iterator[U] = {
      getLocalValues() match {
        case Some(result) =>
          return result
        case None => // do nothing
      }

      val iterator = rdd.iterator(partition, context)
      // Keep read lock, because next we need read it. And don't tell master.
      blockManager.putIterator[U](blockId2, iterator, level, false, true) match {
        case true =>
          cachedInLocal = true
          // After we cached the block, we also hold the block read lock until this task finished.
          holdReadLock = true
        case false =>
          // There shouldn't a error caused by put in memory, because we use MEMORY_AND_DISK to
          // cache it.
          throw new SparkException(s"Cache block $blockId2 in local failed even though it's $level")
      }

      logInfo(s"Cache the block $blockId2 to local successful.")
      getLocalValues() match {
        // We don't need release the read lock, it will release after the iterator completion.
        case Some(result) => result
        case None =>
          throw new SparkException(s"Block $blockId2 was not found even though it's read-locked")
      }
    }

    // Get block from local, and update the metrics.
    def getLocalValues(): Option[Iterator[U]] = {
      blockManager.getLocalValues(blockId2) match {
        case Some(result) =>
          val existingMetrics = context.taskMetrics().inputMetrics
          existingMetrics.incBytesRead(result.bytes)
          val localIter =
            new InterruptibleIterator[U](context, result.data.asInstanceOf[Iterator[U]]) {
              override def next(): U = {
                existingMetrics.incRecordsRead(1)
                delegate.next()
              }
          }
          Some(localIter)
        case None =>
          None
      }
    }

    def removeCachedBlock(): Unit = {
      val blockManager = SparkEnv.get.blockManager
      if (holdReadLock) {
        // If hold the read lock, we need release it.
        blockManager.releaseLock(blockId2)
      }
      // Whether the block it persisted by the user.
      val persistedInLocal =
        blockManager.master.getLocations(blockId2).contains(blockManager.blockManagerId)
      if (!persistedInLocal && (cachedInLocal || blockManager.isRemovable(blockId2))) {
        blockManager.removeOrMarkAsRemovable(blockId2, false)
      }
    }

    val resultIter =
      for (x <- rdd1.iterator(currSplit.s1, context);
           y <- getOrElseCache(rdd2, currSplit.s2, context, StorageLevel.MEMORY_AND_DISK))
        yield (x, y)

    CompletionIterator[(T, U), Iterator[(T, U)]](resultIter, removeCachedBlock())
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
