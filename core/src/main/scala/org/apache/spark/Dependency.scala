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

package org.apache.spark

import java.util.concurrent.ScheduledFuture

import scala.reflect.ClassTag

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteProcessor}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *                   explicitly then the default serializer, as specified by `spark.serializer`
 *                   config option, will be used.
 * @param keyOrdering key ordering for RDD's shuffles
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
 * @param shuffleWriterProcessor the processor to control the write behavior in ShuffleMapTask
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false,
    val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor)
  extends Dependency[Product2[K, V]] with Logging {

  if (mapSideCombine) {
    require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
  }
  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  val shuffleId: Int = _rdd.context.newShuffleId()

  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, this)

  private[this] val numPartitions = rdd.partitions.length

  // By default, shuffle merge is allowed for ShuffleDependency if push based shuffle
  // is enabled
  private[this] var _shuffleMergeAllowed = canShuffleMergeBeEnabled()

  private[spark] def setShuffleMergeAllowed(shuffleMergeAllowed: Boolean): Unit = {
    _shuffleMergeAllowed = shuffleMergeAllowed
  }

  def shuffleMergeEnabled : Boolean = shuffleMergeAllowed && mergerLocs.nonEmpty

  def shuffleMergeAllowed : Boolean = _shuffleMergeAllowed

  /**
   * Stores the location of the list of chosen external shuffle services for handling the
   * shuffle merge requests from mappers in this shuffle map stage.
   */
  private[spark] var mergerLocs: Seq[BlockManagerId] = Nil

  /**
   * Stores the information about whether the shuffle merge is finalized for the shuffle map stage
   * associated with this shuffle dependency
   */
  private[this] var _shuffleMergeFinalized: Boolean = false

  /**
   * shuffleMergeId is used to uniquely identify merging process of shuffle
   * by an indeterminate stage attempt.
   */
  private[this] var _shuffleMergeId: Int = 0

  def shuffleMergeId: Int = _shuffleMergeId

  def setMergerLocs(mergerLocs: Seq[BlockManagerId]): Unit = {
    assert(shuffleMergeAllowed)
    this.mergerLocs = mergerLocs
  }

  def getMergerLocs: Seq[BlockManagerId] = mergerLocs

  private[spark] def markShuffleMergeFinalized(): Unit = {
    _shuffleMergeFinalized = true
  }

  private[spark] def isShuffleMergeFinalizedMarked: Boolean = {
    _shuffleMergeFinalized
  }

  /**
   * Returns true if push-based shuffle is disabled or if the shuffle merge for
   * this shuffle is finalized.
   */
  def shuffleMergeFinalized: Boolean = {
    if (shuffleMergeEnabled) {
      isShuffleMergeFinalizedMarked
    } else {
      true
    }
  }

  def newShuffleMergeState(): Unit = {
    _shuffleMergeFinalized = false
    mergerLocs = Nil
    _shuffleMergeId += 1
    finalizeTask = None
    shufflePushCompleted.clear()
  }

  private def canShuffleMergeBeEnabled(): Boolean = {
    val isPushShuffleEnabled = Utils.isPushBasedShuffleEnabled(rdd.sparkContext.conf,
      // invoked at driver
      isDriver = true)
    if (isPushShuffleEnabled && rdd.isBarrier()) {
      logWarning("Push-based shuffle is currently not supported for barrier stages")
    }
    isPushShuffleEnabled && numPartitions > 0 &&
      // TODO: SPARK-35547: Push based shuffle is currently unsupported for Barrier stages
      !rdd.isBarrier()
  }

  @transient private[this] val shufflePushCompleted = new RoaringBitmap()

  /**
   * Mark a given map task as push completed in the tracking bitmap.
   * Using the bitmap ensures that the same map task launched multiple times due to
   * either speculation or stage retry is only counted once.
   * @param mapIndex Map task index
   * @return number of map tasks with block push completed
   */
  private[spark] def incPushCompleted(mapIndex: Int): Int = {
    shufflePushCompleted.add(mapIndex)
    shufflePushCompleted.getCardinality
  }

  // Only used by DAGScheduler to coordinate shuffle merge finalization
  @transient private[this] var finalizeTask: Option[ScheduledFuture[_]] = None

  private[spark] def getFinalizeTask: Option[ScheduledFuture[_]] = finalizeTask

  private[spark] def setFinalizeTask(task: ScheduledFuture[_]): Unit = {
    finalizeTask = Option(task)
  }

  // Set the threshold to 1 billion which leads to an 128MB bitmap and
  // the actual size of `HighlyCompressedMapStatus` can be much larger than 128MB.
  // This may crash the driver with an OOM error.
  if (numPartitions.toLong * partitioner.numPartitions.toLong > (1L << 30)) {
    logWarning(
      log"The number of shuffle blocks " +
        log"(${MDC(NUM_PARTITIONS, numPartitions.toLong * partitioner.numPartitions.toLong)})" +
        log" for shuffleId ${MDC(SHUFFLE_ID, shuffleId)} " +
        log"for ${MDC(RDD_DESCRIPTION, _rdd)} " +
        log"with ${MDC(NUM_PARTITIONS2, numPartitions)} partitions" +
        log" is possibly too large, which could cause the driver to crash with an out-of-memory" +
        log" error. Consider decreasing the number of partitions in this shuffle stage."
    )
  }

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
  _rdd.sparkContext.shuffleDriverComponents.registerShuffle(shuffleId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
