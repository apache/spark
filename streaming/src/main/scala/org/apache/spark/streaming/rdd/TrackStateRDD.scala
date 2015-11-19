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

package org.apache.spark.streaming.rdd

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.streaming.{Time, StateImpl, State}
import org.apache.spark.streaming.util.{EmptyStateMap, StateMap}
import org.apache.spark.util.Utils
import org.apache.spark._

/**
 * Record storing the keyed-state [[TrackStateRDD]]. Each record contains a [[StateMap]] and a
 * sequence of records returned by the tracking function of `trackStateByKey`.
 */
private[streaming] case class TrackStateRDDRecord[K, S, E](
    var stateMap: StateMap[K, S], var emittedRecords: Seq[E])

private[streaming] object TrackStateRDDRecord {
  def updateRecordWithData[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
    prevRecord: Option[TrackStateRDDRecord[K, S, E]],
    dataIterator: Iterator[(K, V)],
    updateFunction: (Time, K, Option[V], State[S]) => Option[E],
    batchTime: Time,
    timeoutThresholdTime: Option[Long],
    removeTimedoutData: Boolean
  ): TrackStateRDDRecord[K, S, E] = {
    // Create a new state map by cloning the previous one (if it exists) or by creating an empty one
    val newStateMap = prevRecord.map { _.stateMap.copy() }. getOrElse { new EmptyStateMap[K, S]() }

    val emittedRecords = new ArrayBuffer[E]
    val wrappedState = new StateImpl[S]()

    // Call the tracking function on each record in the data iterator, and accordingly
    // update the states touched, and collect the data returned by the tracking function
    dataIterator.foreach { case (key, value) =>
      wrappedState.wrap(newStateMap.get(key))
      val emittedRecord = updateFunction(batchTime, key, Some(value), wrappedState)
      if (wrappedState.isRemoved) {
        newStateMap.remove(key)
      } else if (wrappedState.isUpdated || timeoutThresholdTime.isDefined) {
        newStateMap.put(key, wrappedState.get(), batchTime.milliseconds)
      }
      emittedRecords ++= emittedRecord
    }

    // Get the timed out state records, call the tracking function on each and collect the
    // data returned
    if (removeTimedoutData && timeoutThresholdTime.isDefined) {
      newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
        wrappedState.wrapTiminoutState(state)
        val emittedRecord = updateFunction(batchTime, key, None, wrappedState)
        emittedRecords ++= emittedRecord
        newStateMap.remove(key)
      }
    }

    TrackStateRDDRecord(newStateMap, emittedRecords)
  }
}

/**
 * Partition of the [[TrackStateRDD]], which depends on corresponding partitions of prev state
 * RDD, and a partitioned keyed-data RDD
 */
private[streaming] class TrackStateRDDPartition(
    idx: Int,
    @transient private var prevStateRDD: RDD[_],
    @transient private var partitionedDataRDD: RDD[_]) extends Partition {

  private[rdd] var previousSessionRDDPartition: Partition = null
  private[rdd] var partitionedDataRDDPartition: Partition = null

  override def index: Int = idx
  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    previousSessionRDDPartition = prevStateRDD.partitions(index)
    partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
    oos.defaultWriteObject()
  }
}


/**
 * RDD storing the keyed-state of `trackStateByKey` and corresponding emitted records.
 * Each partition of this RDD has a single record of type [[TrackStateRDDRecord]]. This contains a
 * [[StateMap]] (containing the keyed-states) and the sequence of records returned by the tracking
 * function of  `trackStateByKey`.
 * @param prevStateRDD The previous TrackStateRDD on whose StateMap data `this` RDD will be created
 * @param partitionedDataRDD The partitioned data RDD which is used update the previous StateMaps
 *                           in the `prevStateRDD` to create `this` RDD
 * @param trackingFunction The function that will be used to update state and return new data
 * @param batchTime        The time of the batch to which this RDD belongs to. Use to update
 * @param timeoutThresholdTime The time to indicate which keys are timeout
 */
private[streaming] class TrackStateRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
    private var prevStateRDD: RDD[TrackStateRDDRecord[K, S, E]],
    private var partitionedDataRDD: RDD[(K, V)],
    trackingFunction: (Time, K, Option[V], State[S]) => Option[E],
    batchTime: Time,
    timeoutThresholdTime: Option[Long]
  ) extends RDD[TrackStateRDDRecord[K, S, E]](
    partitionedDataRDD.sparkContext,
    List(
      new OneToOneDependency[TrackStateRDDRecord[K, S, E]](prevStateRDD),
      new OneToOneDependency(partitionedDataRDD))
  ) {

  @volatile private var doFullScan = false

  require(prevStateRDD.partitioner.nonEmpty)
  require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

  override val partitioner = prevStateRDD.partitioner

  override def checkpoint(): Unit = {
    super.checkpoint()
    doFullScan = true
  }

  override def compute(
      partition: Partition, context: TaskContext): Iterator[TrackStateRDDRecord[K, S, E]] = {

    val stateRDDPartition = partition.asInstanceOf[TrackStateRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)

    val prevRecord = if (prevStateRDDIterator.hasNext) Some(prevStateRDDIterator.next()) else None
    val newRecord = TrackStateRDDRecord.updateRecordWithData(
      prevRecord,
      dataIterator,
      trackingFunction,
      batchTime,
      timeoutThresholdTime,
      removeTimedoutData = doFullScan // remove timedout data only when full scan is enabled
    )
    Iterator(newRecord)
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new TrackStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
  }

  def setFullScan(): Unit = {
    doFullScan = true
  }
}

private[streaming] object TrackStateRDD {

  def createFromPairRDD[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
      pairRDD: RDD[(K, S)],
      partitioner: Partitioner,
      updateTime: Time): TrackStateRDD[K, V, S, T] = {

    val rddOfTrackStateRecords = pairRDD.partitionBy(partitioner).mapPartitions ({ iterator =>
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, state) => stateMap.put(key, state, updateTime.milliseconds) }
      Iterator(TrackStateRDDRecord(stateMap, Seq.empty[T]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

    val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None

    new TrackStateRDD[K, V, S, T](rddOfTrackStateRecords, emptyDataRDD, noOpFunc, updateTime, None)
  }
}

private[streaming] class EmittedRecordsRDD[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    parent: TrackStateRDD[K, V, S, T]) extends RDD[T](parent) {
  override protected def getPartitions: Array[Partition] = parent.partitions
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    parent.compute(partition, context).flatMap { _.emittedRecords }
  }
}
