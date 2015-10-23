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

package org.apache.spark.streaming.dstream

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.util.StateMap
import org.apache.spark.util.Utils

private[streaming] case class TrackStateRDDRecord[K: ClassTag, S: ClassTag, T: ClassTag](
    stateMap: StateMap[K, S], emittedRecords: Seq[T])


private[streaming] class TrackStateRDDPartition(
    idx: Int,
    @transient private var prevStateRDD: RDD[_],
    @transient private var partitionedDataRDD: RDD[_]) extends Partition {

  private[dstream] var previousSessionRDDPartition: Partition = null
  private[dstream] var partitionedDataRDDPartition: Partition = null

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

private[streaming] class TrackStateRDD[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    _sc: SparkContext,
    private var prevStateRDD: RDD[TrackStateRDDRecord[K, S, T]],
    private var partitionedDataRDD: RDD[(K, V)],
    trackingFunction: (K, Option[V], State[S]) => Option[T],
    currentTime: Long, timeoutThresholdTime: Option[Long]
  ) extends RDD[TrackStateRDDRecord[K, S, T]](
    _sc,
    List(
      new OneToOneDependency[TrackStateRDDRecord[K, S, T]](prevStateRDD),
      new OneToOneDependency(partitionedDataRDD))
  ) {

  @volatile private var doFullScan = false

  require(partitionedDataRDD.partitioner.nonEmpty)
  require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

  override val partitioner = prevStateRDD.partitioner

  override def checkpoint(): Unit = {
    super.checkpoint()
    doFullScan = true
  }

  override def compute(
      partition: Partition, context: TaskContext): Iterator[TrackStateRDDRecord[K, S, T]] = {

    val stateRDDPartition = partition.asInstanceOf[TrackStateRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)
    if (!prevStateRDDIterator.hasNext) {
      throw new SparkException(s"Could not find state map in previous state RDD")
    }

    val newStateMap = prevStateRDDIterator.next().stateMap.copy()
    val emittedRecords = new ArrayBuffer[T]

    val wrappedState = new StateImpl[S]()

    dataIterator.foreach { case (key, value) =>
      wrappedState.wrap(newStateMap.get(key))
      val emittedRecord = trackingFunction(key, Some(value), wrappedState)
      if (wrappedState.isRemoved) {
        newStateMap.remove(key)
      } else if (wrappedState.isUpdated) {
        newStateMap.put(key, wrappedState.get(), currentTime)
      }
      emittedRecords ++= emittedRecord
    }

    if (doFullScan) {
      if (timeoutThresholdTime.isDefined) {
        newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
          wrappedState.wrapTiminoutState(state)
          val emittedRecord = trackingFunction(key, None, wrappedState)
          emittedRecords ++= emittedRecord
        }
      }
    }

    Iterator(TrackStateRDDRecord(newStateMap, emittedRecords))
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new TrackStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
  }

  override def clearDependencies() {
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
  }
}

private[streaming] object TrackStateRDD {
  def createFromPairRDD[K: ClassTag, S: ClassTag, T: ClassTag](
      pairRDD: RDD[(K, S)],
      partitioner: Partitioner,
      updateTime: Long): RDD[TrackStateRDDRecord[K, S, T]] = {

    val createRecord = (iterator: Iterator[(K, S)]) => {
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, state) => stateMap.put(key, state, updateTime) }
      Iterator(TrackStateRDDRecord(stateMap, Seq.empty[T]))
    }
    pairRDD.partitionBy(partitioner).mapPartitions[TrackStateRDDRecord[K, S, T]](
      createRecord, true)
  }
}


// -----------------------------------------------
// ---------------- SessionDStream ---------------
// -----------------------------------------------


private[streaming] class TrackeStateDStream[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    parent: DStream[(K, V)], spec: TrackStateSpecImpl[K, V, S, T])
  extends DStream[TrackStateRDDRecord[K, S, T]](parent.context) {

  persist(StorageLevel.MEMORY_ONLY)

  private val partitioner = spec.getPartitioner().getOrElse(
    new HashPartitioner(ssc.sc.defaultParallelism))

  private val trackingFunction = spec.getFunction()

  override def slideDuration: Duration = parent.slideDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override val mustCheckpoint = true

  /** Method that generates a RDD for the given time */
  override def compute(validTime: Time): Option[RDD[TrackStateRDDRecord[K, S, T]]] = {
    val prevStateRDD = getOrCompute(validTime - slideDuration).getOrElse {
      TrackStateRDD.createFromPairRDD[K, S, T](
        spec.getInitialStateRDD().getOrElse(new EmptyRDD[(K, S)](ssc.sparkContext)),
        partitioner,
        validTime.milliseconds
      )
    }
    val newDataRDD = parent.getOrCompute(validTime).get
    val partitionedDataRDD = newDataRDD.partitionBy(partitioner)
    val timeoutThresholdTime = spec.getTimeoutInterval().map { interval =>
      (validTime - interval).milliseconds
    }

    Some(new TrackStateRDD(
      ssc.sparkContext, prevStateRDD, partitionedDataRDD,
      trackingFunction, validTime.milliseconds, timeoutThresholdTime))
  }
}
