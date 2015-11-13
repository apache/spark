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

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.rdd.{TrackStateRDD, TrackStateRDDRecord}

/**
 * :: Experimental ::
 * DStream representing the stream of records emitted by the tracking function in the
 * `trackStateByKey` operation on a
 * [[org.apache.spark.streaming.dstream.PairDStreamFunctions pair DStream]].
 * Additionally, it also gives access to the stream of state snapshots, that is, the state data of
 * all keys after a batch has updated them.
 *
 * @tparam KeyType Class of the state key
 * @tparam ValueType Class of the state value
 * @tparam StateType Class of the state data
 * @tparam EmittedType Class of the emitted records
 */
@Experimental
sealed abstract class TrackStateDStream[KeyType, ValueType, StateType, EmittedType: ClassTag](
    ssc: StreamingContext) extends DStream[EmittedType](ssc) {

  /** Return a pair DStream where each RDD is the snapshot of the state of all the keys. */
  def stateSnapshots(): DStream[(KeyType, StateType)]
}

/** Internal implementation of the [[TrackStateDStream]] */
private[streaming] class TrackStateDStreamImpl[
    KeyType: ClassTag, ValueType: ClassTag, StateType: ClassTag, EmittedType: ClassTag](
    dataStream: DStream[(KeyType, ValueType)],
    spec: StateSpecImpl[KeyType, ValueType, StateType, EmittedType])
  extends TrackStateDStream[KeyType, ValueType, StateType, EmittedType](dataStream.context) {

  private val internalStream =
    new InternalTrackStateDStream[KeyType, ValueType, StateType, EmittedType](dataStream, spec)

  override def slideDuration: Duration = internalStream.slideDuration

  override def dependencies: List[DStream[_]] = List(internalStream)

  override def compute(validTime: Time): Option[RDD[EmittedType]] = {
    internalStream.getOrCompute(validTime).map { _.flatMap[EmittedType] { _.emittedRecords } }
  }

  /**
   * Forward the checkpoint interval to the internal DStream that computes the state maps. This
   * to make sure that this DStream does not get checkpointed, only the internal stream.
   */
  override def checkpoint(checkpointInterval: Duration): DStream[EmittedType] = {
    internalStream.checkpoint(checkpointInterval)
    this
  }

  /** Return a pair DStream where each RDD is the snapshot of the state of all the keys. */
  def stateSnapshots(): DStream[(KeyType, StateType)] = {
    internalStream.flatMap {
      _.stateMap.getAll().map { case (k, s, _) => (k, s) }.toTraversable }
  }

  def keyClass: Class[_] = implicitly[ClassTag[KeyType]].runtimeClass

  def valueClass: Class[_] = implicitly[ClassTag[ValueType]].runtimeClass

  def stateClass: Class[_] = implicitly[ClassTag[StateType]].runtimeClass

  def emittedClass: Class[_] = implicitly[ClassTag[EmittedType]].runtimeClass
}

/**
 * A DStream that allows per-key state to be maintains, and arbitrary records to be generated
 * based on updates to the state. This is the main DStream that implements the `trackStateByKey`
 * operation on DStreams.
 *
 * @param parent Parent (key, value) stream that is the source
 * @param spec Specifications of the trackStateByKey operation
 * @tparam K   Key type
 * @tparam V   Value type
 * @tparam S   Type of the state maintained
 * @tparam E   Type of the emitted data
 */
private[streaming]
class InternalTrackStateDStream[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
    parent: DStream[(K, V)], spec: StateSpecImpl[K, V, S, E])
  extends DStream[TrackStateRDDRecord[K, S, E]](parent.context) {

  persist(StorageLevel.MEMORY_ONLY)

  private val partitioner = spec.getPartitioner().getOrElse(
    new HashPartitioner(ssc.sc.defaultParallelism))

  private val trackingFunction = spec.getFunction()

  override def slideDuration: Duration = parent.slideDuration

  override def dependencies: List[DStream[_]] = List(parent)

  /** Enable automatic checkpointing */
  override val mustCheckpoint = true

  /** Method that generates a RDD for the given time */
  override def compute(validTime: Time): Option[RDD[TrackStateRDDRecord[K, S, E]]] = {
    // Get the previous state or create a new empty state RDD
    val prevStateRDD = getOrCompute(validTime - slideDuration).getOrElse {
      TrackStateRDD.createFromPairRDD[K, V, S, E](
        spec.getInitialStateRDD().getOrElse(new EmptyRDD[(K, S)](ssc.sparkContext)),
        partitioner, validTime
      )
    }

    // Compute the new state RDD with previous state RDD and partitioned data RDD
    parent.getOrCompute(validTime).map { dataRDD =>
      val partitionedDataRDD = dataRDD.partitionBy(partitioner)
      val timeoutThresholdTime = spec.getTimeoutInterval().map { interval =>
        (validTime - interval).milliseconds
      }
      new TrackStateRDD(
        prevStateRDD, partitionedDataRDD, trackingFunction, validTime, timeoutThresholdTime)
    }
  }
}
