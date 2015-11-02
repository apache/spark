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
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.rdd.{TrackStateRDD, TrackStateRDDRecord}



abstract class EmittedRecordsDStream[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    ssc: StreamingContext) extends DStream[T](ssc) {

  def stateSnapshots(): DStream[(K, S)]
}


private[streaming] class EmittedRecordsDStreamImpl[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    trackStateDStream: TrackStateDStream[K, V, S, T])
  extends EmittedRecordsDStream[K, V, S, T](trackStateDStream.context) {

  override def slideDuration: Duration = trackStateDStream.slideDuration

  override def dependencies: List[DStream[_]] = List(trackStateDStream)

  override def compute(validTime: Time): Option[RDD[T]] = {
    trackStateDStream.getOrCompute(validTime).map { _.flatMap[T] { _.emittedRecords } }
  }

  def stateSnapshots(): DStream[(K, S)] = {
    trackStateDStream.flatMap[(K, S)] { _.stateMap.getAll().map { case (k, s, _) => (k, s) }.toTraversable }
  }
}

/**
 * A DStream that allows per-key state to be maintains, and arbitrary records to be generated
 * based on updates to the state.
 *
 * @param parent Parent (key, value) stream that is the source
 * @param spec Specifications of the trackStateByKey operation
 * @tparam K   Key type
 * @tparam V   Value type
 * @tparam S   Type of the state maintained
 * @tparam T   Type of the eiitted records
 */
private[streaming] class TrackStateDStream[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
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
      TrackStateRDD.createFromPairRDD[K, V, S, T](
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

    Some(new TrackStateRDD(prevStateRDD, partitionedDataRDD,
      trackingFunction, validTime.milliseconds, timeoutThresholdTime))
  }
}
