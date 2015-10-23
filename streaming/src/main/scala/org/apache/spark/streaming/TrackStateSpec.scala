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

package org.apache.spark.streaming

import scala.reflect.ClassTag

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD


/**
 * Abstract class having all the specifications of DStream.trackStateByKey().
 * Use the `TrackStateSpec.create()` or `TrackStateSpec.create()` to create instances of this class.
 *
 * {{{
 *    TrackStateSpec(trackingFunction)            // in Scala
 *    TrackStateSpec.create(trackingFunction)     // in Java
 * }}}
 */
sealed abstract class TrackStateSpec[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag]
  extends Serializable {

  def initialState(rdd: RDD[(K, S)]): this.type
  def initialState(javaPairRDD: JavaPairRDD[K, S]): this.type

  def numPartitions(numPartitions: Int): this.type
  def partitioner(partitioner: Partitioner): this.type

  def timeout(interval: Duration): this.type
}


/** Builder object for creating instances of TrackStateSpec */
object TrackStateSpec {

  def apply[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
      trackingFunction: (K, Option[V], State[S]) => Option[T]): TrackStateSpec[K, V, S, T] = {
    new TrackStateSpecImpl[K, V, S, T](trackingFunction)
  }

  def create[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
      trackingFunction: (K, Option[V], State[S]) => Option[T]): TrackStateSpec[K, V, S, T] = {
    apply(trackingFunction)
  }
}


/** Internal implementation of [[TrackStateSpec]] interface */
private[streaming]
case class TrackStateSpecImpl[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    function: (K, Option[V], State[S]) => Option[T]) extends TrackStateSpec[K, V, S, T] {

  require(function != null)

  @volatile private var partitioner: Partitioner = null
  @volatile private var initialStateRDD: RDD[(K, S)] = null
  @volatile private var timeoutInterval: Duration = null


  def initialState(rdd: RDD[(K, S)]): this.type = {
    this.initialStateRDD = rdd
    this
  }

  def initialState(javaPairRDD: JavaPairRDD[K, S]): this.type = {
    this.initialStateRDD = javaPairRDD.rdd
    this
  }


  def numPartitions(numPartitions: Int): this.type = {
    this.partitioner(new HashPartitioner(numPartitions))
    this
  }

  def partitioner(partitioner: Partitioner): this.type = {
    this.partitioner = partitioner
    this
  }

  def timeout(interval: Duration): this.type = {
    this.timeoutInterval = interval
    this
  }

  // ================= Private Methods =================

  private[streaming] def getFunction(): (K, Option[V], State[S]) => Option[T] = function

  private[streaming] def getInitialStateRDD(): Option[RDD[(K, S)]] = Option(initialStateRDD)

  private[streaming] def getPartitioner(): Option[Partitioner] = Option(partitioner)

  private[streaming] def getTimeoutInterval(): Option[Duration] = Option(timeoutInterval)
}