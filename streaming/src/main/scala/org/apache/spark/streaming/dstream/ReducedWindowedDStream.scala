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

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.{CoGroupedRDD, MapPartitionsRDD}
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.{Duration, Interval, Time}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[streaming]
class ReducedWindowedDStream[K: ClassTag, V: ClassTag](
    parent: DStream[(K, V)],
    reduceFunc: (V, V) => V,
    invReduceFunc: (V, V) => V,
    filterFunc: Option[((K, V)) => Boolean],
    _windowDuration: Duration,
    _slideDuration: Duration,
    partitioner: Partitioner
  ) extends DStream[(K, V)](parent.ssc) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  // Reduce each batch of data using reduceByKey which will be further reduced by window
  // by ReducedWindowedDStream
  private val reducedStream = parent.reduceByKey(reduceFunc, partitioner)

  // Persist RDDs to memory by default as these RDDs are going to be reused.
  super.persist(StorageLevel.MEMORY_ONLY_SER)
  reducedStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(reducedStream)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(storageLevel: StorageLevel): DStream[(K, V)] = {
    super.persist(storageLevel)
    reducedStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[(K, V)] = {
    super.checkpoint(interval)
    // reducedStream.checkpoint(interval)
    this
  }

  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    val reduceF = reduceFunc
    val invReduceF = invReduceFunc

    val currentTime = validTime
    val currentWindow = new Interval(currentTime - windowDuration + parent.slideDuration,
      currentTime)
    val previousWindow = currentWindow - slideDuration

    logDebug("Window time = " + windowDuration)
    logDebug("Slide time = " + slideDuration)
    logDebug("ZeroTime = " + zeroTime)
    logDebug("Current window = " + currentWindow)
    logDebug("Previous window = " + previousWindow)

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //

    // Get the RDDs of the reduced values in "old time steps"
    val oldRDDs =
      reducedStream.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration)
    logDebug("# old RDDs = " + oldRDDs.size)

    // Get the RDDs of the reduced values in "new time steps"
    val newRDDs =
      reducedStream.slice(previousWindow.endTime + parent.slideDuration, currentWindow.endTime)
    logDebug("# new RDDs = " + newRDDs.size)

    // Get the RDD of the reduced value of the previous window
    val previousWindowRDD =
      getOrCompute(previousWindow.endTime).getOrElse(ssc.sc.makeRDD(Seq[(K, V)]()))

    // Make the list of RDDs that needs to cogrouped together for reducing their reduced values
    val allRDDs = new ArrayBuffer[RDD[(K, V)]]() += previousWindowRDD ++= oldRDDs ++= newRDDs

    // Cogroup the reduced RDDs and merge the reduced values
    val cogroupedRDD = new CoGroupedRDD[K](allRDDs.toSeq.asInstanceOf[Seq[RDD[(K, _)]]],
      partitioner)
    // val mergeValuesFunc = mergeValues(oldRDDs.size, newRDDs.size) _

    val numOldValues = oldRDDs.size
    val numNewValues = newRDDs.size

    val mergeValues = (arrayOfValues: Array[Iterable[V]]) => {
      if (arrayOfValues.size != 1 + numOldValues + numNewValues) {
        throw new Exception("Unexpected number of sequences of reduced values")
      }
      // Getting reduced values "old time steps" that will be removed from current window
      val oldValues = (1 to numOldValues).map(i => arrayOfValues(i)).filter(!_.isEmpty).map(_.head)
      // Getting reduced values "new time steps"
      val newValues =
        (1 to numNewValues).map(i => arrayOfValues(numOldValues + i)).filter(!_.isEmpty).map(_.head)

      if (arrayOfValues(0).isEmpty) {
        // If previous window's reduce value does not exist, then at least new values should exist
        if (newValues.isEmpty) {
          throw new Exception("Neither previous window has value for key, nor new values found. " +
            "Are you sure your key class hashes consistently?")
        }
        // Reduce the new values
        newValues.reduce(reduceF) // return
      } else {
        // Get the previous window's reduced value
        var tempValue = arrayOfValues(0).head
        // If old values exists, then inverse reduce then from previous value
        if (!oldValues.isEmpty) {
          tempValue = invReduceF(tempValue, oldValues.reduce(reduceF))
        }
        // If new values exists, then reduce them with previous value
        if (!newValues.isEmpty) {
          tempValue = reduceF(tempValue, newValues.reduce(reduceF))
        }
        tempValue // return
      }
    }

    val mergedValuesRDD = cogroupedRDD.asInstanceOf[RDD[(K, Array[Iterable[V]])]]
      .mapValues(mergeValues)

    if (filterFunc.isDefined) {
      Some(mergedValuesRDD.filter(filterFunc.get))
    } else {
      Some(mergedValuesRDD)
    }
  }
}
