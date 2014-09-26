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

package org.apache.spark.streaming.api.python

import java.util.{ArrayList => JArrayList}

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{CoGroupedRDD, UnionRDD, PartitionerAwareUnionRDD, RDD}
import org.apache.spark.api.java._
import org.apache.spark.api.python._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Interval, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
 * Interface for Python callback function with two arguments
 */
trait PythonRDDFunction {
  def call(rdd: JavaRDD[_], time: Long): JavaRDD[Array[Byte]]
}

/**
 * Interface for Python callback function with three arguments
 */
trait PythonRDDFunction2 {
  def call(rdd: JavaRDD[_], rdd2: JavaRDD[_], time: Long): JavaRDD[Array[Byte]]
}

/**
 * Transformed DStream in Python.
 *
 * If the result RDD is PythonRDD, then it will cache it as an template for future use,
 * this can reduce the Python callbacks.
 */
private[spark] class PythonTransformedDStream (parent: DStream[_], func: PythonRDDFunction,
                                var reuse: Boolean = false)
  extends DStream[Array[Byte]] (parent.ssc) {

  var lastResult: PythonRDD = _

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd1 = parent.getOrCompute(validTime).getOrElse(null)
    if (rdd1 == null) {
      return None
    }
    if (reuse && lastResult != null) {
      Some(lastResult.copyTo(rdd1))
    } else {
      val r = func.call(JavaRDD.fromRDD(rdd1), validTime.milliseconds).rdd
      if (reuse && lastResult == null) {
        r match {
          case rdd: PythonRDD =>
            if (rdd.parent(0) == rdd1) {
              // only one PythonRDD
              lastResult = rdd
            } else {
              // may have multiple stages
              reuse = false
            }
        }
      }
      Some(r)
    }
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}

/**
 * Transformed from two DStreams in Python.
 */
private[spark] class PythonTransformed2DStream (parent: DStream[_], parent2: DStream[_], func: PythonRDDFunction2)
  extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies = List(parent, parent2)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    def resultRdd(stream: DStream[_]): JavaRDD[_] = stream.getOrCompute(validTime) match {
      case Some(rdd) => JavaRDD.fromRDD(rdd)
      case None => null
    }
    Some(func.call(resultRdd(parent), resultRdd(parent2), validTime.milliseconds))
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}


/**
 * Copied from ReducedWindowedDStream
 */
private[spark]
class PythonReducedWindowedDStream(
                                  parent: DStream[Array[Byte]],
                                  reduceFunc: PythonRDDFunction,
                                  invReduceFunc: PythonRDDFunction2,
                                  _windowDuration: Duration,
                                  _slideDuration: Duration
                                  ) extends DStream[Array[Byte]](parent.ssc) {

  assert(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  assert(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )


  // Persist RDDs to memory by default as these RDDs are going to be reused.
  super.persist(StorageLevel.MEMORY_ONLY)

  def windowDuration: Duration =  _windowDuration

  override def dependencies = List(parent)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    None
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


    // Get the RDD of the reduced value of the previous window
    val previousWindowRDD =
      getOrCompute(previousWindow.endTime)

    if (windowDuration > slideDuration * 5 && previousWindowRDD.isDefined) {
      // subtle the values from old RDDs
      val oldRDDs =
        parent.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration)
      val subbed = if (oldRDDs.size > 0) {
        invReduceFunc.call(JavaRDD.fromRDD(previousWindowRDD.get),
          JavaRDD.fromRDD(ssc.sc.union(oldRDDs)), validTime.milliseconds).rdd
      } else {
        previousWindowRDD.get
      }

      // add the RDDs of the reduced values in "new time steps"
      val newRDDs =
        parent.slice(previousWindow.endTime, currentWindow.endTime - parent.slideDuration)

      if (newRDDs.size > 0) {
        Some(reduceFunc.call(JavaRDD.fromRDD(ssc.sc.union(newRDDs).union(subbed)), validTime.milliseconds))
      } else {
        Some(subbed)
      }
    } else {
      // Get the RDDs of the reduced values in current window
      val currentRDDs =
        parent.slice(currentWindow.beginTime, currentWindow.endTime - parent.slideDuration)
      if (currentRDDs.size > 0) {
        Some(reduceFunc.call(JavaRDD.fromRDD(ssc.sc.union(currentRDDs)), validTime.milliseconds))
      } else {
        None
      }
    }
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}


/**
 * This is used for foreachRDD() in Python
 */
class PythonForeachDStream(
    prev: DStream[Array[Byte]],
    foreachFunction: PythonRDDFunction
  ) extends ForEachDStream[Array[Byte]](
    prev,
    (rdd: RDD[Array[Byte]], time: Time) => {
      foreachFunction.call(rdd.toJavaRDD(), time.milliseconds)
    }
  ) {

  this.register()
}


/**
 * This is a input stream just for the unitest. This is equivalent to a checkpointable,
 * replayable, reliable message queue like Kafka. It requires a JArrayList of JavaRDD,
 * and returns the i_th element at the i_th batch under manual clock.
 */

class PythonDataInputStream(
    ssc_ : JavaStreamingContext,
    inputRDDs: JArrayList[JavaRDD[Array[Byte]]]
  ) extends InputDStream[Array[Byte]](JavaStreamingContext.toStreamingContext(ssc_)) {

  def start() {}

  def stop() {}

  def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val emptyRDD = ssc.sparkContext.emptyRDD[Array[Byte]]
    val index = ((validTime - zeroTime) / slideDuration - 1).toInt
    val selectedRDD = {
      if (inputRDDs.isEmpty) {
        emptyRDD
      } else if (index < inputRDDs.size()) {
        inputRDDs.get(index).rdd
      } else {
        emptyRDD
      }
    }

    Some(selectedRDD)
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}
