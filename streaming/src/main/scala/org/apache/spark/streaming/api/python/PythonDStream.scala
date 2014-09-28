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
import scala.collection.JavaConversions._

import org.apache.spark.api.java._
import org.apache.spark.api.java.function.{Function2 => JFunction2}
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Interval, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

/**
 * Interface for Python callback function with three arguments
 */
trait PythonRDDFunction {
  def call(rdd: JavaRDD[_], rdd2: JavaRDD[_], time: Long): JavaRDD[Array[Byte]]
}

class RDDFunction(pfunc: PythonRDDFunction) extends Serializable {

  def apply(rdd: Option[RDD[_]], time: Time): Option[RDD[Array[Byte]]] = {
      apply(rdd, None, time)
  }

  def wrapRDD(rdd: Option[RDD[_]]): JavaRDD[_] = {
    if (rdd.isDefined) {
      JavaRDD.fromRDD(rdd.get)
    } else {
      null
    }
  }

  def apply(rdd: Option[RDD[_]], rdd2: Option[RDD[_]], time: Time): Option[RDD[Array[Byte]]] = {
    val r = pfunc.call(wrapRDD(rdd), wrapRDD(rdd2), time.milliseconds)
    if (r != null) {
      Some(r.rdd)
    } else {
      None
    }
  }
}

private[python]
abstract class PythonDStream(parent: DStream[_]) extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  val asJavaDStream  = JavaDStream.fromDStream(this)
}

private[spark] object PythonDStream {

  // helper function for DStream.foreachRDD(),
  // cannot be `foreachRDD`, it will confusing py4j
  def callForeachRDD(jdstream: JavaDStream[Array[Byte]], pyfunc: PythonRDDFunction): Unit = {
    jdstream.dstream.foreachRDD((rdd, time) => pyfunc.call(rdd, null, time.milliseconds))
  }

  // convert list of RDD into queue of RDDs, for ssc.queueStream()
  def toRDDQueue(rdds: JArrayList[JavaRDD[Array[Byte]]]): java.util.Queue[JavaRDD[Array[Byte]]] = {
    val queue = new java.util.LinkedList[JavaRDD[Array[Byte]]]
    rdds.forall(queue.add(_))
    queue
  }
}

/**
 * Transformed DStream in Python.
 *
 * If the result RDD is PythonRDD, then it will cache it as an template for future use,
 * this can reduce the Python callbacks.
 */
private[spark] class PythonTransformedDStream (parent: DStream[_], pfunc: PythonRDDFunction,
                                var reuse: Boolean = false)
  extends PythonDStream(parent) {

  val func = new RDDFunction(pfunc)
  var lastResult: PythonRDD = _

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd1 = parent.getOrCompute(validTime)
    if (rdd1.isEmpty) {
      return None
    }
    if (reuse && lastResult != null) {
      Some(lastResult.copyTo(rdd1.get))
    } else {
      val r = func(rdd1, validTime)
      if (reuse && r.isDefined && lastResult == null) {
        r.get match {
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
      r
    }
  }
}

/**
 * Transformed from two DStreams in Python.
 */
private[spark]
class PythonTransformed2DStream(parent: DStream[_], parent2: DStream[_],
                                pfunc: PythonRDDFunction)
  extends DStream[Array[Byte]] (parent.ssc) {

  val func = new RDDFunction(pfunc)

  override def slideDuration: Duration = parent.slideDuration

  override def dependencies = List(parent, parent2)

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    func(parent.getOrCompute(validTime), parent2.getOrCompute(validTime), validTime)
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}

/**
 * similar to StateDStream
 */
private[spark]
class PythonStateDStream(parent: DStream[Array[Byte]], preduceFunc: PythonRDDFunction)
  extends PythonDStream(parent) {

  val reduceFunc = new RDDFunction(preduceFunc)

  super.persist(StorageLevel.MEMORY_ONLY)
  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration)
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      reduceFunc(lastState, rdd, validTime)
    } else {
      lastState
    }
  }
}

/**
 * similar to ReducedWindowedDStream
 */
private[spark]
class PythonReducedWindowedDStream(parent: DStream[Array[Byte]],
                                   preduceFunc: PythonRDDFunction,
                                   pinvReduceFunc: PythonRDDFunction,
                                   _windowDuration: Duration,
                                   _slideDuration: Duration
                                   ) extends PythonStateDStream(parent, preduceFunc) {

  assert(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  assert(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  val invReduceFunc = new RDDFunction(pinvReduceFunc)

  def windowDuration: Duration = _windowDuration
  override def slideDuration: Duration = _slideDuration
  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val currentTime = validTime
    val currentWindow = new Interval(currentTime - windowDuration + parent.slideDuration,
      currentTime)
    val previousWindow = currentWindow - slideDuration

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
    val previousWindowRDD = getOrCompute(previousWindow.endTime)

    // for small window, reduce once will be better than twice
    if (windowDuration > slideDuration * 5 && previousWindowRDD.isDefined) {
      // subtract the values from old RDDs
      val oldRDDs =
        parent.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration)
      val subbed = if (oldRDDs.size > 0) {
        invReduceFunc(previousWindowRDD, Some(ssc.sc.union(oldRDDs)), validTime)
      } else {
        previousWindowRDD
      }

      // add the RDDs of the reduced values in "new time steps"
      val newRDDs =
        parent.slice(previousWindow.endTime, currentWindow.endTime - parent.slideDuration)

      if (newRDDs.size > 0) {
        reduceFunc(subbed, Some(ssc.sc.union(newRDDs)), validTime)
      } else {
        subbed
      }
    } else {
      // Get the RDDs of the reduced values in current window
      val currentRDDs =
        parent.slice(currentWindow.beginTime, currentWindow.endTime - parent.slideDuration)
      if (currentRDDs.size > 0) {
        reduceFunc(None, Some(ssc.sc.union(currentRDDs)), validTime)
      } else {
        None
      }
    }
  }
}