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

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java._
import org.apache.spark.api.python._
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._


/**
 * Interface for Python callback function
 */
trait PythonRDDFunction {
  def call(rdd: JavaRDD[_], rdd2: JavaRDD[_], time: Long): JavaRDD[Array[Byte]]
}


/**
 * Transformed DStream in Python.
 *
 * If the result RDD is PythonRDD, then it will cache it as an template for future use,
 * this can reduce the Python callbacks.
 *
 * @param parent
 * @param parent2
 * @param func
 * @param cache
 */
class PythonTransformedDStream (parent: DStream[_], parent2: DStream[_], func: PythonRDDFunction,
                                cache: Boolean = false) //TODO: better name
  extends DStream[Array[Byte]] (parent.ssc) {

  var lastResult: PythonRDD = _

  override def dependencies = {
    if (parent2 == null) {
      List(parent)
    } else {
      List(parent, parent2)
    }
  }

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd1 = parent.getOrCompute(validTime).getOrElse(null)
    val rdd2 = if (parent2 != null) parent2.getOrCompute(validTime).getOrElse(null) else null

    val r = if (rdd2 != null) {
      func.call(JavaRDD.fromRDD(rdd1), JavaRDD.fromRDD(rdd2), validTime.milliseconds)
    } else if (cache && lastResult != null) {
      lastResult.copyTo(rdd1).asJavaRDD
    } else {
      func.call(JavaRDD.fromRDD(rdd1), null, validTime.milliseconds)
    }
    if (r != null) {
      if (lastResult == null && r.isInstanceOf[PythonRDD]) {
        lastResult = r.asInstanceOf[PythonRDD]
      }
      Some(r)
    } else {
      None
    }
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}


/**
 * This is used for foreachRDD() in Python
 * @param prev
 * @param foreachFunction
 */
class PythonForeachDStream(
    prev: DStream[Array[Byte]],
    foreachFunction: PythonRDDFunction
  ) extends ForEachDStream[Array[Byte]](
    prev,
    (rdd: RDD[Array[Byte]], time: Time) => {
      foreachFunction.call(rdd.toJavaRDD(), null, time.milliseconds)
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
