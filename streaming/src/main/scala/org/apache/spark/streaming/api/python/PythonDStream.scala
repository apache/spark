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

import java.io._
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._


import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java._
import org.apache.spark.api.python._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{StreamingContext, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._


class PythonDStream[T: ClassTag](
    parent: DStream[T],
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    preservePartitoning: Boolean,
    pythonExec: String,
    broadcastVars: JList[Broadcast[Array[Byte]]],
    accumulator: Accumulator[JList[Array[Byte]]])
  extends DStream[Array[Byte]](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  //pythonDStream compute
  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    parent.getOrCompute(validTime) match{
      case Some(rdd) =>
        // create PythonRDD to compute Python functions.
        val pythonRDD = new PythonRDD(rdd, command, envVars, pythonIncludes,
          preservePartitoning, pythonExec, broadcastVars, accumulator)
        Some(pythonRDD.asJavaRDD.rdd)
      case None => None
    }
  }

  def foreachRDD(foreachFunc: PythonRDDFunction) {
    new PythonForeachDStream(this, context.sparkContext.clean(foreachFunc, false)).register()
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}


private class PythonPairwiseDStream(
    prev:DStream[Array[Byte]],
    partitioner: Partitioner
  ) extends DStream[Array[Byte]](prev.ssc){
  override def dependencies = List(prev)

  override def slideDuration: Duration = prev.slideDuration

  override def compute(validTime:Time):Option[RDD[Array[Byte]]]={
    prev.getOrCompute(validTime) match{
      case Some(rdd)=>Some(rdd)
        val pairwiseRDD = new PairwiseRDD(rdd)
        /*
         * Since python function is executed by Scala after StreamingContext.start.
         * What PythonPairwiseDStream does is equivalent to python code in pyspark.
         *
         * with _JavaStackTrace(self.context) as st:
         *    pairRDD = self.ctx._jvm.PairwiseRDD(keyed._jrdd.rdd()).asJavaPairRDD()
         *    partitioner = self.ctx._jvm.PythonPartitioner(numPartitions,
         *                                                  id(partitionFunc))
         * jrdd = pairRDD.partitionBy(partitioner).values()
         * rdd = RDD(jrdd, self.ctx, BatchedSerializer(outputSerializer))
         */
        Some(pairwiseRDD.asJavaPairRDD.partitionBy(partitioner).values().rdd)
      case None => None
    }
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}


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

class PythonTestInputStream(
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
