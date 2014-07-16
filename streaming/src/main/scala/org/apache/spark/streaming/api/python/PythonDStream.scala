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

import java.util.{List => JList, ArrayList => JArrayList, Map => JMap, Collections}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.python._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Duration, Time}
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
        val pythonRDD = new PythonRDD(rdd, command, envVars, pythonIncludes, preservePartitoning, pythonExec, broadcastVars, accumulator)
        Some(pythonRDD.asJavaRDD.rdd)
      case None => None
    }
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}
