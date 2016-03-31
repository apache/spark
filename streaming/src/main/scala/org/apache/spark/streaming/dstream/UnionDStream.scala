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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD

private[streaming]
class UnionDStream[T: ClassTag](parents: Array[DStream[T]])
  extends DStream[T](parents.head.ssc) {

  require(parents.length > 0, "List of DStreams to union is empty")
  require(parents.map(_.ssc).distinct.size == 1, "Some of the DStreams have different contexts")
  require(parents.map(_.slideDuration).distinct.size == 1,
    "Some of the DStreams have different slide durations")

  override def dependencies: List[DStream[_]] = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val rdds = new ArrayBuffer[RDD[T]]()
    parents.map(_.getOrCompute(validTime)).foreach {
      case Some(rdd) => rdds += rdd
      case None => throw new SparkException("Could not generate RDD from a parent for unifying at" +
        s" time $validTime")
    }
    if (rdds.size > 0) {
      Some(new UnionRDD(ssc.sc, rdds))
    } else {
      None
    }
  }
}
