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

import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[streaming]
class UnionDStream[T: ClassTag](parents: Array[DStream[T]])
  extends DStream[T](parents.head.ssc) {

  if (parents.length == 0) {
    throw new IllegalArgumentException("Empty array of parents")
  }

  if (parents.map(_.ssc).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different StreamingContexts")
  }

  if (parents.map(_.slideDuration).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different slide times")
  }

  override def dependencies = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val rdds = new ArrayBuffer[RDD[T]]()
    parents.map(_.getOrCompute(validTime)).foreach(_ match {
      case Some(rdd) => rdds += rdd
      case None => throw new Exception("Could not generate RDD from a parent for unifying at time " + validTime)
    })
    if (rdds.size > 0) {
      Some(new UnionRDD(ssc.sc, rdds))
    } else {
      None
    }
  }
}
