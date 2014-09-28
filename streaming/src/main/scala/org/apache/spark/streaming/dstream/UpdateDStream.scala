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
import scala.reflect.ClassTag

class UpdateDStream[U: ClassTag, T: ClassTag, V: ClassTag](
    parent: DStream[T],
    updateFunc: (Option[RDD[T]], RDD[V]) => RDD[U],
    val basicRdd: RDD[V]
  ) extends DStream[T](parent.ssc) {

  var updatedRDD: RDD[U] = updateFunc(None, basicRdd)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val rdd = parent.getOrCompute(validTime)
    updatedRDD = updateFunc(rdd, basicRdd)
    rdd
  }

  def getUpdatedRDD: RDD[U] = this.updatedRDD
}
