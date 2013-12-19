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
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Interval, Time, DStream}

import scala.reflect.ClassTag

private[streaming]
class WindowedDStream[T: ClassTag](
    parent: DStream[T],
    _windowDuration: Duration,
    _slideDuration: Duration)
  extends DStream[T](parent.ssc) {

  if (!_windowDuration.isMultipleOf(parent.slideDuration))
    throw new Exception("The window duration of WindowedDStream (" + _slideDuration + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")

  if (!_slideDuration.isMultipleOf(parent.slideDuration))
    throw new Exception("The slide duration of WindowedDStream (" + _slideDuration + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")

  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration =  _windowDuration

  override def dependencies = List(parent)

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    Some(new UnionRDD(ssc.sc, parent.slice(currentWindow)))
  }
}
