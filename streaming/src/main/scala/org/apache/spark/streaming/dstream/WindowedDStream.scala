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

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration

private[streaming]
class WindowedDStream[T: ClassTag](
    parent: DStream[T],
    _windowDuration: Duration,
    _slideDuration: Duration)
  extends DStream[T](parent.ssc) {

  if (!_windowDuration.isMultipleOf(parent.slideDuration)) {
    throw new Exception("The window duration of windowed DStream (" + _windowDuration + ") " +
    "must be a multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")
  }

  if (!_slideDuration.isMultipleOf(parent.slideDuration)) {
    throw new Exception("The slide duration of windowed DStream (" + _slideDuration + ") " +
    "must be a multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")
  }

  // Persist parent level by default, as those RDDs are going to be obviously reused.
  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[T]] = List(parent)

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(level: StorageLevel): DStream[T] = {
    // Do not let this windowed DStream be persisted as windowed (union-ed) RDDs share underlying
    // RDDs and persisting the windowed RDDs would store numerous copies of the underlying data.
    // Instead control the persistence of the parent DStream.
    parent.persist(level)
    this
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val rddsInWindow = parent.slice(currentWindow)
    Some(ssc.sc.union(rddsInWindow))
  }
}
