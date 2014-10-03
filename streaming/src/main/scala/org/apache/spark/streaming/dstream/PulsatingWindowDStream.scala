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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.streaming.{Time, Duration, Interval}
import scala.reflect.ClassTag


private[streaming]
class PulsatingWindowDStream[T: ClassTag](parent: DStream[T],
                                               sizeNumBatches: Int,
                                               delayNumBins: Int)
  extends DStream[T](parent.ssc) {

  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def parentRememberDuration: Duration = 
    rememberDuration + parent.slideDuration * sizeNumBatches * (delayNumBins + 1)

  override def compute(validTime: Time): Option[RDD[T]] = {

    val binStart = (validTime - Duration(1)).floor(slideDuration * sizeNumBatches) - 
      slideDuration * sizeNumBatches * delayNumBins

    val currentWindow = new Interval(binStart + slideDuration, validTime)

    Some(new UnionRDD(parent.ssc.sc, parent.slice(currentWindow)))
  }
}



