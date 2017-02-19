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
package org.apache.spark.streaming.scheduler.batch

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class BasicBatchEstimator(conf: SparkConf)
  extends BatchEstimator with Logging{
  // The batch interval list
  private var batchList: Array[Long] = null

  // The id of the batch interval list
  private var id: Int = 0

  // The length of the batch interval list
  private var length: Int = 0

  init(conf)

  /** Initialize the batchList from the configure */
  def init(conf: SparkConf ) : Unit = {
    conf.get("spark.streaming.BatchEstimator", "basic") match {
      case "basic" =>
        val batchinfo = conf.get(
          "spark.streaming.BatchEstimator.basic.batchList", "0")
        val batchStr = batchinfo.split(",")
        length = batchStr.length
        batchList = new Array[Long](length)
        for(i <- 0 until length) {
          batchList(i) = toMaybeLong(batchStr.apply(i))
        }
      case estimator =>
        throw new IllegalArgumentException(s"Unknown rate estimator: $estimator")
    }
  }

  /** Convert A String to a Long value */
  def toMaybeLong(s : String ): Long = {
    return scala.util.Try(s.toLong).toOption.getOrElse(0)
  }

  /** algorithm to get the next batch interval */
  def compute(latestProcessingTime: Long, latestBatchInterval: Long): Option[Long] = {
    this.synchronized {
      if (id >= length) {
        id = 0
      }
      var batch = batchList.apply(id) * 1000
      id = id + 1
      if(batch == 0) {
        batch = latestBatchInterval
      }
      return Some(batch)
    }
  }
}
