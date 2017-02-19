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

import org.apache.spark.streaming.Duration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging


trait BatchEstimator extends Serializable {
  def compute(processingTime: Long, batchInterval: Long) : Option[Long]
}

object BatchEstimator extends Logging{

  //
  def create(conf: SparkConf, batchInterval: Duration): BatchEstimator = {

    if (isDefaultBatchIntervalEnabled(conf)) {
      conf.get("spark.streaming.BatchEstimator", "default") match {
        case "default" =>
          val proportional = conf.getDouble(
            "spark.streaming.BatchEstimator.default.proportional", 0.85)
          val converge = conf.getDouble("spark.streaming.BatchEstimator.default.converge", 30)
          val gradient = conf.getDouble("spark.streaming.BatchEstimator.default.gradient", 0.85)
          val stateThreshold = conf.getDouble(
            "spark.streaming.BatchEstimator.default.stateThreshold", 1.4)

          new DefaultBatchEstimator(proportional, converge, gradient, stateThreshold)

        case estimator =>
          throw new IllegalArgumentException(s"Unkown rate estimator: $estimator")
      }
    } else {
      conf.get("spark.streaming.BatchEstimator", "basic") match {
        case "basic" =>
          new BasicBatchEstimator(conf)

        case estimator =>
          throw new IllegalArgumentException(s"Unkown rate estimator: $estimator")
      }
    }
  }


  def isDefaultBatchIntervalEnabled(conf: SparkConf): Boolean =
    conf.getBoolean("spark.streaming.DefaultBatchInterval.enabled", false)

  def isBasicBatchIntervalEnabled(conf: SparkConf): Boolean =
    conf.getBoolean("spark.streaming.BasicBatchInterval.enabled", false)
}
