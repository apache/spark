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

package org.apache.spark.ml.util

import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

private[ml] class Instrumentation[E <: Estimator[_]] private (
    estimator: E, dataset: RDD[_]) extends Logging {

  init()

  private def init(): Unit = {
    log(s"training: numPartitions=${dataset.partitions.length}" +
      s" storageLevel=${dataset.getStorageLevel}")
  }

  def log(msg: String): Unit = {
    val className = estimator.getClass.getSimpleName
    logInfo(s"Instrumentation($className-${estimator.uid}-${dataset.hashCode()}):$msg")
  }

  def logParams(params: Param[_]*): Unit = {
    val pairs = for {
      p <- params
      value <- estimator.get(p)
    } yield {
      s"${p.name}=$value"
    }
    log(s"${pairs.mkString(" ")}")
  }

  def logNumFeatures(num: Long): Unit = {
    log(s"numFeatures=$num")
  }

  def logNumClasses(num: Long): Unit = {
    log(s"numClasses=$num")
  }

  def logSuccess(model: Model[_]): Unit = {
    log(s"training finished")
  }
}

/**
 * Some common methods for logging information about a training session.
 */
private[ml] object Instrumentation {
  def create[E <: Estimator[_]](
      estimator: E, dataset: Dataset[_]): Instrumentation[E] = {
    new Instrumentation[E](estimator, dataset.rdd)
  }

  def create[E <: Estimator[_]](
      estimator: E, dataset: RDD[_]): Instrumentation[E] = {
    new Instrumentation[E](estimator, dataset)
  }

}