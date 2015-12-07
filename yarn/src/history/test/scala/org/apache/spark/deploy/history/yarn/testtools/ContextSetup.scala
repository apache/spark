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

package org.apache.spark.deploy.history.yarn.testtools

import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Trait implemented by everything setting up a context; the model is that
 * the traits can be chained, with the final state determined by the order
 *
 * 1. base implementation does nothing.
 * 2. subclass traits are expected to call the superclass first, then
 *   apply their own options.
 */
trait ContextSetup {

  def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    sparkConf
  }
}

trait TimelineServiceEnabled extends ContextSetup {
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    hadoopOpt(super.setupConfiguration(sparkConf),
               YarnConfiguration.TIMELINE_SERVICE_ENABLED, "true")
  }
}

trait TimelineServiceDisabled extends ContextSetup {
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    hadoopOpt(super.setupConfiguration(sparkConf),
               YarnConfiguration.TIMELINE_SERVICE_ENABLED, "false")
  }
}

/**
 * Add the timeline options
 */
trait TimelineOptionsInContext extends ContextSetup {
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    YarnTestUtils.addBasicTimelineOptions(super.setupConfiguration(sparkConf))
  }
}

/**
 * request that created history services register with the spark context for lifecycle events
 */
trait HistoryServiceListeningToSparkContext extends ContextSetup {
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf).set(YarnHistoryService.REGISTER_LISTENER, "true")
  }
}

/**
 * request that created history services are not registered with the spark context for
 * lifecycle events
 */
trait HistoryServiceNotListeningToSparkContext extends ContextSetup {
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf).set(YarnHistoryService.REGISTER_LISTENER, "false")
  }
}

/**
 * Switch to single entry batch sizes
 */
trait TimelineSingleEntryBatchSize extends ContextSetup {
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf).set(YarnHistoryService.BATCH_SIZE, "1")
  }

}
