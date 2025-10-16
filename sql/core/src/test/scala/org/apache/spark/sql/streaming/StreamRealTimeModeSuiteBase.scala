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

package org.apache.spark.sql.streaming

import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.execution.datasources.v2.LowLatencyClock
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.GlobalSingletonManualClock
import org.apache.spark.sql.test.TestSparkSession

/**
 * Base class for tests that require real-time mode.
 */
trait StreamRealTimeModeSuiteBase extends StreamTest with Matchers {
  override protected val defaultTrigger = RealTimeTrigger.apply("4 seconds")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION,
        defaultTrigger.batchDurationMs)
  }

  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]", // Ensure enough number of cores to ensure concurrent schedule of all tasks.
      "streaming-rtm-context",
      sparkConf.set("spark.sql.testkey", "true")))
}

abstract class StreamRealTimeModeManualClockSuiteBase extends StreamRealTimeModeSuiteBase {
  var clock = new GlobalSingletonManualClock()

  override def beforeAll(): Unit = {
    super.beforeAll()
    LowLatencyClock.setClock(clock)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    GlobalSingletonManualClock.reset()
  }

  val advanceRealTimeClock = new ExternalAction {
    override def runAction(): Unit = {
      clock.advance(defaultTrigger.batchDurationMs)
    }
  }
}

