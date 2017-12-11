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

package org.apache.spark.sql.streaming.continuous

import java.io.{File, InterruptedIOException, IOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeoutException, TimeUnit}

import scala.reflect.ClassTag
import scala.util.control.ControlThrowable

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class ContinuousSuite extends StreamTest {
  import testImplicits._

  private def waitForRateSourceTriggers(query: StreamExecution, numTriggers: Int): Unit = {
    query match {
      case s: ContinuousExecution =>
        s.awaitInitialization(streamingTimeout.toMillis)
        Thread.sleep(5000)
        val reader = s.lastExecution.executedPlan.collectFirst {
          case DataSourceV2ScanExec(_, r: ContinuousRateStreamReader) => r
        }.get

        while (System.currentTimeMillis < reader.lastStartTime + 9100) {
          Thread.sleep(reader.lastStartTime + 9100 - System.currentTimeMillis)
        }
    }
  }

  private def incrementEpoch(query: StreamExecution): Unit = {
    query match {
      case s: ContinuousExecution =>
        EpochCoordinatorRef.get(s.id.toString, SparkEnv.get)
          .askSync[Long](IncrementAndGetEpoch())
        Thread.sleep(200)
    }
  }

  test("basic rate source") {
    val df = spark.readStream.format("rate").load().select('value)

    // TODO: validate against low trigger interval
    testStream(df, useV2Sink = true)(
      StartStream(Trigger.Continuous(1000000)),
      Execute(waitForRateSourceTriggers(_, 10)),
      Execute(incrementEpoch),
      CheckAnswer(scala.Range(0, 50): _*),
      StopStream,
      StartStream(Trigger.Continuous(1000000)),
      Execute(waitForRateSourceTriggers(_, 10)),
      Execute(incrementEpoch),
      CheckAnswer(scala.Range(0, 100): _*))
  }

  /* test("repeatedly restart") {
    val df = spark.readStream.format("rate").option("continuous", "true").load().select('value)

    // TODO: validate against low trigger interval
    testStream(df)(
      StartStream(Trigger.Continuous("1 second")),
      Execute(_ => Thread.sleep(3000)),
      CheckAnswer(scala.Range(0, 10): _*),
      StopStream,
      StartStream(Trigger.Continuous("1 second")),
      StopStream,
      StartStream(Trigger.Continuous("1 second")),
      StopStream,
      StartStream(Trigger.Continuous("1 second")),
      Execute(_ => Thread.sleep(3000)),
      CheckAnswer(scala.Range(0, 20): _*))
  } */

  test("query without test harness") {
    val df = spark.readStream.format("rate").load().select('value)
    val query = df.writeStream
      .format("memory")
      .queryName("noharness")
      .trigger(Trigger.Continuous(1000))
      .start()
    waitForRateSourceTriggers(query.asInstanceOf[StreamingQueryWrapper].streamingQuery, 0)
    query.stop()

    val results = spark.read.table("noharness").collect()
    assert(!results.isEmpty)
  }
}
