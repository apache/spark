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
package org.apache.spark.network.yarn

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.metrics2.{MetricsInfo, MetricsRecordBuilder}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyInt, anyLong}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.server.OneForOneStreamManager
import org.apache.spark.network.shuffle.{ExternalBlockHandler, ExternalShuffleBlockResolver}

class YarnShuffleServiceMetricsSuite extends SparkFunSuite with Matchers {

  val streamManager = mock(classOf[OneForOneStreamManager])
  val blockResolver = mock(classOf[ExternalShuffleBlockResolver])
  when(blockResolver.getRegisteredExecutorsSize).thenReturn(42)

  val metrics = new ExternalBlockHandler(streamManager, blockResolver).getAllMetrics

  test("metrics named as expected") {
    val allMetrics = Seq(
      "openBlockRequestLatencyMillis", "registerExecutorRequestLatencyMillis",
      "blockTransferRate", "blockTransferMessageRate", "blockTransferAvgSize_1min",
      "blockTransferRateBytes", "registeredExecutorsSize", "numActiveConnections",
      "numCaughtExceptions", "finalizeShuffleMergeLatencyMillis",
      "fetchMergedBlocksMetaLatencyMillis")

    // Use sorted Seq instead of Set for easier comparison when there is a mismatch
    metrics.getMetrics.keySet().asScala.toSeq.sorted should be (allMetrics.sorted)
  }

  // these metrics will generate more metrics on the collector
  for (testname <- Seq("openBlockRequestLatencyMillis",
      "registerExecutorRequestLatencyMillis",
      "blockTransferRateBytes", "blockTransferRate", "blockTransferMessageRate")) {
    test(s"$testname - collector receives correct types") {
      val builder = mock(classOf[MetricsRecordBuilder])
      val counterNames = mutable.Buffer[String]()
      when(builder.addCounter(any(), anyLong())).thenAnswer(iom => {
        counterNames += iom.getArgument[MetricsInfo](0).name()
        builder
      })
      val gaugeLongNames = mutable.Buffer[String]()
      when(builder.addGauge(any(), anyLong())).thenAnswer(iom => {
        gaugeLongNames += iom.getArgument[MetricsInfo](0).name()
        builder
      })
      val gaugeDoubleNames = mutable.Buffer[String]()
      when(builder.addGauge(any(), anyDouble())).thenAnswer(iom => {
        gaugeDoubleNames += iom.getArgument[MetricsInfo](0).name()
        builder
      })

      YarnShuffleServiceMetrics.collectMetric(builder, testname,
        metrics.getMetrics.get(testname))

      assert(counterNames === Seq(s"${testname}_count"))
      val (expectLong, expectDouble) =
        if (testname.matches("blockTransfer(Message)?Rate(Bytes)?$")) {
          // blockTransfer(Message)?Rate(Bytes)? metrics are Meter so just have rate information
          (Seq(), Seq("1", "5", "15", "Mean").map(suffix => s"${testname}_rate$suffix"))
        } else {
          // other metrics are Timer so have rate and timing information
          (
              Seq(s"${testname}_nanos_max", s"${testname}_nanos_min"),
              Seq("rate1", "rate5", "rate15", "rateMean", "nanos_mean", "nanos_stdDev",
                "nanos_1stPercentile", "nanos_5thPercentile", "nanos_25thPercentile",
                "nanos_50thPercentile", "nanos_75thPercentile", "nanos_95thPercentile",
                "nanos_98thPercentile", "nanos_99thPercentile", "nanos_999thPercentile")
                  .map(suffix => s"${testname}_$suffix")
          )
        }
      assert(gaugeLongNames.sorted === expectLong.sorted)
      assert(gaugeDoubleNames.sorted === expectDouble.sorted)
    }
  }

  // this metric writes only one gauge to the collector
  test("registeredExecutorsSize - collector receives correct types") {
    val builder = mock(classOf[MetricsRecordBuilder])

    YarnShuffleServiceMetrics.collectMetric(builder, "registeredExecutorsSize",
      metrics.getMetrics.get("registeredExecutorsSize"))

    // only one
    verify(builder).addGauge(any(), anyInt())
  }
}
