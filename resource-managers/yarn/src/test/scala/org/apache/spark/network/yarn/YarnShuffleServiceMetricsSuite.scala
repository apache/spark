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
      val rates = Seq("rate1", "rate5", "rate15", "rateMean")
      val percentiles =
        "1stPercentile" +: Seq(5, 25, 50, 75, 95, 98, 99, 999).map(_ + "thPercentile")
      val (expectLong, expectDouble) =
        if (testname.matches("blockTransfer(Message)?Rate(Bytes)?$")) {
          // blockTransfer(Message)?Rate(Bytes)? metrics are Meter so just have rate information
          (Seq(), rates)
        } else {
          // other metrics are Timer so have rate and timing information
          (Seq("max", "min"), rates ++ Seq("mean", "stdDev") ++ percentiles)
        }
      assert(gaugeLongNames.sorted === expectLong.map(testname + "_" + _).sorted)
      assert(gaugeDoubleNames.sorted === expectDouble.map(testname + "_" + _).sorted)
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
