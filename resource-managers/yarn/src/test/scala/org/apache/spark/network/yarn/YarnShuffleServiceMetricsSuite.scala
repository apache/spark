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

import org.apache.hadoop.metrics2.MetricsRecordBuilder
import org.mockito.ArgumentMatchers.{any, anyDouble, anyInt, anyLong}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.server.OneForOneStreamManager
import org.apache.spark.network.shuffle.{ExternalShuffleBlockHandler, ExternalShuffleBlockResolver}

class YarnShuffleServiceMetricsSuite extends SparkFunSuite with Matchers {

  val streamManager = mock(classOf[OneForOneStreamManager])
  val blockResolver = mock(classOf[ExternalShuffleBlockResolver])
  when(blockResolver.getRegisteredExecutorsSize).thenReturn(42)

  val metrics = new ExternalShuffleBlockHandler(streamManager, blockResolver).getAllMetrics

  test("metrics named as expected") {
    val allMetrics = Set(
      "openBlockRequestLatencyMillis", "registerExecutorRequestLatencyMillis",
      "blockTransferRateBytes", "registeredExecutorsSize", "numActiveConnections",
      "numRegisteredConnections")

    metrics.getMetrics.keySet().asScala should be (allMetrics)
  }

  // these three metrics have the same effect on the collector
  for (testname <- Seq("openBlockRequestLatencyMillis",
      "registerExecutorRequestLatencyMillis",
      "blockTransferRateBytes")) {
    test(s"$testname - collector receives correct types") {
      val builder = mock(classOf[MetricsRecordBuilder])
      when(builder.addCounter(any(), anyLong())).thenReturn(builder)
      when(builder.addGauge(any(), anyDouble())).thenReturn(builder)

      YarnShuffleServiceMetrics.collectMetric(builder, testname,
        metrics.getMetrics.get(testname))

      verify(builder).addCounter(any(), anyLong())
      verify(builder, times(4)).addGauge(any(), anyDouble())
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
