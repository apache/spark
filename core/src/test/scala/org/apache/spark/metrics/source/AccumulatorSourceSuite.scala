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

package org.apache.spark.metrics.source

import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, times, verify, when}

import org.apache.spark.{SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

class AccumulatorSourceSuite extends SparkFunSuite {
  test("that that accumulators register against the metric system's register") {
    val acc1 = new LongAccumulator()
    val acc2 = new LongAccumulator()
    val mockContext = mock(classOf[SparkContext])
    val mockEnvironment = mock(classOf[SparkEnv])
    val mockMetricSystem = mock(classOf[MetricsSystem])
    when(mockEnvironment.metricsSystem) thenReturn (mockMetricSystem)
    when(mockContext.env) thenReturn (mockEnvironment)
    val accs = Map("my-accumulator-1" -> acc1,
                   "my-accumulator-2" -> acc2)
    LongAccumulatorSource.register(mockContext, accs)
    val captor = ArgumentCaptor.forClass(classOf[AccumulatorSource])
    verify(mockMetricSystem, times(1)).registerSource(captor.capture())
    val source = captor.getValue()
    val gauges = source.metricRegistry.getGauges()
    assert (gauges.size == 2)
    assert (gauges.firstKey == "my-accumulator-1")
    assert (gauges.lastKey == "my-accumulator-2")
  }

  test("the accumulators value property is checked when the gauge's value is requested") {
    val acc1 = new LongAccumulator()
    acc1.add(123)
    val acc2 = new LongAccumulator()
    acc2.add(456)
    val mockContext = mock(classOf[SparkContext])
    val mockEnvironment = mock(classOf[SparkEnv])
    val mockMetricSystem = mock(classOf[MetricsSystem])
    when(mockEnvironment.metricsSystem) thenReturn (mockMetricSystem)
    when(mockContext.env) thenReturn (mockEnvironment)
    val accs = Map("my-accumulator-1" -> acc1,
                   "my-accumulator-2" -> acc2)
    LongAccumulatorSource.register(mockContext, accs)
    val captor = ArgumentCaptor.forClass(classOf[AccumulatorSource])
    verify(mockMetricSystem, times(1)).registerSource(captor.capture())
    val source = captor.getValue()
    val gauges = source.metricRegistry.getGauges()
    assert(gauges.get("my-accumulator-1").getValue() == 123)
    assert(gauges.get("my-accumulator-2").getValue() == 456)
  }

  test("the double accumulators value property is checked when the gauge's value is requested") {
    val acc1 = new DoubleAccumulator()
    acc1.add(123.123)
    val acc2 = new DoubleAccumulator()
    acc2.add(456.456)
    val mockContext = mock(classOf[SparkContext])
    val mockEnvironment = mock(classOf[SparkEnv])
    val mockMetricSystem = mock(classOf[MetricsSystem])
    when(mockEnvironment.metricsSystem) thenReturn (mockMetricSystem)
    when(mockContext.env) thenReturn (mockEnvironment)
    val accs = Map(
      "my-accumulator-1" -> acc1,
      "my-accumulator-2" -> acc2)
    DoubleAccumulatorSource.register(mockContext, accs)
    val captor = ArgumentCaptor.forClass(classOf[AccumulatorSource])
    verify(mockMetricSystem, times(1)).registerSource(captor.capture())
    val source = captor.getValue()
    val gauges = source.metricRegistry.getGauges()
    assert(gauges.get("my-accumulator-1").getValue() == 123.123)
    assert(gauges.get("my-accumulator-2").getValue() == 456.456)
  }
}
