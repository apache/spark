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

package org.apache.spark.deploy.history

import scala.collection.JavaConverters._

import com.codahale.metrics.{Counter, Timer}
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.ManualClock

class HistoryMetricSourceSuite extends SparkFunSuite with Matchers {

  test("LambdaLongGauge") {
    assert(3L === new LambdaLongGauge(() => 3L).getValue)
  }

  test("TimestampGauge lifecycle") {
    val clock = new ManualClock(1)
    val ts = new TimestampGauge(clock)
    assert (0L === ts.getValue)
    ts.touch()
    assert (1L === ts.getValue)
    clock.setTime(1000)
    assert (1L === ts.getValue)
    ts.touch()
    assert (1000 === ts.getValue)
  }

  test("HistoryMetricSource registration and lookup") {
    val threeGauge = new LambdaLongGauge(() => 3L)
    val clock = new ManualClock(1)
    val ts = new TimestampGauge(clock)
    val timer = new Timer
    val counter = new Counter()
    val source = new TestMetricSource
    source.register(Seq(
      ("three", threeGauge),
      ("timestamp", ts),
      ("timer", timer),
      ("counter", counter)))
    logInfo(source.toString)

    val registry = source.metricRegistry
    val counters = registry.getCounters.asScala
    counters.size should be (1)
    assert(counter === counters("t.counter"))
    assert(counter === source.getCounter("counter").get)

    val gauges = registry.getGauges.asScala
    gauges.size should be(2)

    assert(ts === source.getLongGauge("timestamp").get)
    assert(threeGauge === source.getLongGauge("three").get)
    assert(timer === source.getTimer("timer").get)

  }

  test("Handle failing Gauge.getValue in toString()") {
    var zero = 0L
    val trouble = new LambdaLongGauge(() => 1L/zero)
    intercept[ArithmeticException](trouble.getValue)
    val source = new TestMetricSource
    source.register(Seq(("trouble", trouble)))
    val s = source.toString
    logInfo(s)
    s should include("ArithmeticException")
  }

  private class TestMetricSource extends HistoryMetricSource("t") {
    override def sourceName: String = "TestMetricSource"
  }
}
