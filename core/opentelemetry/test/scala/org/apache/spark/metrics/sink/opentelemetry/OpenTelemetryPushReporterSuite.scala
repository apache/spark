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

package org.apache.spark.metrics.sink.opentelemetry

import com.codahale.metrics._
import org.junit.jupiter.api.Assertions.assertNotNull
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite

class OpenTelemetryPushReporterSuite
  extends SparkFunSuite with PrivateMethodTester {
  val reporter = new OpenTelemetryPushReporter(
    registry = new MetricRegistry(),
    trustedCertificatesPath = null,
    privateKeyPemPath = null,
    certificatePemPath = null
  )

  test("Normalize metric name key") {
    val name = "local-1592132938718.driver.LiveListenerBus." +
      "listenerProcessingTime.org.apache.spark.HeartbeatReceiver"
    val metricsName = reporter invokePrivate PrivateMethod[String](
        Symbol("normalizeMetricName")
      )(name)
    assert(
      metricsName == "local_1592132938718_driver_livelistenerbus_" +
        "listenerprocessingtime_org_apache_spark_heartbeatreceiver"
    )
  }

  test("OpenTelemetry actions when one codahale gauge is added") {
    val gauge = new Gauge[Double] {
      override def getValue: Double = 1.23
    }
    reporter.onGaugeAdded("test-gauge", gauge)
    assertNotNull(reporter.openTelemetryGauges("test_gauge"))
  }

  test("OpenTelemetry actions when one codahale counter is added") {
    val counter = new Counter
    reporter.onCounterAdded("test_counter", counter)
    assertNotNull(reporter.openTelemetryCounters("test_counter"))
  }

  test("OpenTelemetry actions when one codahale histogram is added") {
    val histogram = new Histogram(new UniformReservoir)
    reporter.onHistogramAdded("test_hist", histogram)
    assertNotNull(reporter.openTelemetryHistograms("test_hist_count"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_max"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_min"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_mean"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_std_dev"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_50_percentile"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_75_percentile"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_95_percentile"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_98_percentile"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_99_percentile"))
    assertNotNull(reporter.openTelemetryHistograms("test_hist_999_percentile"))
  }

  test("OpenTelemetry actions when one codahale meter is added") {
    val meter = new Meter()
    reporter.onMeterAdded("test_meter", meter)
    assertNotNull(reporter.openTelemetryGauges("test_meter_meter_count"))
    assertNotNull(reporter.openTelemetryGauges("test_meter_meter_mean_rate"))
    assertNotNull(
      reporter.openTelemetryGauges("test_meter_meter_one_minute_rate")
    )
    assertNotNull(
      reporter.openTelemetryGauges("test_meter_meter_five_minute_rate")
    )
    assertNotNull(
      reporter.openTelemetryGauges("test_meter_meter_fifteen_minute_rate")
    )
  }

  test("OpenTelemetry actions when one codahale timer is added") {
    val timer = new Timer()
    reporter.onTimerAdded("test_timer", timer)
    assertNotNull(reporter.openTelemetryHistograms("test_timer_timer_count"))
    assertNotNull(reporter.openTelemetryHistograms("test_timer_timer_max"))
    assertNotNull(reporter.openTelemetryHistograms("test_timer_timer_min"))
    assertNotNull(reporter.openTelemetryHistograms("test_timer_timer_mean"))
    assertNotNull(reporter.openTelemetryHistograms("test_timer_timer_std_dev"))
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_50_percentile")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_75_percentile")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_95_percentile")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_95_percentile")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_99_percentile")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_999_percentile")
    )

    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_fifteen_minute_rate")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_five_minute_rate")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_one_minute_rate")
    )
    assertNotNull(
      reporter.openTelemetryHistograms("test_timer_timer_mean_rate")
    )
  }
}
