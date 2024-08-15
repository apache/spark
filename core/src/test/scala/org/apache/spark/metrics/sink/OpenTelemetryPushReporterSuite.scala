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

package org.apache.spark.metrics.sink

import com.codahale.metrics.MetricRegistry
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite

class OpenTelemetryPushReporterSuite
    extends SparkFunSuite
    with PrivateMethodTester {
  test("normalize metric name key") {
    val reporter = new OpenTelemetryPushReporter(
      registry = new MetricRegistry(),
      trustedCertificatesPath = null,
      privateKeyPemPath = null,
      certificatePemPath = null
    )
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
}
