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

import java.net.{DatagramPacket, DatagramSocket}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties
import java.util.concurrent.TimeUnit._

import scala.jdk.CollectionConverters._

import com.codahale.metrics._

import org.apache.spark.SparkFunSuite
import org.apache.spark.metrics.sink.StatsdSink._

class StatsdSinkSuite extends SparkFunSuite {
  private val defaultProps = Map(
    STATSD_KEY_PREFIX -> "spark",
    STATSD_KEY_PERIOD -> "1",
    STATSD_KEY_UNIT -> "seconds",
    STATSD_KEY_HOST -> "127.0.0.1"
  )
  // The maximum size of a single datagram packet payload. Payloads
  // larger than this will be truncated.
  private val maxPayloadSize = 256 // bytes

  // The receive buffer must be large enough to hold all inflight
  // packets. This includes any kernel and protocol overhead.
  // This value was determined experimentally and should be
  // increased if timeouts are seen.
  private val socketMinRecvBufferSize = 16384 // bytes
  private val socketTimeout = 30000           // milliseconds

  private def withSocketAndSink(testCode: (DatagramSocket, StatsdSink) => Any): Unit = {
    val socket = new DatagramSocket

    // Leave the receive buffer size untouched unless it is too
    // small. If the receive buffer is too small packets will be
    // silently dropped and receive operations will timeout.
    if (socket.getReceiveBufferSize() < socketMinRecvBufferSize) {
      socket.setReceiveBufferSize(socketMinRecvBufferSize)
    }

    socket.setSoTimeout(socketTimeout)
    val props = new Properties
    defaultProps.foreach(e => props.put(e._1, e._2))
    props.put(STATSD_KEY_PORT, socket.getLocalPort.toString)
    props.put(STATSD_KEY_REGEX, "counter|gauge|histogram|timer")
    val registry = new MetricRegistry
    val sink = new StatsdSink(props, registry)
    try {
      testCode(socket, sink)
    } finally {
      socket.close()
    }
  }

  test("metrics StatsD sink with Counter") {
    withSocketAndSink { (socket, sink) =>
      val counter = new Counter
      counter.inc(12)
      sink.registry.register("counter", counter)
      sink.report()

      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      socket.receive(p)

      val result = new String(p.getData, 0, p.getLength, UTF_8)
      assert(result === "spark.counter:12|c", "Counter metric received should match data sent")
    }
  }

  test("metrics StatsD sink with Gauge") {
    withSocketAndSink { (socket, sink) =>
      val gauge = new Gauge[Double] {
        override def getValue: Double = 1.23
      }
      sink.registry.register("gauge", gauge)
      sink.report()

      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      socket.receive(p)

      val result = new String(p.getData, 0, p.getLength, UTF_8)
      assert(result === "spark.gauge:1.23|g", "Gauge metric received should match data sent")
    }
  }

  test("metrics StatsD sink with Histogram") {
    withSocketAndSink { (socket, sink) =>
      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      val histogram = new Histogram(new UniformReservoir)
      histogram.update(10)
      histogram.update(20)
      histogram.update(30)
      sink.registry.register("histogram", histogram)
      sink.report()

      val expectedResults = Set(
        "spark.histogram.count:3|g",
        "spark.histogram.max:30|ms",
        "spark.histogram.mean:20.00|ms",
        "spark.histogram.min:10|ms",
        "spark.histogram.stddev:10.00|ms",
        "spark.histogram.p50:20.00|ms",
        "spark.histogram.p75:30.00|ms",
        "spark.histogram.p95:30.00|ms",
        "spark.histogram.p98:30.00|ms",
        "spark.histogram.p99:30.00|ms",
        "spark.histogram.p999:30.00|ms"
      )

      (1 to expectedResults.size).foreach { i =>
        socket.receive(p)
        val result = new String(p.getData, 0, p.getLength, UTF_8)
        logInfo(s"Received histogram result $i: '$result'")
        assert(expectedResults.contains(result),
          "Histogram metric received should match data sent")
      }
    }
  }

  test("metrics StatsD sink with Timer") {
    withSocketAndSink { (socket, sink) =>
      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      val timer = new Timer()
      timer.update(1, SECONDS)
      timer.update(2, SECONDS)
      timer.update(3, SECONDS)
      sink.registry.register("timer", timer)
      sink.report()

      val expectedResults = Set(
        "spark.timer.max:3000.00|ms",
        "spark.timer.mean:2000.00|ms",
        "spark.timer.min:1000.00|ms",
        "spark.timer.stddev:816.50|ms",
        "spark.timer.p50:2000.00|ms",
        "spark.timer.p75:3000.00|ms",
        "spark.timer.p95:3000.00|ms",
        "spark.timer.p98:3000.00|ms",
        "spark.timer.p99:3000.00|ms",
        "spark.timer.p999:3000.00|ms",
        "spark.timer.count:3|g",
        "spark.timer.m1_rate:0.00|ms",
        "spark.timer.m5_rate:0.00|ms",
        "spark.timer.m15_rate:0.00|ms"
      )
      // mean rate varies on each test run
      val oneMoreResult = """spark.timer.mean_rate:\d+\.\d\d\|ms"""

      (1 to (expectedResults.size + 1)).foreach { i =>
        socket.receive(p)
        val result = new String(p.getData, 0, p.getLength, UTF_8)
        logInfo(s"Received timer result $i: '$result'")
        assert(expectedResults.contains(result) || result.matches(oneMoreResult),
          "Timer metric received should match data sent")
      }
    }
  }


  test("metrics StatsD sink with filtered Gauge") {
    withSocketAndSink { (socket, sink) =>
      val gauge = new Gauge[Double] {
        override def getValue: Double = 1.23
      }

      val filteredMetricKeys = Set(
        "gauge",
        "gauge-1"
      )

      filteredMetricKeys.foreach(sink.registry.register(_, gauge))

      sink.registry.register("excluded-metric", gauge)
      sink.report()

      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      socket.receive(p)

      val metricKeys = sink.registry.getGauges(sink.filter).keySet.asScala

      assert(metricKeys.equals(filteredMetricKeys),
        "Should contain only metrics matches regex filter")
    }
  }
}

