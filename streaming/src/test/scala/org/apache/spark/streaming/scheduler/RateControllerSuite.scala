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

package org.apache.spark.streaming.scheduler

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, TestOutputStreamWithPartitions, TestSuiteBase, Time}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.rate.RateEstimator

class RateControllerSuite extends TestSuiteBase {

  test("rate controller publishes updates") {
    val ssc = new StreamingContext(conf, batchDuration)
    val dstream = new MockRateLimitDStream(ssc)
    val output = new TestOutputStreamWithPartitions(dstream)
    output.register()
    runStreams(ssc, 1, 1)

    eventually(timeout(2.seconds)) {
      assert(dstream.publishCalls === 1)
    }
  }
}

/**
 * An InputDStream that counts how often its rate controller `publish` method was called.
 */
private class MockRateLimitDStream(@transient ssc: StreamingContext)
    extends InputDStream[Int](ssc) {

  @volatile
  var publishCalls = 0

  private object ConstantEstimator extends RateEstimator {
    def compute(
        time: Long,
        elements: Long,
        processingDelay: Long,
        schedulingDelay: Long): Option[Double] = {
      Some(100.0)
    }
  }

  override val rateController: RateController = new RateController(id, ConstantEstimator) {
    override def publish(rate: Long): Unit = {
      publishCalls += 1
    }
  }

  def compute(validTime: Time): Option[RDD[Int]] = {
    val data = Seq(1)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, StreamInputInfo(id, data.size))
    Some(ssc.sc.parallelize(data))
  }

  def stop(): Unit = {}

  def start(): Unit = {}
}
