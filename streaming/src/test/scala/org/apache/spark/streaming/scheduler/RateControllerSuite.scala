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

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.rate.RateEstimator

class RateControllerSuite extends TestSuiteBase {

  override def useManualClock: Boolean = false

  test("rate controller publishes updates") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val dstream = new RateLimitInputDStream(ssc)
      dstream.register()
      ssc.start()

      eventually(timeout(10.seconds)) {
        assert(dstream.publishCalls > 0)
      }
    }
  }

  test("publish rates reach receivers") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val dstream = new RateLimitInputDStream(ssc) {
        override val rateController =
          Some(new ReceiverRateController(id, new ConstantEstimator(200.0)))
      }
      dstream.register()
      SingletonTestRateReceiver.reset()
      ssc.start()

      eventually(timeout(10.seconds)) {
        assert(dstream.getCurrentRateLimit === Some(200))
      }
    }
  }

  test("multiple publish rates reach receivers") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val rates = Seq(100L, 200L, 300L)

      val dstream = new RateLimitInputDStream(ssc) {
        override val rateController =
          Some(new ReceiverRateController(id, new ConstantEstimator(rates.map(_.toDouble): _*)))
      }
      SingletonTestRateReceiver.reset()
      dstream.register()

      val observedRates = mutable.HashSet.empty[Long]
      ssc.start()

      eventually(timeout(20.seconds)) {
        dstream.getCurrentRateLimit.foreach(observedRates += _)
        // Long.MaxValue (essentially, no rate limit) is the initial rate limit for any Receiver
        observedRates should contain theSameElementsAs (rates :+ Long.MaxValue)
      }
    }
  }
}

private[streaming] class ConstantEstimator(rates: Double*) extends RateEstimator {
  private var idx: Int = 0

  private def nextRate(): Double = {
    val rate = rates(idx)
    idx = (idx + 1) % rates.size
    rate
  }

  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double] = Some(nextRate())
}
