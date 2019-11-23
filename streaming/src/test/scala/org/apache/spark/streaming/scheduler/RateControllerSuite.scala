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

import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.rate.RateEstimator

class RateControllerSuite extends TestSuiteBase {

  override def useManualClock: Boolean = false

  override def batchDuration: Duration = Milliseconds(50)

  test("RateController - rate controller publishes updates after batches complete") {
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
      val dstream = new RateTestInputDStream(ssc)
      dstream.register()
      ssc.start()

      eventually(timeout(10.seconds)) {
        assert(dstream.publishedRates > 0)
      }
    }
  }

  test("ReceiverRateController - published rates reach receivers") {
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
      val estimator = new ConstantEstimator(100)
      val dstream = new RateTestInputDStream(ssc) {
        override val rateController =
          Some(new ReceiverRateController(id, estimator))
      }
      dstream.register()
      ssc.start()

      // Wait for receiver to start
      eventually(timeout(5.seconds)) {
        RateTestReceiver.getActive().nonEmpty
      }

      // Update rate in the estimator and verify whether the rate was published to the receiver
      def updateRateAndVerify(rate: Long): Unit = {
        estimator.updateRate(rate)
        eventually(timeout(5.seconds)) {
          assert(RateTestReceiver.getActive().get.getDefaultBlockGeneratorRateLimit() === rate)
        }
      }

      // Verify multiple rate update
      Seq(100, 200, 300).foreach { rate =>
        updateRateAndVerify(rate)
      }
    }
  }
}

private[streaming] class ConstantEstimator(@volatile private var rate: Long)
  extends RateEstimator {

  def updateRate(newRate: Long): Unit = {
    rate = newRate
  }

  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double] = Some(rate)
}
