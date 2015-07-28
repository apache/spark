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

  override def actuallyWait: Boolean = true

  test("rate controller publishes updates") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val dstream = new MockRateLimitDStream(ssc, Seq(Seq(1)), 1)
      val output = new TestOutputStreamWithPartitions(dstream)
      output.register()
      runStreams(ssc, 1, 1)

      eventually(timeout(2.seconds)) {
        assert(dstream.publishCalls === 1)
      }
    }
  }

  test("receiver rate controller updates reach receivers") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val dstream = new RateLimitInputDStream(ssc) {
        override val rateController =
          Some(new ReceiverRateController(id, new ConstantEstimator(200.0)))
      }
      SingletonDummyReceiver.reset()

      val output = new TestOutputStreamWithPartitions(dstream)
      output.register()
      runStreams(ssc, 2, 2)

      eventually(timeout(5.seconds)) {
        assert(dstream.getCurrentRateLimit === Some(200))
      }
    }
  }

  test("multiple rate controller updates reach receivers") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val rates = Seq(100L, 200L, 300L)

      val dstream = new RateLimitInputDStream(ssc) {
        override val rateController =
          Some(new ReceiverRateController(id, new ConstantEstimator(rates.map(_.toDouble): _*)))
      }
      SingletonDummyReceiver.reset()

      val output = new TestOutputStreamWithPartitions(dstream)
      output.register()

      val observedRates = mutable.HashSet.empty[Long]

      @volatile var done = false
      runInBackground {
        while (!done) {
          try {
            dstream.getCurrentRateLimit.foreach(observedRates += _)
          } catch {
            case NonFatal(_) => () // don't stop if the executor wasn't installed yet
          }
          Thread.sleep(20)
        }
      }
      runStreams(ssc, 4, 4)
      done = true

      // Long.MaxValue (essentially, no rate limit) is the initial rate limit for any Receiver
      observedRates should contain theSameElementsAs (rates :+ Long.MaxValue)
    }
  }

  private def runInBackground(f: => Unit): Unit = {
    new Thread {
      override def run(): Unit = {
        f
      }
    }.start()
  }
}

/**
 * An InputDStream that counts how often its rate controller `publish` method was called.
 */
private class MockRateLimitDStream[T: ClassTag](
    @transient ssc: StreamingContext,
    input: Seq[Seq[T]],
    numPartitions: Int) extends TestInputStream[T](ssc, input, numPartitions) {

  @volatile
  var publishCalls = 0

  override val rateController: Option[RateController] =
    Some(new RateController(id, new ConstantEstimator(100.0)) {
      override def publish(rate: Long): Unit = {
        publishCalls += 1
      }
    })
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
