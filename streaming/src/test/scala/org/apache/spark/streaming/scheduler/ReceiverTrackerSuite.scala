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

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.storage.StorageLevel

/** Testsuite for receiver scheduling */
class ReceiverTrackerSuite extends TestSuiteBase {
  val sparkConf = new SparkConf().setMaster("local[8]").setAppName("test")
  val ssc = new StreamingContext(sparkConf, Milliseconds(100))

  ignore("Receiver tracker - propagates rate limit") {
    object ReceiverStartedWaiter extends StreamingListener {
      @volatile
      var started = false

      override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
        started = true
      }
    }

    ssc.addStreamingListener(ReceiverStartedWaiter)
    ssc.scheduler.listenerBus.start(ssc.sc)
    SingletonTestRateReceiver.reset()

    val newRateLimit = 100L
    val inputDStream = new RateLimitInputDStream(ssc)
    val tracker = new ReceiverTracker(ssc)
    tracker.start()

    // we wait until the Receiver has registered with the tracker,
    // otherwise our rate update is lost
    eventually(timeout(5 seconds)) {
      assert(ReceiverStartedWaiter.started)
    }
    tracker.sendRateUpdate(inputDStream.id, newRateLimit)
    // this is an async message, we need to wait a bit for it to be processed
    eventually(timeout(3 seconds)) {
      assert(inputDStream.getCurrentRateLimit.get === newRateLimit)
    }
  }
}

/**
 * An input DStream with a hard-coded receiver that gives access to internals for testing.
 *
 * @note Make sure to call {{{SingletonDummyReceiver.reset()}}} before using this in a test,
 *       or otherwise you may get {{{NotSerializableException}}} when trying to serialize
 *       the receiver.
 * @see [[[SingletonDummyReceiver]]].
 */
private[streaming] class RateLimitInputDStream(@transient ssc_ : StreamingContext)
  extends ReceiverInputDStream[Int](ssc_) {

  override def getReceiver(): RateTestReceiver = SingletonTestRateReceiver

  def getCurrentRateLimit: Option[Long] = {
    invokeExecutorMethod.getCurrentRateLimit
  }

  @volatile
  var publishCalls = 0

  override val rateController: Option[RateController] = {
    Some(new RateController(id, new ConstantEstimator(100.0)) {
      override def publish(rate: Long): Unit = {
        publishCalls += 1
      }
    })
  }

  private def invokeExecutorMethod: ReceiverSupervisor = {
    val c = classOf[Receiver[_]]
    val ex = c.getDeclaredMethod("executor")
    ex.setAccessible(true)
    ex.invoke(SingletonTestRateReceiver).asInstanceOf[ReceiverSupervisor]
  }
}

/**
 * A Receiver as an object so we can read its rate limit. Make sure to call `reset()` when
 * reusing this receiver, otherwise a non-null `executor_` field will prevent it from being
 * serialized when receivers are installed on executors.
 *
 * @note It's necessary to be a top-level object, or else serialization would create another
 *       one on the executor side and we won't be able to read its rate limit.
 */
private[streaming] object SingletonTestRateReceiver extends RateTestReceiver(0) {

  /** Reset the object to be usable in another test. */
  def reset(): Unit = {
    executor_ = null
  }
}

/**
 * Dummy receiver implementation
 */
private[streaming] class RateTestReceiver(receiverId: Int, host: Option[String] = None)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  setReceiverId(receiverId)

  override def onStart(): Unit = {}

  override def onStop(): Unit = {}

  override def preferredLocation: Option[String] = host
}
