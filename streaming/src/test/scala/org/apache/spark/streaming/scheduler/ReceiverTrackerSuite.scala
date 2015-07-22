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
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver._
import org.apache.spark.util.Utils
import org.apache.spark.streaming.dstream.InputDStream
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/** Testsuite for receiver scheduling */
class ReceiverTrackerSuite extends TestSuiteBase {
  val sparkConf = new SparkConf().setMaster("local[8]").setAppName("test")
  val ssc = new StreamingContext(sparkConf, Milliseconds(100))
  val tracker = new ReceiverTracker(ssc)
  val launcher = new tracker.ReceiverLauncher()
  val executors: List[String] = List("0", "1", "2", "3")

  test("receiver scheduling - all or none have preferred location") {

    def parse(s: String): Array[Array[String]] = {
      val outerSplit = s.split("\\|")
      val loc = new Array[Array[String]](outerSplit.length)
      var i = 0
      for (i <- 0 until outerSplit.length) {
        loc(i) = outerSplit(i).split("\\,")
      }
      loc
    }

    def testScheduler(numReceivers: Int, preferredLocation: Boolean, allocation: String) {
      val receivers =
        if (preferredLocation) {
          Array.tabulate(numReceivers)(i => new DummyReceiver(host =
            Some(((i + 1) % executors.length).toString)))
        } else {
          Array.tabulate(numReceivers)(_ => new DummyReceiver)
        }
      val locations = launcher.scheduleReceivers(receivers, executors)
      val expectedLocations = parse(allocation)
      assert(locations.deep === expectedLocations.deep)
    }

    testScheduler(numReceivers = 5, preferredLocation = false, allocation = "0|1|2|3|0")
    testScheduler(numReceivers = 3, preferredLocation = false, allocation = "0,3|1|2")
    testScheduler(numReceivers = 4, preferredLocation = true, allocation = "1|2|3|0")
  }

  test("receiver scheduling - some have preferred location") {
    val numReceivers = 4;
    val receivers: Seq[Receiver[_]] = Seq(new DummyReceiver(host = Some("1")),
      new DummyReceiver, new DummyReceiver, new DummyReceiver)
    val locations = launcher.scheduleReceivers(receivers, executors)
    assert(locations(0)(0) === "1")
    assert(locations(1)(0) === "0")
    assert(locations(2)(0) === "1")
    assert(locations(0).length === 1)
    assert(locations(3).length === 1)
  }

  test("Receiver tracker - propagates rate limit") {
    object ReceiverStartedWaiter extends StreamingListener {
      @volatile
      var started = false

      override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
        started = true
      }
    }

    ssc.addStreamingListener(ReceiverStartedWaiter)
    ssc.scheduler.listenerBus.start(ssc.sc)

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

/** An input DStream with a hard-coded receiver that gives access to internals for testing. */
private class RateLimitInputDStream(@transient ssc_ : StreamingContext)
  extends ReceiverInputDStream[Int](ssc_) {

  override def getReceiver(): DummyReceiver = SingletonDummyReceiver

  def getCurrentRateLimit: Option[Long] = {
    invokeExecutorMethod.getCurrentRateLimit
  }

  private def invokeExecutorMethod: ReceiverSupervisor = {
    val c = classOf[Receiver[_]]
    val ex = c.getDeclaredMethod("executor")
    ex.setAccessible(true)
    ex.invoke(SingletonDummyReceiver).asInstanceOf[ReceiverSupervisor]
  }
}

/**
 * A Receiver as an object so we can read its rate limit.
 *
 * @note It's necessary to be a top-level object, or else serialization would create another
 *       one on the executor side and we won't be able to read its rate limit.
 */
private object SingletonDummyReceiver extends DummyReceiver

/**
 * Dummy receiver implementation
 */
private class DummyReceiver(host: Option[String] = None)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  def onStart() {
  }

  def onStop() {
  }

  override def preferredLocation: Option[String] = host
}
