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

import java.util.concurrent.{TimeUnit, CountDownLatch}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListener}
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver._

/** Testsuite for receiver scheduling */
class ReceiverTrackerSuite extends TestSuiteBase {

  test("send rate update to receivers") {
    withStreamingContext(new StreamingContext(conf, Milliseconds(100))) { ssc =>
      ssc.scheduler.listenerBus.start(ssc.sc)

      val newRateLimit = 100L
      val inputDStream = new RateTestInputDStream(ssc)
      val tracker = new ReceiverTracker(ssc)
      tracker.start()
      try {
        // we wait until the Receiver has registered with the tracker,
        // otherwise our rate update is lost
        eventually(timeout(5 seconds)) {
          assert(RateTestReceiver.getActive().nonEmpty)
        }


        // Verify that the rate of the block generator in the receiver get updated
        val activeReceiver = RateTestReceiver.getActive().get
        tracker.sendRateUpdate(inputDStream.id, newRateLimit)
        eventually(timeout(5 seconds)) {
          assert(activeReceiver.getDefaultBlockGeneratorRateLimit() === newRateLimit,
            "default block generator did not receive rate update")
          assert(activeReceiver.getCustomBlockGeneratorRateLimit() === newRateLimit,
            "other block generator did not receive rate update")
        }
      } finally {
        tracker.stop(false)
      }
    }
  }

  test("receiver timeout") {
    val conf = new SparkConf().
      setMaster("local[1]").
      setAppName("test").
      set("spark.streaming.receiver.launching.timeout", "10ms")
    withStreamingContext(new StreamingContext(conf, Milliseconds(100))) { ssc =>
      val input1 = ssc.receiverStream(new TestReceiver)
      val input2 = ssc.receiverStream(new TestReceiver)
      input1.union(input2).foreachRDD { rdd =>
        rdd.foreach { v =>
          // We don't care about the output
        }
      }
      ssc.start()
      // We have 2 receivers but only 1 executor, so ReceiverTracker should stop StreamingContext
      // after 10ms.
      eventually(timeout(30.seconds), interval(100.millis)) {
        assert(ssc.getState() === StreamingContextState.STOPPED)
      }
    }
  }

  test("restart receiver timeout") {
    // This test is testing that when we restart a receiver, but it cannot start in time because
    // there is no available executors (Such as executors have been occupied by long running jobs,
    // or many executors are dead), we should stop StreamingContext.
    val conf = new SparkConf().
      setMaster("local[1]").
      setAppName("test").
      // 10 seconds should be enough to start a receiver and also we don't need to wait a lot of
      // time in this test.
      set("spark.streaming.receiver.launching.timeout", "10s")
    withStreamingContext(new StreamingContext(conf, Milliseconds(100))) { ssc =>
      CancellableTestReceiver.reset()
      ssc.receiverStream(new CancellableTestReceiver).foreachRDD { rdd =>
        rdd.foreach { v =>
          // We don't care about the output
        }
      }
      val receiverCount = new AtomicLong(0)
      ssc.addStreamingListener(new StreamingListener {
        override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
          receiverCount.incrementAndGet()
        }
      })
      ssc.start()
      // Wait until the receiver start
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(receiverCount.get === 1)
      }

      val jobStarted = new CountDownLatch(1)
      // Use a special property so as that we can ignore other jobs
      ssc.sparkContext.setLocalProperty("this-is-a-long-running-job", "true")
      ssc.sparkContext.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          if (jobStart.properties.getProperty("this-is-a-long-running-job") != null) {
            jobStarted.countDown()
          }
        }
      })
      // Submit a long running job
      ssc.sparkContext.parallelize(1 to 1).foreachAsync { _ =>
        val startTime = System.currentTimeMillis()
        while (!RestartReceiverTimeoutHelper.shouldExitJob &&
          (System.currentTimeMillis() - startTime) < 30000) { // 30 seconds timeout
          Thread.sleep(10)
        }
      }
      // Make sure the Spark job has been started. Note: although it has been started, but all tasks
      // should be waiting for idle executors.
      jobStarted.await(10, TimeUnit.SECONDS)

      // Cancel the receiver so that ReceiverTracker can restart it and release the executor for
      // the long running Spark job
      CancellableTestReceiver.cancel()
      // However, because there is only one executor and it's occupied by the running job, the
      // receiver should not be able to start in 10 seconds. Then ReceiverTracker should stop
      // StreamingContext.
      eventually(timeout(30.seconds), interval(100.millis)) {
        assert(ssc.scheduler.receiverTracker.isTrackerStarted === false)
        // Note: after checking "ssc.scheduler.receiverTracker.isTrackerStarted", we can cancel
        // the Spark job now. We should cancel it before checking "ssc.getState()", because
        // "ssc.getState()" won't be changed to STOPPED until SparkContext has been stopped.
        // Otherwise, we need more time for this test case.
        RestartReceiverTimeoutHelper.exitJob()
        assert(ssc.getState() === StreamingContextState.STOPPED)
      }
    }
  }
}

/**
 * A global helper class to control if we should exit the spark job in "restart receiver timeout"
 */
private[streaming] object RestartReceiverTimeoutHelper {

  private var _exitJob = false

  def shouldExitJob: Boolean = synchronized {
    _exitJob
  }

  def exitJob(): Unit = synchronized {
    _exitJob = true
  }

  def reset(): Unit = synchronized {
    _exitJob = false
  }
}

private[streaming] class CancellableTestReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  def onStart() {
    val thread = new Thread() {
      override def run() {
        while (!CancellableTestReceiver.isCancelled) {
          Thread.sleep(10)
        }
        CancellableTestReceiver.this.stop("Cancelled")
      }
    }
    thread.start()
  }

  def onStop() {
    CancellableTestReceiver.cancel()
  }
}

private[streaming] object CancellableTestReceiver {

  private var cancelled = false

  def isCancelled: Boolean = synchronized {
    cancelled
  }

  def cancel(): Unit = synchronized {
    cancelled = true
  }

  def reset(): Unit = synchronized {
    cancelled = false
  }
}

/** An input DStream with for testing rate controlling */
private[streaming] class RateTestInputDStream(@transient ssc_ : StreamingContext)
  extends ReceiverInputDStream[Int](ssc_) {

  override def getReceiver(): Receiver[Int] = new RateTestReceiver(id)

  @volatile
  var publishedRates = 0

  override val rateController: Option[RateController] = {
    Some(new RateController(id, new ConstantEstimator(100)) {
      override def publish(rate: Long): Unit = {
        publishedRates += 1
      }
    })
  }
}

/** A receiver implementation for testing rate controlling */
private[streaming] class RateTestReceiver(receiverId: Int, host: Option[String] = None)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  private lazy val customBlockGenerator = supervisor.createBlockGenerator(
    new BlockGeneratorListener {
      override def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {}
      override def onError(message: String, throwable: Throwable): Unit = {}
      override def onGenerateBlock(blockId: StreamBlockId): Unit = {}
      override def onAddData(data: Any, metadata: Any): Unit = {}
    }
  )

  setReceiverId(receiverId)

  override def onStart(): Unit = {
    customBlockGenerator
    RateTestReceiver.registerReceiver(this)
  }

  override def onStop(): Unit = {
    RateTestReceiver.deregisterReceiver()
  }

  override def preferredLocation: Option[String] = host

  def getDefaultBlockGeneratorRateLimit(): Long = {
    supervisor.getCurrentRateLimit
  }

  def getCustomBlockGeneratorRateLimit(): Long = {
    customBlockGenerator.getCurrentLimit
  }
}

/**
 * A helper object to RateTestReceiver that give access to the currently active RateTestReceiver
 * instance.
 */
private[streaming] object RateTestReceiver {
  @volatile private var activeReceiver: RateTestReceiver = null

  def registerReceiver(receiver: RateTestReceiver): Unit = {
    activeReceiver = receiver
  }

  def deregisterReceiver(): Unit = {
    activeReceiver = null
  }

  def getActive(): Option[RateTestReceiver] = Option(activeReceiver)
}
