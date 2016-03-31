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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart, TaskLocality}
import org.apache.spark.scheduler.TaskLocality.TaskLocality
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

  test("should restart receiver after stopping it") {
    withStreamingContext(new StreamingContext(conf, Milliseconds(100))) { ssc =>
      @volatile var startTimes = 0
      ssc.addStreamingListener(new StreamingListener {
        override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
          startTimes += 1
        }
      })
      val input = ssc.receiverStream(new StoppableReceiver)
      val output = new TestOutputStream(input)
      output.register()
      ssc.start()
      StoppableReceiver.shouldStop = true
      eventually(timeout(10 seconds), interval(10 millis)) {
        // The receiver is stopped once, so if it's restarted, it should be started twice.
        assert(startTimes === 2)
      }
    }
  }

  test("SPARK-11063: TaskSetManager should use Receiver RDD's preferredLocations") {
    // Use ManualClock to prevent from starting batches so that we can make sure the only task is
    // for starting the Receiver
    val _conf = conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    withStreamingContext(new StreamingContext(_conf, Milliseconds(100))) { ssc =>
      @volatile var receiverTaskLocality: TaskLocality = null
      ssc.sparkContext.addSparkListener(new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          receiverTaskLocality = taskStart.taskInfo.taskLocality
        }
      })
      val input = ssc.receiverStream(new TestReceiver)
      val output = new TestOutputStream(input)
      output.register()
      ssc.start()
      eventually(timeout(10 seconds), interval(10 millis)) {
        // If preferredLocations is set correctly, receiverTaskLocality should be PROCESS_LOCAL
        assert(receiverTaskLocality === TaskLocality.PROCESS_LOCAL)
      }
    }
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

/**
 * A custom receiver that could be stopped via StoppableReceiver.shouldStop
 */
class StoppableReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        while (!StoppableReceiver.shouldStop) {
          Thread.sleep(10)
        }
        StoppableReceiver.this.stop("stop")
      }
    }
    thread.start()
  }

  def onStop() {
    StoppableReceiver.shouldStop = true
    receivingThreadOption.foreach(_.join())
    // Reset it so as to restart it
    StoppableReceiver.shouldStop = false
  }
}

object StoppableReceiver {
  @volatile var shouldStop = false
}
