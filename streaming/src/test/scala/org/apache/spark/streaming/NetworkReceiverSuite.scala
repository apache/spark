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

package org.apache.spark.streaming

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver, ReceiverSupervisor}
import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

/** Testsuite for testing the network receiver behavior */
class NetworkReceiverSuite extends FunSuite with Timeouts {

  test("network receiver life cycle") {

    val receiver = new FakeReceiver
    val executor = new FakeReceiverSupervisor(receiver)

    assert(executor.isAllEmpty)

    // Thread that runs the executor
    val executingThread = new Thread() {
      override def run() {
        executor.start()
        executor.awaitTermination()
      }
    }

    // Start the receiver
    executingThread.start()

    // Verify that the receiver
    intercept[Exception] {
      failAfter(200 millis) {
        executingThread.join()
      }
    }

    // Verify that receiver was started
    assert(receiver.onStartCalled)
    assert(executor.isReceiverStarted)
    assert(receiver.isStarted)
    assert(!receiver.isStopped())
    assert(receiver.otherThread.isAlive)
    eventually(timeout(100 millis), interval(10 millis)) {
      assert(receiver.receiving)
    }

    // Verify whether the data stored by the receiver was sent to the executor
    val byteBuffer = ByteBuffer.allocate(100)
    val arrayBuffer = new ArrayBuffer[Int]()
    val iterator = arrayBuffer.iterator
    receiver.store(1)
    receiver.store(byteBuffer)
    receiver.store(arrayBuffer)
    receiver.store(iterator)
    assert(executor.singles.size === 1)
    assert(executor.singles.head === 1)
    assert(executor.byteBuffers.size === 1)
    assert(executor.byteBuffers.head.eq(byteBuffer))
    assert(executor.iterators.size === 1)
    assert(executor.iterators.head.eq(iterator))
    assert(executor.arrayBuffers.size === 1)
    assert(executor.arrayBuffers.head.eq(arrayBuffer))

    // Verify whether the exceptions reported by the receiver was sent to the executor
    val exception = new Exception
    receiver.reportError("Error", exception)
    assert(executor.errors.size === 1)
    assert(executor.errors.head.eq(exception))

    // Verify restarting actually stops and starts the receiver
    receiver.restart("restarting", null, 100)
    eventually(timeout(50 millis), interval(10 millis)) {
      // receiver will be stopped async
      assert(receiver.isStopped)
      assert(receiver.onStopCalled)
    }
    eventually(timeout(1000 millis), interval(100 millis)) {
      // receiver will be started async
      assert(receiver.onStartCalled)
      assert(executor.isReceiverStarted)
      assert(receiver.isStarted)
      assert(!receiver.isStopped)
      assert(receiver.receiving)
    }

    // Verify that stopping actually stops the thread
    failAfter(100 millis) {
      receiver.stop("test")
      assert(receiver.isStopped)
      assert(!receiver.otherThread.isAlive)

      // The thread that started the executor should complete
      // as stop() stops everything
      executingThread.join()
    }
  }

  test("block generator") {
    val blockGeneratorListener = new FakeBlockGeneratorListener
    val blockInterval = 200
    val conf = new SparkConf().set("spark.streaming.blockInterval", blockInterval.toString)
    val blockGenerator = new BlockGenerator(blockGeneratorListener, 1, conf)
    val expectedBlocks = 5
    val waitTime = expectedBlocks * blockInterval + (blockInterval / 2)
    val generatedData = new ArrayBuffer[Int]

    // Generate blocks
    val startTime = System.currentTimeMillis()
    blockGenerator.start()
    var count = 0
    while(System.currentTimeMillis - startTime < waitTime) {
      blockGenerator += count
      generatedData += count
      count += 1
      Thread.sleep(10)
    }
    blockGenerator.stop()

    val recordedData = blockGeneratorListener.arrayBuffers.flatten
    assert(blockGeneratorListener.arrayBuffers.size > 0)
    assert(recordedData.toSet === generatedData.toSet)
  }

  test("block generator throttling") {
    val blockGeneratorListener = new FakeBlockGeneratorListener
    val blockInterval = 50
    val maxRate = 200
    val conf = new SparkConf().set("spark.streaming.blockInterval", blockInterval.toString).
      set("spark.streaming.receiver.maxRate", maxRate.toString)
    val blockGenerator = new BlockGenerator(blockGeneratorListener, 1, conf)
    val expectedBlocks = 20
    val waitTime = expectedBlocks * blockInterval
    val expectedMessages = maxRate * waitTime / 1000
    val expectedMessagesPerBlock = maxRate * blockInterval / 1000
    val generatedData = new ArrayBuffer[Int]

    // Generate blocks
    val startTime = System.currentTimeMillis()
    blockGenerator.start()
    var count = 0
    while(System.currentTimeMillis - startTime < waitTime) {
      blockGenerator += count
      generatedData += count
      count += 1
      Thread.sleep(1)
    }
    blockGenerator.stop()

    val recordedData = blockGeneratorListener.arrayBuffers
    assert(blockGeneratorListener.arrayBuffers.size > 0)
    assert(recordedData.flatten.toSet === generatedData.toSet)
    // recordedData size should be close to the expected rate
    assert(recordedData.flatten.size >= expectedMessages * 0.9 &&
      recordedData.flatten.size <= expectedMessages * 1.1 )
    // the first and last block may be incomplete, so we slice them out
    recordedData.slice(1, recordedData.size - 1).foreach { block =>
      assert(block.size >= expectedMessagesPerBlock * 0.8 &&
        block.size <= expectedMessagesPerBlock * 1.2 )
    }
  }

  /**
   * An implementation of NetworkReceiver that is used for testing a receiver's life cycle.
   */
  class FakeReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) {
    var otherThread: Thread = null
    var receiving = false
    var onStartCalled = false
    var onStopCalled = false

    def onStart() {
      otherThread = new Thread() {
        override def run() {
          receiving = true
          while(!isStopped()) {
            Thread.sleep(10)
          }
        }
      }
      onStartCalled = true
      otherThread.start()

    }

    def onStop() {
      onStopCalled = true
      otherThread.join()
    }

    def reset() {
      receiving = false
      onStartCalled = false
      onStopCalled = false
    }
  }

  /**
   * An implementation of NetworkReceiverExecutor used for testing a NetworkReceiver.
   * Instead of storing the data in the BlockManager, it stores all the data in a local buffer
   * that can used for verifying that the data has been forwarded correctly.
   */
  class FakeReceiverSupervisor(receiver: FakeReceiver)
    extends ReceiverSupervisor(receiver, new SparkConf()) {
    val singles = new ArrayBuffer[Any]
    val byteBuffers = new ArrayBuffer[ByteBuffer]
    val iterators = new ArrayBuffer[Iterator[_]]
    val arrayBuffers = new ArrayBuffer[ArrayBuffer[_]]
    val errors = new ArrayBuffer[Throwable]

    /** Check if all data structures are clean */
    def isAllEmpty = {
      singles.isEmpty && byteBuffers.isEmpty && iterators.isEmpty &&
        arrayBuffers.isEmpty && errors.isEmpty
    }

    def pushSingle(data: Any) {
      singles += data
    }

    def pushBytes(
        bytes: ByteBuffer,
        optionalMetadata: Option[Any],
        optionalBlockId: Option[StreamBlockId]
      ) {
      byteBuffers += bytes
    }

    def pushIterator(
        iterator: Iterator[_],
        optionalMetadata: Option[Any],
        optionalBlockId: Option[StreamBlockId]
      ) {
      iterators += iterator
    }

    def pushArrayBuffer(
        arrayBuffer: ArrayBuffer[_],
        optionalMetadata: Option[Any],
        optionalBlockId: Option[StreamBlockId]
      ) {
      arrayBuffers +=  arrayBuffer
    }

    def reportError(message: String, throwable: Throwable) {
      errors += throwable
    }
  }

  /**
   * An implementation of BlockGeneratorListener that is used to test the BlockGenerator.
   */
  class FakeBlockGeneratorListener(pushDelay: Long = 0) extends BlockGeneratorListener {
    // buffer of data received as ArrayBuffers
    val arrayBuffers = new ArrayBuffer[ArrayBuffer[Int]]
    val errors = new ArrayBuffer[Throwable]

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      val bufferOfInts = arrayBuffer.map(_.asInstanceOf[Int])
      arrayBuffers += bufferOfInts
      Thread.sleep(0)
    }

    def onError(message: String, throwable: Throwable) {
      errors += throwable
    }
  }
}

