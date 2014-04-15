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

import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, NetworkReceiver, NetworkReceiverExecutor}
/** Testsuite for testing the network receiver behavior */
class NetworkReceiverSuite extends FunSuite with Timeouts {

  test("network receiver life cycle") {
    val receiver = new FakeReceiver
    val executor = new FakeReceiverExecutor(receiver)

    // Thread that runs the executor
    val executingThread = new Thread() {
      override def run() {
        println("Running receiver")
        executor.run()
        println("Finished receiver")
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

    // Verify that onStart was called, and onStop wasn't called
    assert(receiver.started)
    assert(receiver.otherThread.isAlive)
    assert(!receiver.stopped)
    assert(executor.isAllEmpty)

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

    // Verify that stopping actually stops the thread
    failAfter(100 millis) {
      receiver.stop()
      executingThread.join()
      assert(!receiver.otherThread.isAlive)
    }

    // Verify that onStop was called
    assert(receiver.stopped)
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
}

/**
 * An implementation of NetworkReceiver that is used for testing a receiver's life cycle.
 */
class FakeReceiver extends NetworkReceiver[Int](StorageLevel.MEMORY_ONLY) {
  var started = false
  var stopped = false
  val otherThread = new Thread() {
    override def run() {
      while(!stopped) {
        Thread.sleep(10)
      }
    }
  }

  def onStart() {
    otherThread.start()
    started = true
  }
  def onStop() {
    stopped = true
    otherThread.join()
  }
}

/**
 * An implementation of NetworkReceiverExecutor used for testing a NetworkReceiver.
 * Instead of storing the data in the BlockManager, it stores all the data in a local buffer
 * that can used for verifying that the data has been forwarded correctly.
 */
class FakeReceiverExecutor(receiver: FakeReceiver) extends NetworkReceiverExecutor(receiver) {
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