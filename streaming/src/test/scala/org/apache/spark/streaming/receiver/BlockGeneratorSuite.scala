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

package org.apache.spark.streaming.receiver

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.util.ManualClock

class BlockGeneratorSuite extends SparkFunSuite with BeforeAndAfter with TimeLimits {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  private val blockIntervalMs = 10
  private val conf = new SparkConf().set("spark.streaming.blockInterval", s"${blockIntervalMs}ms")
  @volatile private var blockGenerator: BlockGenerator = null

  after {
    if (blockGenerator != null) {
      blockGenerator.stop()
    }
  }

  test("block generation and data callbacks") {
    val listener = new TestBlockGeneratorListener
    val clock = new ManualClock()

    require(blockIntervalMs > 5)
    require(listener.onAddDataCalled === false)
    require(listener.onGenerateBlockCalled === false)
    require(listener.onPushBlockCalled === false)

    // Verify that creating the generator does not start it
    blockGenerator = new BlockGenerator(listener, 0, conf, clock)
    assert(blockGenerator.isActive() === false, "block generator active before start()")
    assert(blockGenerator.isStopped() === false, "block generator stopped before start()")
    assert(listener.onAddDataCalled === false)
    assert(listener.onGenerateBlockCalled === false)
    assert(listener.onPushBlockCalled === false)

    // Verify start marks the generator active, but does not call the callbacks
    blockGenerator.start()
    assert(blockGenerator.isActive(), "block generator active after start()")
    assert(blockGenerator.isStopped() === false, "block generator stopped after start()")
    withClue("callbacks called before adding data") {
      assert(listener.onAddDataCalled === false)
      assert(listener.onGenerateBlockCalled === false)
      assert(listener.onPushBlockCalled === false)
    }

    // Verify whether addData() adds data that is present in generated blocks
    val data1 = 1 to 10
    data1.foreach { blockGenerator.addData _ }
    withClue("callbacks called on adding data without metadata and without block generation") {
      assert(listener.onAddDataCalled === false) // should be called only with addDataWithCallback()
      assert(listener.onGenerateBlockCalled === false)
      assert(listener.onPushBlockCalled === false)
    }
    clock.advance(blockIntervalMs)  // advance clock to generate blocks
    withClue("blocks not generated or pushed") {
      eventually(timeout(1.second)) {
        assert(listener.onGenerateBlockCalled)
        assert(listener.onPushBlockCalled)
      }
    }
    listener.pushedData.asScala.toSeq should contain theSameElementsInOrderAs (data1)
    assert(listener.onAddDataCalled === false) // should be called only with addDataWithCallback()

    // Verify addDataWithCallback() add data+metadata and callbacks are called correctly
    val data2 = 11 to 20
    val metadata2 = data2.map { _.toString }
    data2.zip(metadata2).foreach { case (d, m) => blockGenerator.addDataWithCallback(d, m) }
    assert(listener.onAddDataCalled)
    listener.addedData.asScala.toSeq should contain theSameElementsInOrderAs (data2)
    listener.addedMetadata.asScala.toSeq should contain theSameElementsInOrderAs (metadata2)
    clock.advance(blockIntervalMs)  // advance clock to generate blocks
    eventually(timeout(1.second)) {
      val combined = data1 ++ data2
      listener.pushedData.asScala.toSeq should contain theSameElementsInOrderAs combined
    }

    // Verify addMultipleDataWithCallback() add data+metadata and callbacks are called correctly
    val data3 = 21 to 30
    val metadata3 = "metadata"
    blockGenerator.addMultipleDataWithCallback(data3.iterator, metadata3)
    val combinedMetadata = metadata2 :+ metadata3
    listener.addedMetadata.asScala.toSeq should contain theSameElementsInOrderAs (combinedMetadata)
    clock.advance(blockIntervalMs)  // advance clock to generate blocks
    eventually(timeout(1.second)) {
      val combinedData = data1 ++ data2 ++ data3
      listener.pushedData.asScala.toSeq should contain theSameElementsInOrderAs (combinedData)
    }

    // Stop the block generator by starting the stop on a different thread and
    // then advancing the manual clock for the stopping to proceed.
    val thread = stopBlockGenerator(blockGenerator)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      clock.advance(blockIntervalMs)
      assert(blockGenerator.isStopped())
    }
    thread.join()

    // Verify that the generator cannot be used any more
    intercept[SparkException] {
      blockGenerator.addData(1)
    }
    intercept[SparkException] {
      blockGenerator.addDataWithCallback(1, 1)
    }
    intercept[SparkException] {
      blockGenerator.addMultipleDataWithCallback(Iterator(1), 1)
    }
    intercept[SparkException] {
      blockGenerator.start()
    }
    blockGenerator.stop()   // Calling stop again should be fine
  }

  test("stop ensures correct shutdown") {
    val listener = new TestBlockGeneratorListener
    val clock = new ManualClock()
    blockGenerator = new BlockGenerator(listener, 0, conf, clock)
    require(listener.onGenerateBlockCalled === false)
    blockGenerator.start()
    assert(blockGenerator.isActive(), "block generator")
    assert(blockGenerator.isStopped() === false)

    val data = 1 to 1000
    data.foreach { blockGenerator.addData _ }

    // Verify that stop() shutdowns everything in the right order
    // - First, stop receiving new data
    // - Second, wait for final block with all buffered data to be generated
    // - Finally, wait for all blocks to be pushed
    clock.advance(1) // to make sure that the timer for another interval to complete
    val thread = stopBlockGenerator(blockGenerator)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(blockGenerator.isActive() === false)
    }
    assert(blockGenerator.isStopped() === false)

    // Verify that data cannot be added
    intercept[SparkException] {
      blockGenerator.addData(1)
    }
    intercept[SparkException] {
      blockGenerator.addDataWithCallback(1, null)
    }
    intercept[SparkException] {
      blockGenerator.addMultipleDataWithCallback(Iterator(1), null)
    }

    // Verify that stop() stays blocked until another block containing all the data is generated
    // This intercept always succeeds, as the body either will either throw a timeout exception
    // (expected as stop() should never complete) or a SparkException (unexpected as stop()
    // completed and thread terminated).
    val exception = intercept[Exception] {
      failAfter(200.milliseconds) {
        thread.join()
        throw new SparkException(
          "BlockGenerator.stop() completed before generating timer was stopped")
      }
    }
    exception should not be a [SparkException]


    // Verify that the final data is present in the final generated block and
    // pushed before complete stop
    assert(blockGenerator.isStopped() === false) // generator has not stopped yet
    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      // Keep calling `advance` to avoid blocking forever in `clock.waitTillTime`
      clock.advance(blockIntervalMs)
      assert(thread.isAlive === false)
    }
    assert(blockGenerator.isStopped()) // generator has finally been completely stopped
    assert(listener.pushedData.asScala.toSeq === data, "All data not pushed by stop()")
  }

  test("block push errors are reported") {
    val listener = new TestBlockGeneratorListener {
      override def onPushBlock(
          blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
        throw new SparkException("test")
      }
    }
    blockGenerator = new BlockGenerator(listener, 0, conf)
    blockGenerator.start()
    assert(listener.onErrorCalled === false)
    blockGenerator.addData(1)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(listener.onErrorCalled)
    }
    blockGenerator.stop()
  }

  /**
   * Helper method to stop the block generator with manual clock in a different thread,
   * so that the main thread can advance the clock that allows the stopping to proceed.
   */
  private def stopBlockGenerator(blockGenerator: BlockGenerator): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        blockGenerator.stop()
      }
    }
    thread.start()
    thread
  }

  /** A listener for BlockGenerator that records the data in the callbacks */
  private class TestBlockGeneratorListener extends BlockGeneratorListener {
    val pushedData = new ConcurrentLinkedQueue[Any]
    val addedData = new ConcurrentLinkedQueue[Any]
    val addedMetadata = new ConcurrentLinkedQueue[Any]
    @volatile var onGenerateBlockCalled = false
    @volatile var onAddDataCalled = false
    @volatile var onPushBlockCalled = false
    @volatile var onErrorCalled = false

    override def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      pushedData.addAll(arrayBuffer.asJava)
      onPushBlockCalled = true
    }
    override def onError(message: String, throwable: Throwable): Unit = {
      onErrorCalled = true
    }
    override def onGenerateBlock(blockId: StreamBlockId): Unit = {
      onGenerateBlockCalled = true
    }
    override def onAddData(data: Any, metadata: Any): Unit = {
      addedData.add(data)
      addedMetadata.add(metadata)
      onAddDataCalled = true
    }
  }
}
