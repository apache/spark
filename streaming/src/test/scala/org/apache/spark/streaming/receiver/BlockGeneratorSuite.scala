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

import scala.collection.mutable

import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers._
import org.scalatest.PrivateMethodTester._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.{SparkException, SparkConf, SparkFunSuite}

class BlockGeneratorSuite extends SparkFunSuite with BeforeAndAfter {

  import BlockGeneratorSuite._

  private val conf = new SparkConf().set("spark.streaming.blockInterval", "100ms")
  @volatile private var blockGenerator: BlockGenerator = null

  before {
    if (blockGenerator != null) {
      blockGenerator.stop()
    }
  }

  test("block generation and callbacks") {
    val listener = new TestBlockGeneratorListener
    blockGenerator = new BlockGenerator(listener, 0, conf)
    require(listener.onAddDataCalled === false)
    require(listener.onGenerateBlockCalled === false)
    require(listener.onPushBlockCalled === false)
    assert(blockGenerator.isStopped() === false)

    blockGenerator.start()
    assert(listener.onAddDataCalled === false)
    assert(listener.onGenerateBlockCalled === false)
    assert(listener.onPushBlockCalled === false)
    assert(blockGenerator.isStopped() === false)

    // Verify data is added and callback is called
    val data1 = 1 to 10
    data1.foreach { blockGenerator.addData _ }
    assert(listener.onAddDataCalled === false)
    assert(listener.onGenerateBlockCalled === false)
    assert(listener.onPushBlockCalled === false)

    eventually(timeout(10 seconds)) {
      assert(listener.onGenerateBlockCalled === true)
      assert(listener.onPushBlockCalled === true)
      listener.pushedData should contain theSameElementsInOrderAs (data1)
    }

    // Verify metadata is also received correctly
    val data2 = 11 to 20
    val metadata2 = data2.map { _.toString }
    data2.zip(metadata2).foreach { case (d, m) => blockGenerator.addDataWithCallback(d, m) }
    assert(listener.onAddDataCalled === true)
    listener.addedData should contain theSameElementsInOrderAs (data2)
    listener.addedMetadata should contain theSameElementsInOrderAs (metadata2)

    eventually(timeout(10 seconds)) {
      listener.pushedData should contain theSameElementsInOrderAs (data1 ++ data2)
    }

    // Verify that the block is eventually generated+pushed, callbacks are called and
    // the pushed data is correct
    assert(blockGenerator.isStopped() === false)
    blockGenerator.stop()
    assert(blockGenerator.isStopped() === true)
    blockGenerator.stop()   // Calling stop again should be fine
  }

  test("using stopped block generator fails") {
    blockGenerator = new BlockGenerator(new TestBlockGeneratorListener, 0, conf)
    blockGenerator.start()
    blockGenerator.stop()
    intercept[SparkException] {
      blockGenerator.addData(1)
    }
    intercept[SparkException] {
      blockGenerator.addDataWithCallback(1, 1)
    }
    intercept[SparkException] {
      blockGenerator.addMultipleDataWithCallback(Iterator(1), 1)
    }
  }

  test("stop ensures all data is pushed") {
    val listener = new TestBlockGeneratorListener
    blockGenerator = new BlockGenerator(listener, 0, conf)
    require(listener.onGenerateBlockCalled === false)
    blockGenerator.start()

    // Add data, then stop immediately and verify that all data has been pushed by the
    // time stop() finishes
    val data = 1 to 1000
    data.foreach { blockGenerator.addData _ }
    blockGenerator.stop()
    assert(listener.pushedData === data)
  }

  private class TestBlockGeneratorListener extends BlockGeneratorListener {
    val pushedData = new mutable.ArrayBuffer[Any] with mutable.SynchronizedBuffer[Any]
    val addedData = new mutable.ArrayBuffer[Any] with mutable.SynchronizedBuffer[Any]
    val addedMetadata = new mutable.ArrayBuffer[Any] with mutable.SynchronizedBuffer[Any]
    @volatile var onGenerateBlockCalled = false
    @volatile var onAddDataCalled = false
    @volatile var onPushBlockCalled = false

    override def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      onPushBlockCalled = true
      pushedData ++= arrayBuffer
    }
    override def onError(message: String, throwable: Throwable): Unit = {}
    override def onGenerateBlock(blockId: StreamBlockId): Unit = {
      onGenerateBlockCalled = true
    }
    override def onAddData(data: Any, metadata: Any): Unit = {
      onAddDataCalled = true
      addedData += data
      addedMetadata += metadata
    }
  }
}

object BlockGeneratorSuite {
  def getCurrentBuffer(blockGenerator: BlockGenerator): mutable.ArrayBuffer[Any] = {
    val currentBuffer = PrivateMethod[mutable.ArrayBuffer[Any]]('currentBuffer)
    blockGenerator.invokePrivate(currentBuffer())
  }
}
