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

package org.apache.spark.storage

import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.mockito.Mockito
import org.mockito.Mockito.atLeastOnce
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.memory.MemoryMode
import org.apache.spark.serializer.{JavaSerializer, SerializationStream, SerializerManager}
import org.apache.spark.storage.memory.{MemoryStore, PartiallySerializedBlock, RedirectableOutputStream}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

class PartiallySerializedBlockSuite
    extends SparkFunSuite
    with BeforeAndAfterEach
    with PrivateMethodTester {

  private val blockId = new TestBlockId("test")
  private val conf = new SparkConf()
  private val memoryStore = Mockito.mock(classOf[MemoryStore], Mockito.RETURNS_SMART_NULLS)
  private val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)

  private val getSerializationStream =
    PrivateMethod[SerializationStream](Symbol("serializationStream"))
  private val getRedirectableOutputStream =
    PrivateMethod[RedirectableOutputStream](Symbol("redirectableOutputStream"))

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Mockito.reset(memoryStore)
  }

  private def partiallyUnroll[T: ClassTag](
      iter: Iterator[T],
      numItemsToBuffer: Int): PartiallySerializedBlock[T] = {

    val bbos: ChunkedByteBufferOutputStream = {
      val spy = Mockito.spy[ChunkedByteBufferOutputStream](
        new ChunkedByteBufferOutputStream(128, ByteBuffer.allocate))
      Mockito.doAnswer { (invocationOnMock: InvocationOnMock) =>
        Mockito.spy[ChunkedByteBuffer](
          invocationOnMock.callRealMethod().asInstanceOf[ChunkedByteBuffer])
      }.when(spy).toChunkedByteBuffer
      spy
    }

    val serializer = serializerManager
      .getSerializer(implicitly[ClassTag[T]], autoPick = true).newInstance()
    val redirectableOutputStream = Mockito.spy[RedirectableOutputStream](
      new RedirectableOutputStream)
    redirectableOutputStream.setOutputStream(bbos)
    val serializationStream = Mockito.spy[SerializationStream](
      serializer.serializeStream(redirectableOutputStream))

    (1 to numItemsToBuffer).foreach { _ =>
      assert(iter.hasNext)
      serializationStream.writeObject[T](iter.next())
    }

    val unrollMemory = bbos.size
    new PartiallySerializedBlock[T](
      memoryStore,
      serializerManager,
      blockId,
      serializationStream = serializationStream,
      redirectableOutputStream,
      unrollMemory = unrollMemory,
      memoryMode = MemoryMode.ON_HEAP,
      bbos,
      rest = iter,
      classTag = implicitly[ClassTag[T]])
  }

  test("valuesIterator() and finishWritingToStream() cannot be called after discard() is called") {
    val partiallySerializedBlock = partiallyUnroll((1 to 10).iterator, 2)
    partiallySerializedBlock.discard()
    intercept[SparkException] {
      partiallySerializedBlock.finishWritingToStream(null)
    }
    intercept[SparkException] {
      partiallySerializedBlock.valuesIterator
    }
  }

  test("discard() can be called more than once") {
    val partiallySerializedBlock = partiallyUnroll((1 to 10).iterator, 2)
    partiallySerializedBlock.discard()
    partiallySerializedBlock.discard()
  }

  test("cannot call valuesIterator() more than once") {
    val partiallySerializedBlock = partiallyUnroll((1 to 10).iterator, 2)
    partiallySerializedBlock.valuesIterator
    intercept[SparkException] {
      partiallySerializedBlock.valuesIterator
    }
  }

  test("cannot call finishWritingToStream() more than once") {
    val partiallySerializedBlock = partiallyUnroll((1 to 10).iterator, 2)
    partiallySerializedBlock.finishWritingToStream(new ByteBufferOutputStream())
    intercept[SparkException] {
      partiallySerializedBlock.finishWritingToStream(new ByteBufferOutputStream())
    }
  }

  test("cannot call finishWritingToStream() after valuesIterator()") {
    val partiallySerializedBlock = partiallyUnroll((1 to 10).iterator, 2)
    partiallySerializedBlock.valuesIterator
    intercept[SparkException] {
      partiallySerializedBlock.finishWritingToStream(new ByteBufferOutputStream())
    }
  }

  test("cannot call valuesIterator() after finishWritingToStream()") {
    val partiallySerializedBlock = partiallyUnroll((1 to 10).iterator, 2)
    partiallySerializedBlock.finishWritingToStream(new ByteBufferOutputStream())
    intercept[SparkException] {
      partiallySerializedBlock.valuesIterator
    }
  }

  test("buffers are deallocated in a TaskCompletionListener") {
    try {
      TaskContext.setTaskContext(TaskContext.empty())
      val partiallySerializedBlock = partiallyUnroll((1 to 10).iterator, 2)
      TaskContext.get().asInstanceOf[TaskContextImpl].markTaskCompleted(None)
      Mockito.verify(partiallySerializedBlock.getUnrolledChunkedByteBuffer).dispose()
      Mockito.verifyNoMoreInteractions(memoryStore)
    } finally {
      TaskContext.unset()
    }
  }

  private def testUnroll[T: ClassTag](
      testCaseName: String,
      items: Seq[T],
      numItemsToBuffer: Int): Unit = {

    test(s"$testCaseName with discard() and numBuffered = $numItemsToBuffer") {
      val partiallySerializedBlock = partiallyUnroll(items.iterator, numItemsToBuffer)
      partiallySerializedBlock.discard()

      Mockito.verify(memoryStore).releaseUnrollMemoryForThisTask(
        MemoryMode.ON_HEAP, partiallySerializedBlock.unrollMemory)
      Mockito.verify(partiallySerializedBlock.invokePrivate(getSerializationStream())).close()
      Mockito.verify(partiallySerializedBlock.invokePrivate(getRedirectableOutputStream())).close()
      Mockito.verifyNoMoreInteractions(memoryStore)
      Mockito.verify(partiallySerializedBlock.getUnrolledChunkedByteBuffer, atLeastOnce).dispose()
    }

    test(s"$testCaseName with finishWritingToStream() and numBuffered = $numItemsToBuffer") {
      val partiallySerializedBlock = partiallyUnroll(items.iterator, numItemsToBuffer)
      val bbos = Mockito.spy[ByteBufferOutputStream](new ByteBufferOutputStream())
      partiallySerializedBlock.finishWritingToStream(bbos)

      Mockito.verify(memoryStore).releaseUnrollMemoryForThisTask(
        MemoryMode.ON_HEAP, partiallySerializedBlock.unrollMemory)
      Mockito.verify(partiallySerializedBlock.invokePrivate(getSerializationStream())).close()
      Mockito.verify(partiallySerializedBlock.invokePrivate(getRedirectableOutputStream())).close()
      Mockito.verify(bbos).close()
      Mockito.verifyNoMoreInteractions(memoryStore)
      Mockito.verify(partiallySerializedBlock.getUnrolledChunkedByteBuffer, atLeastOnce).dispose()

      val serializer = serializerManager
        .getSerializer(implicitly[ClassTag[T]], autoPick = true).newInstance()
      val deserialized =
        serializer.deserializeStream(new ByteBufferInputStream(bbos.toByteBuffer)).asIterator.toSeq
      assert(deserialized === items)
    }

    test(s"$testCaseName with valuesIterator() and numBuffered = $numItemsToBuffer") {
      val partiallySerializedBlock = partiallyUnroll(items.iterator, numItemsToBuffer)
      val valuesIterator = partiallySerializedBlock.valuesIterator
      Mockito.verify(partiallySerializedBlock.invokePrivate(getSerializationStream())).close()
      Mockito.verify(partiallySerializedBlock.invokePrivate(getRedirectableOutputStream())).close()

      val deserializedItems = valuesIterator.toArray.toSeq
      Mockito.verify(memoryStore).releaseUnrollMemoryForThisTask(
        MemoryMode.ON_HEAP, partiallySerializedBlock.unrollMemory)
      Mockito.verifyNoMoreInteractions(memoryStore)
      Mockito.verify(partiallySerializedBlock.getUnrolledChunkedByteBuffer, atLeastOnce).dispose()
      assert(deserializedItems === items)
    }
  }

  testUnroll("basic numbers", 1 to 1000, numItemsToBuffer = 50)
  testUnroll("basic numbers", 1 to 1000, numItemsToBuffer = 0)
  testUnroll("basic numbers", 1 to 1000, numItemsToBuffer = 1000)
  testUnroll("case classes", (1 to 1000).map(x => MyCaseClass(x.toString)), numItemsToBuffer = 50)
  testUnroll("case classes", (1 to 1000).map(x => MyCaseClass(x.toString)), numItemsToBuffer = 0)
  testUnroll("case classes", (1 to 1000).map(x => MyCaseClass(x.toString)), numItemsToBuffer = 1000)
  testUnroll("empty iterator", Seq.empty[String], numItemsToBuffer = 0)
}

private case class MyCaseClass(str: String)
