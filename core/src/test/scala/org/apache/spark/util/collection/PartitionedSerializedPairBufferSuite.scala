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

package org.apache.spark.util.collection

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.common.io.ByteStreams

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.Mockito.RETURNS_SMART_NULLS
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Matchers._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.DiskBlockObjectWriter

class PartitionedSerializedPairBufferSuite extends SparkFunSuite {
  test("OrderedInputStream single record") {
    val serializerInstance = new KryoSerializer(new SparkConf()).newInstance

    val buffer = new PartitionedSerializedPairBuffer[Int, SomeStruct](4, 32, serializerInstance)
    val struct = SomeStruct("something", 5)
    buffer.insert(4, 10, struct)

    val bytes = ByteStreams.toByteArray(buffer.orderedInputStream)

    val baos = new ByteArrayOutputStream()
    val stream = serializerInstance.serializeStream(baos)
    stream.writeObject(10)
    stream.writeObject(struct)
    stream.close()

    baos.toByteArray should be (bytes)
  }

  test("insert single record") {
    val serializerInstance = new KryoSerializer(new SparkConf()).newInstance
    val buffer = new PartitionedSerializedPairBuffer[Int, SomeStruct](4, 32, serializerInstance)
    val struct = SomeStruct("something", 5)
    buffer.insert(4, 10, struct)
    val elements = buffer.partitionedDestructiveSortedIterator(None).toArray
    elements.size should be (1)
    elements.head should be (((4, 10), struct))
  }

  test("insert multiple records") {
    val serializerInstance = new KryoSerializer(new SparkConf()).newInstance
    val buffer = new PartitionedSerializedPairBuffer[Int, SomeStruct](4, 32, serializerInstance)
    val struct1 = SomeStruct("something1", 8)
    buffer.insert(6, 1, struct1)
    val struct2 = SomeStruct("something2", 9)
    buffer.insert(4, 2, struct2)
    val struct3 = SomeStruct("something3", 10)
    buffer.insert(5, 3, struct3)

    val elements = buffer.partitionedDestructiveSortedIterator(None).toArray
    elements.size should be (3)
    elements(0) should be (((4, 2), struct2))
    elements(1) should be (((5, 3), struct3))
    elements(2) should be (((6, 1), struct1))
  }

  test("write single record") {
    val serializerInstance = new KryoSerializer(new SparkConf()).newInstance
    val buffer = new PartitionedSerializedPairBuffer[Int, SomeStruct](4, 32, serializerInstance)
    val struct = SomeStruct("something", 5)
    buffer.insert(4, 10, struct)
    val it = buffer.destructiveSortedWritablePartitionedIterator(None)
    val (writer, baos) = createMockWriter()
    assert(it.hasNext)
    it.nextPartition should be (4)
    it.writeNext(writer)
    assert(!it.hasNext)

    val stream = serializerInstance.deserializeStream(new ByteArrayInputStream(baos.toByteArray))
    stream.readObject[AnyRef]() should be (10)
    stream.readObject[AnyRef]() should be (struct)
  }

  test("write multiple records") {
    val serializerInstance = new KryoSerializer(new SparkConf()).newInstance
    val buffer = new PartitionedSerializedPairBuffer[Int, SomeStruct](4, 32, serializerInstance)
    val struct1 = SomeStruct("something1", 8)
    buffer.insert(6, 1, struct1)
    val struct2 = SomeStruct("something2", 9)
    buffer.insert(4, 2, struct2)
    val struct3 = SomeStruct("something3", 10)
    buffer.insert(5, 3, struct3)

    val it = buffer.destructiveSortedWritablePartitionedIterator(None)
    val (writer, baos) = createMockWriter()
    assert(it.hasNext)
    it.nextPartition should be (4)
    it.writeNext(writer)
    assert(it.hasNext)
    it.nextPartition should be (5)
    it.writeNext(writer)
    assert(it.hasNext)
    it.nextPartition should be (6)
    it.writeNext(writer)
    assert(!it.hasNext)

    val stream = serializerInstance.deserializeStream(new ByteArrayInputStream(baos.toByteArray))
    val iter = stream.asIterator
    iter.next() should be (2)
    iter.next() should be (struct2)
    iter.next() should be (3)
    iter.next() should be (struct3)
    iter.next() should be (1)
    iter.next() should be (struct1)
    assert(!iter.hasNext)
  }

  def createMockWriter(): (DiskBlockObjectWriter, ByteArrayOutputStream) = {
    val writer = mock(classOf[DiskBlockObjectWriter], RETURNS_SMART_NULLS)
    val baos = new ByteArrayOutputStream()
    when(writer.write(any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocationOnMock: InvocationOnMock): Unit = {
        val args = invocationOnMock.getArguments
        val bytes = args(0).asInstanceOf[Array[Byte]]
        val offset = args(1).asInstanceOf[Int]
        val length = args(2).asInstanceOf[Int]
        baos.write(bytes, offset, length)
      }
    })
    (writer, baos)
  }
}

case class SomeStruct(str: String, num: Int)
