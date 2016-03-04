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

import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.Arrays

import org.mockito.Mockito.{mock, RETURNS_SMART_NULLS}

import org.apache.spark.{SparkConf, SparkFunSuite}

class DiskStoreSuite extends SparkFunSuite {
  test("reads of memory-mapped and non memory-mapped files are equivalent") {
    val confKey = "spark.storage.memoryMapThreshold"

    // Create a non-trivial (not all zeros) byte array
    var counter = 0.toByte
    def incr: Byte = {counter = (counter + 1).toByte; counter;}
    val bytes = Array.fill[Byte](1000)(incr)
    val byteBuffer = ByteBuffer.wrap(bytes)

    val blockId = BlockId("rdd_1_2")
    val blockManager = mock(classOf[BlockManager], RETURNS_SMART_NULLS)
    val diskBlockManager = new DiskBlockManager(blockManager, new SparkConf())

    // This sequence of mocks makes these tests fairly brittle. It would
    // be nice to refactor classes involved in disk storage in a way that
    // allows for easier testing.

    val diskStoreMapped = new DiskStore(new SparkConf().set(confKey, "0"), diskBlockManager)
    diskStoreMapped.putBytes(blockId, byteBuffer)
    val mapped = diskStoreMapped.getBytes(blockId)
    assert(diskStoreMapped.remove(blockId))

    val diskStoreNotMapped = new DiskStore(new SparkConf().set(confKey, "1m"), diskBlockManager)
    diskStoreNotMapped.putBytes(blockId, byteBuffer)
    val notMapped = diskStoreNotMapped.getBytes(blockId)

    // Not possible to do isInstanceOf due to visibility of HeapByteBuffer
    assert(notMapped.getClass.getName.endsWith("HeapByteBuffer"),
      "Expected HeapByteBuffer for un-mapped read")
    assert(mapped.isInstanceOf[MappedByteBuffer], "Expected MappedByteBuffer for mapped read")

    def arrayFromByteBuffer(in: ByteBuffer): Array[Byte] = {
      val array = new Array[Byte](in.remaining())
      in.get(array)
      array
    }

    val mappedAsArray = arrayFromByteBuffer(mapped)
    val notMappedAsArray = arrayFromByteBuffer(notMapped)
    assert(Arrays.equals(mappedAsArray, bytes))
    assert(Arrays.equals(notMappedAsArray, bytes))
  }
}
