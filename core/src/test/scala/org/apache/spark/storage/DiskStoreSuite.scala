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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.util.Utils

class DiskStoreSuite extends SparkFunSuite {

  test("reads of memory-mapped and non memory-mapped files are equivalent") {
    assume(!Utils.isWindows)
    val confKey = "spark.storage.memoryMapThreshold"

    // Create a non-trivial (not all zeros) byte array
    val bytes = Array.tabulate[Byte](1000)(_.toByte)
    val byteBuffer = new ChunkedByteBuffer(ByteBuffer.wrap(bytes))

    val blockId = BlockId("rdd_1_2")
    val diskBlockManager = new DiskBlockManager(new SparkConf(), deleteFilesOnStop = true)

    val diskStoreMapped = new DiskStore(new SparkConf().set(confKey, "0"), diskBlockManager)
    diskStoreMapped.putBytes(blockId, byteBuffer)
    val mapped = diskStoreMapped.getBytes(blockId)
    assert(diskStoreMapped.remove(blockId))

    val diskStoreNotMapped = new DiskStore(new SparkConf().set(confKey, "1m"), diskBlockManager)
    diskStoreNotMapped.putBytes(blockId, byteBuffer)
    val notMapped = diskStoreNotMapped.getBytes(blockId)

    // Not possible to do isInstanceOf due to visibility of HeapByteBuffer
    assert(notMapped.getChunks().forall(_.getClass.getName.endsWith("HeapByteBuffer")),
      "Expected HeapByteBuffer for un-mapped read")
    assert(mapped.getChunks().forall(_.isInstanceOf[MappedByteBuffer]),
      "Expected MappedByteBuffer for mapped read")

    def arrayFromByteBuffer(in: ByteBuffer): Array[Byte] = {
      val array = new Array[Byte](in.remaining())
      in.get(array)
      array
    }

    assert(Arrays.equals(mapped.toArray, bytes))
    assert(Arrays.equals(notMapped.toArray, bytes))
  }
}
