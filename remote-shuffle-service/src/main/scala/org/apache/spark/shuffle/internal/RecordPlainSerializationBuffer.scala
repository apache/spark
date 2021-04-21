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

package org.apache.spark.shuffle.internal

import java.io._

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.PartitionedPairBuffer

class RecordPlainSerializationBuffer[K, V](
    serializer: Serializer,
    spillSize: Int)
  extends RecordSerializationBuffer[K, V]
  with Serializable
  with Logging {

  private var partitionedBuffer = createPartitionedPairBuffer()
  private var partitionedBufferSize = 0

  private val serializerInstance = serializer.newInstance()

  override def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    partitionedBuffer.insert(partitionId, record._1, record._2)
    partitionedBufferSize += 1

    val estimatedSize = filledBytes
    if (estimatedSize >= spillSize) {
      serializeBuffer()
    } else {
      Seq.empty
    }
  }

  override def filledBytes: Int = {
    if (partitionedBufferSize == 0) {
      0
    } else {
      partitionedBuffer.estimateSize().intValue()
    }
  }

  override def clear(): Seq[(Int, Array[Byte])] = {
    serializeBuffer()
  }

  private def createPartitionedPairBuffer() = {
    partitionedBufferSize = 0
    // TODO pass initialCapacity to PartitionedPairBuffer
    new PartitionedPairBuffer[K, V]()
  }

  private def serializeBuffer(): Seq[(Int, Array[Byte])] = {
    val itr = partitionedBuffer.partitionedDestructiveSortedIterator(None)
    val result = serializeSortedPartitionedRecords(itr, serializerInstance, spillSize)
    partitionedBuffer = createPartitionedPairBuffer()
    result
  }
}


