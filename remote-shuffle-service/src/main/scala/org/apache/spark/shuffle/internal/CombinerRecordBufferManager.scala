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
import java.util.Comparator

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.PartitionedAppendOnlyMap

class CombinerRecordBufferManager[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    serializer: Serializer,
    spillSize: Int)
  extends RecordBufferManager[K, V]
  with Serializable
  with Logging {

  private var partitionedMap = createPartitionedAppendOnlyMap()

  private val serializerInstance = serializer.newInstance()

  override def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte], Int)] = {
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, record._2) else createCombiner(record._2)
    }

    partitionedMap.changeValue((partitionId, record._1), update)

    val estimatedSize = filledBytes
    if (estimatedSize >= spillSize) {
      serializeMap()
    } else {
      Seq.empty
    }
  }

  override def filledBytes: Int = {
    if (partitionedMap.isEmpty) {
      0
    } else {
      partitionedMap.estimateSize().intValue()
    }
  }

  override def clear(): Seq[(Int, Array[Byte], Int)] = {
    serializeMap()
  }

  private def createPartitionedAppendOnlyMap() = {
    // TODO pass initialCapacity to PartitionedAppendOnlyMap
    new PartitionedAppendOnlyMap[K, C]()
  }

  private def serializeMap(): Seq[(Int, Array[Byte], Int)] = {
    val comparator = new Comparator[(Int, K)] {
      override def compare(o1: (Int, K), o2: (Int, K)): Int = {
        Integer.compare(o1._1, o2._1)
      }
    }
    val itr = partitionedMap.destructiveSortedIterator(comparator)
    val result = serializeSortedPartitionedRecords(itr, serializerInstance, spillSize)
    partitionedMap = createPartitionedAppendOnlyMap()
    result
  }
}


