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

import com.esotericsoftware.kryo.io.Output

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.PartitionedAppendOnlyMap

import scala.collection.mutable.Map

class RecordCombinedSerializationBuffer[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    serializer: Serializer,
    spillSize: Int)
  extends RecordSerializationBuffer[K, V]
  with Serializable
  with Logging {

  private var partitionedMap = createPartitionedAppendOnlyMap()

  private val serializerInstance = serializer.newInstance()

  override def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, record._2) else createCombiner(record._2)
    }

    partitionedMap.changeValue((partitionId, record._1), update)

    val estimatedSize = filledBytes
    if (estimatedSize >= spillSize) {
      val result = serializeMap()
      partitionedMap = createPartitionedAppendOnlyMap()
      result
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

  override def clear(): Seq[(Int, Array[Byte])] = {
    val result = serializeMap()
    partitionedMap = new PartitionedAppendOnlyMap[K, C]()
    result
  }

  private def createPartitionedAppendOnlyMap() = {
    // TODO pass initialCapacity to PartitionedAppendOnlyMap
    new PartitionedAppendOnlyMap[K, C]()
  }

  private def serializeMap(): Seq[(Int, Array[Byte])] = {
    val map: Map[Int, WriterBufferManagerValue] = Map()
    val itr = partitionedMap.iterator
    while (itr.hasNext) {
      val item = itr.next()
      val partitionId = item._1._1
      val key: Any = item._1._2
      val value: Any = item._2
      val stream =
      map.get(partitionId) match {
        case Some(v) =>
          v.serializeStream
        case None =>
          // use 1g as max buffer size for Output to avoid KryoException: Buffer overflow
          val output = new Output(spillSize, 1024 * 1024 * 1024)
          val stream = serializerInstance.serializeStream(output)
          map.put(partitionId, WriterBufferManagerValue(stream, output))
          stream
      }
      stream.writeKey(key)
      stream.writeValue(value)
    }

    map.values.foreach(_.serializeStream.flush())
    val result = map.map(t => (t._1, t._2.output.toBytes)).toSeq
    map.values.foreach(_.serializeStream.close())
    map.clear()

    result
  }
}


