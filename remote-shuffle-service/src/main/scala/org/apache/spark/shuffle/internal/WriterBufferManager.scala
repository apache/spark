/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal

import com.esotericsoftware.kryo.io.Output
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException
import org.apache.spark.serializer.{SerializationStream, Serializer}

import scala.collection.mutable
import scala.collection.mutable.Map

case class BufferManagerOptions(individualBufferSize: Int, individualBufferMax: Int,
                                bufferSpillThreshold: Int, supportAggregate: Boolean)

case class WriterBufferManagerValue(serializeStream: SerializationStream, output: Output)

class WriteBufferManager[K, V](serializer: Serializer,
                         bufferSize: Int,
                         maxBufferSize: Int,
                         spillSize: Int,
                         numPartitions: Int,
                         createCombiner: Option[V => Any] = None)
    extends RecordSerializationBuffer[K, V]
    with Logging {
  private val partitionBuffers: Array[WriterBufferManagerValue] =
    new Array[WriterBufferManagerValue](numPartitions)

  private var totalBytes = 0

  private val serializerInstance = serializer.newInstance()

  def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    val key: Any = record._1
    val value: Any = createCombiner.map(_.apply(record._2)).getOrElse(record._2)
    var result: mutable.Buffer[(Int, Array[Byte])] = null
    val v = partitionBuffers(partitionId)
    if (v != null) {
      val stream = v.serializeStream
      val oldSize = v.output.position()
      stream.writeKey(key)
      stream.writeValue(value)
      val newSize = v.output.position()
      if (newSize >= bufferSize) {
        // partition buffer is full, add it to the result as spill data
        if (result == null) {
          result = mutable.Buffer[(Int, Array[Byte])]()
        }
        v.serializeStream.flush()
        result.append((partitionId, v.output.toBytes))
        v.serializeStream.close()
        partitionBuffers(partitionId) = null
        totalBytes -= oldSize
      } else {
        totalBytes += (newSize - oldSize)
      }
    }
    else {
      val output = new Output(bufferSize, maxBufferSize)
      val stream = serializerInstance.serializeStream(output)
      stream.writeKey(key)
      stream.writeValue(value)
      val newSize = output.position()
      if (newSize >= bufferSize) {
        // partition buffer is full, add it to the result as spill data
        if (result == null) {
          result = mutable.Buffer[(Int, Array[Byte])]()
        }
        stream.flush()
        result.append((partitionId, output.toBytes))
        stream.close()
      } else {
        partitionBuffers(partitionId) = WriterBufferManagerValue(stream, output)
        totalBytes = totalBytes + newSize
      }
    }

    if (totalBytes >= spillSize) {
      // data for all partitions exceeds threshold, add all data to the result as spill data
      if (result == null) {
        result = mutable.Buffer[(Int, Array[Byte])]()
      }
      val allData = clear()
      result.appendAll(allData)
    }

    if (result == null) {
      Nil
    } else {
      result
    }
  }

  def filledBytes: Int = {
    var i = 0
    var sum = 0
    while (i < partitionBuffers.length) {
      val t = partitionBuffers(i)
      if (t != null) {
        flushStream(t.serializeStream, t.output)
        sum += t.output.position()
      }
      i += 1
    }
    if (sum != totalBytes) {
      throw new RssInvalidDataException(
        s"Inconsistent internal state, total bytes is $totalBytes, but should be $sum")
    }
    totalBytes
  }

  def clear(): Seq[(Int, Array[Byte])] = {
    val result = mutable.Buffer[(Int, Array[Byte])]()
    var i = 0
    while (i < partitionBuffers.length) {
      val t = partitionBuffers(i)
      if (t != null) {
        t.serializeStream.flush()
        result.append((i, t.output.toBytes))
        t.serializeStream.close()
        partitionBuffers(i) = null
      }
      i += 1
    }
    totalBytes = 0
    result
  }

  private def flushStream(serializeStream: SerializationStream, output: Output) = {
    val oldPosition = output.position()
    serializeStream.flush()
    val numBytes = output.position() - oldPosition
    totalBytes += numBytes
  }
}
