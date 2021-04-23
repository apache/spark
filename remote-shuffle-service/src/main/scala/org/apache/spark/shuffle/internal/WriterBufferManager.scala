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
                         createCombiner: Option[V => Any] = None)
    extends RecordSerializationBuffer[K, V]
    with Logging {
  private val map: Map[Int, WriterBufferManagerValue] = Map()

  private var totalBytes = 0

  private val serializerInstance = serializer.newInstance()

  def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    val key: Any = record._1
    val value: Any = createCombiner.map(_.apply(record._2)).getOrElse(record._2)
    var result: mutable.Buffer[(Int, Array[Byte])] = null
    map.get(partitionId) match {
      case Some(v) =>
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
          map.remove(partitionId)
          totalBytes -= oldSize
        } else {
          totalBytes += (newSize - oldSize)
        }
      case None =>
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
          map.put(partitionId, WriterBufferManagerValue(stream, output))
          totalBytes = totalBytes + newSize
        }
    }

    if (totalBytes >= spillSize) {
      // data for all partitions exceeds threshold, add all data to the result as spill data
      if (result == null) {
        result = mutable.Buffer[(Int, Array[Byte])]()
      }
      map.values.foreach(_.serializeStream.flush())
      result.appendAll(map.map(t => (t._1, t._2.output.toBytes)))
      map.foreach(t => t._2.serializeStream.close())
      map.clear()
      totalBytes = 0
    }

    if (result == null) {
      Nil
    } else {
      result
    }
  }

  def filledBytes: Int = {
    map.values.foreach(t => flushStream(t.serializeStream, t.output))
    val sum = map.map(_._2.output.position()).sum
    if (sum != totalBytes) {
      throw new RssInvalidDataException(
        s"Inconsistent internal state, total bytes is $totalBytes, but should be $sum")
    }
    totalBytes
  }

  def clear(): Seq[(Int, Array[Byte])] = {
    map.values.foreach(_.serializeStream.flush())
    val result = map.map(t => (t._1, t._2.output.toBytes)).toSeq
    map.values.foreach(_.serializeStream.close())
    map.clear()
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
