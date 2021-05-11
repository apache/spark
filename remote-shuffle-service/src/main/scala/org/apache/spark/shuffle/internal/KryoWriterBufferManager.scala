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

import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException
import org.apache.spark.serializer._

import scala.collection.mutable

/*
 * This class is specially optimized for Kyro serializer to reduce memory copy.
 */
class KyroWriteBufferManager[K, V](serializerInstance: KryoSerializerInstance,
                                   bufferSize: Int,
                                   maxBufferSize: Int,
                                   spillSize: Int,
                                   numPartitions: Int,
                                   createCombiner: Option[V => Any] = None)
    extends RecordSerializationBuffer[K, V]
    with Logging {
  private val partitionBuffers: Array[RssKryoSerializationStream] =
    new Array[RssKryoSerializationStream](numPartitions)

  private var totalBytes = 0

  def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte], Int)] = {
    val key: Any = record._1
    val value: Any = createCombiner.map(_.apply(record._2)).getOrElse(record._2)
    var result: mutable.Buffer[(Int, Array[Byte], Int)] = null
    val stream = partitionBuffers(partitionId)
    if (stream != null) {
      val oldSize = stream.position()
      stream.writeKey(key)
      stream.writeValue(value)
      val newSize = stream.position()
      if (newSize >= bufferSize) {
        // partition buffer is full, add it to the result as spill data
        if (result == null) {
          result = mutable.Buffer[(Int, Array[Byte], Int)]()
        }
        val bytes = stream.getBuffer
        val length = stream.position()
        result.append((partitionId, bytes, length))
        stream.close()
        partitionBuffers(partitionId) = null
        totalBytes -= oldSize
      } else {
        totalBytes += (newSize - oldSize)
      }
    }
    else {
      val stream = RssKryoSerializationStream.newStream(serializerInstance,
        bufferSize, maxBufferSize)
      stream.writeKey(key)
      stream.writeValue(value)
      val newSize = stream.position()
      if (newSize >= bufferSize) {
        // partition buffer is full, add it to the result as spill data
        if (result == null) {
          result = mutable.Buffer[(Int, Array[Byte], Int)]()
        }
        val bytes = stream.getBuffer
        val length = stream.position()
        result.append((partitionId, bytes, length))
        stream.close()
      } else {
        partitionBuffers(partitionId) = stream
        totalBytes = totalBytes + newSize
      }
    }

    if (totalBytes >= spillSize) {
      // data for all partitions exceeds threshold, add all data to the result as spill data
      if (result == null) {
        result = mutable.Buffer[(Int, Array[Byte], Int)]()
      }
      clearData(result)
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
        sum += t.position()
      }
      i += 1
    }
    if (sum != totalBytes) {
      throw new RssInvalidDataException(
        s"Inconsistent internal state, total bytes is $totalBytes, but should be $sum")
    }
    totalBytes
  }

  def clear(): Seq[(Int, Array[Byte], Int)] = {
    val result = mutable.Buffer[(Int, Array[Byte], Int)]()
    clearData(result)
    result
  }

  private def clearData(dataCollector: mutable.Buffer[(Int, Array[Byte], Int)]): Unit = {
    var i = 0
    while (i < partitionBuffers.length) {
      val t = partitionBuffers(i)
      if (t != null) {
        if (t.position() > 0) {
          val bytes = t.getBuffer()
          val length = t.position()
          dataCollector.append((i, bytes, length))
        }
        t.close()
        partitionBuffers(i) = null
      }
      i += 1
    }
    totalBytes = 0
  }

}
