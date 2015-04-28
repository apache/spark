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

import java.io.InputStream
import java.nio.IntBuffer
import java.util.Comparator

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.{JavaSerializerInstance, SerializerInstance}
import org.apache.spark.storage.BlockObjectWriter
import org.apache.spark.util.collection.PartitionedSerializedPairBuffer._

/**
 * Append-only buffer of key-value pairs, each with a corresponding partition ID, that serializes
 * its records upon insert and stores them as raw bytes.
 *
 * We use two data-structures to store the contents. The serialized records are stored in a
 * ChainedBuffer that can expand gracefully as records are added. This buffer is accompanied by a
 * metadata buffer that stores pointers into the data buffer as well as the partition ID of each
 * record. Each entry in the metadata buffer takes up a fixed amount of space.
 *
 * Sorting the collection means swapping entries in the metadata buffer - the record buffer need not
 * be modified at all. Storing the partition IDs in the metadata buffer means that comparisons can
 * happen without following any pointers, which should minimize cache misses.
 *
 * Currently, only sorting by partition is supported.
 *
 * @param metaInitialRecords The initial number of entries in the metadata buffer.
 * @param kvBlockSize The size of each byte buffer in the ChainedBuffer used to store the records.
 * @param serializerInstance the serializer used for serializing inserted records.
 */
private[spark] class PartitionedSerializedPairBuffer[K, V](
    metaInitialRecords: Int,
    kvBlockSize: Int,
    serializerInstance: SerializerInstance)
  extends WritablePartitionedPairCollection[K, V] with SizeTracker {

  if (serializerInstance.isInstanceOf[JavaSerializerInstance]) {
    throw new IllegalArgumentException("PartitionedSerializedPairBuffer does not support" +
      " Java-serialized objects.")
  }

  private var metaBuffer = IntBuffer.allocate(metaInitialRecords * RECORD_SIZE)

  private val kvBuffer: ChainedBuffer = new ChainedBuffer(kvBlockSize)
  private val kvOutputStream = new ChainedBufferOutputStream(kvBuffer)
  private val kvSerializationStream = serializerInstance.serializeStream(kvOutputStream)

  def insert(partition: Int, key: K, value: V): Unit = {
    if (metaBuffer.position == metaBuffer.capacity) {
      growMetaBuffer()
    }

    val keyStart = kvBuffer.size
    if (keyStart < 0) {
      throw new Exception(s"Can't grow buffer beyond ${1 << 31} bytes")
    }
    kvSerializationStream.writeObject[Any](key)
    kvSerializationStream.flush()
    val valueStart = kvBuffer.size
    kvSerializationStream.writeObject[Any](value)
    kvSerializationStream.flush()
    val valueEnd = kvBuffer.size

    metaBuffer.put(keyStart)
    metaBuffer.put(valueStart)
    metaBuffer.put(valueEnd)
    metaBuffer.put(partition)
  }

  /** Double the size of the array because we've reached capacity */
  private def growMetaBuffer(): Unit = {
    if (metaBuffer.capacity.toLong * 2 > Int.MaxValue) {
      // Doubling the capacity would create an array bigger than Int.MaxValue, so don't
      throw new Exception(s"Can't grow buffer beyond ${Int.MaxValue} bytes")
    }
    val newMetaBuffer = IntBuffer.allocate(metaBuffer.capacity * 2)
    newMetaBuffer.put(metaBuffer.array)
    metaBuffer = newMetaBuffer
  }

  /** Iterate through the data in a given order. For this class this is not really destructive. */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    sort(keyComparator)
    val is = orderedInputStream
    val deserStream = serializerInstance.deserializeStream(is)
    new Iterator[((Int, K), V)] {
      var metaBufferPos = 0
      def hasNext: Boolean = metaBufferPos < metaBuffer.position
      def next(): ((Int, K), V) = {
        val key = deserStream.readKey[Any]().asInstanceOf[K]
        val value = deserStream.readValue[Any]().asInstanceOf[V]
        val partition = metaBuffer.get(metaBufferPos + PARTITION)
        metaBufferPos += RECORD_SIZE
        ((partition, key), value)
      }
    }
  }

  override def estimateSize: Long = metaBuffer.capacity * 4 + kvBuffer.capacity

  override def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
    : WritablePartitionedIterator = {
    sort(keyComparator)
    writablePartitionedIterator
  }

  override def writablePartitionedIterator(): WritablePartitionedIterator = {
    new WritablePartitionedIterator {
      // current position in the meta buffer in ints
      var pos = 0

      def writeNext(writer: BlockObjectWriter): Unit = {
        val keyStart = metaBuffer.get(pos + KEY_START)
        val valueEnd = metaBuffer.get(pos + VAL_END)
        pos += RECORD_SIZE
        kvBuffer.read(keyStart, writer, valueEnd - keyStart)
        writer.recordWritten()
      }
      def nextPartition(): Int = metaBuffer.get(pos + PARTITION)
      def hasNext(): Boolean = pos < metaBuffer.position
    }
  }

  // Visible for testing
  def orderedInputStream: OrderedInputStream = {
    new OrderedInputStream(metaBuffer, kvBuffer)
  }

  private def sort(keyComparator: Option[Comparator[K]]): Unit = {
    val comparator = if (keyComparator.isEmpty) {
      new Comparator[Int]() {
        def compare(partition1: Int, partition2: Int): Int = {
          partition1 - partition2
        }
      }
    } else {
      throw new UnsupportedOperationException()
    }

    val sorter = new Sorter(new SerializedSortDataFormat)
    sorter.sort(metaBuffer, 0, metaBuffer.position / RECORD_SIZE, comparator)
  }
}

private[spark] class OrderedInputStream(metaBuffer: IntBuffer, kvBuffer: ChainedBuffer)
    extends InputStream {

  private var metaBufferPos = 0
  private var kvBufferPos =
    if (metaBuffer.position > 0) metaBuffer.get(metaBufferPos + KEY_START) else 0

  override def read(bytes: Array[Byte]): Int = read(bytes, 0, bytes.length)

  override def read(bytes: Array[Byte], offs: Int, len: Int): Int = {
    if (metaBufferPos >= metaBuffer.position) {
      return -1
    }
    val bytesRemainingInRecord = metaBuffer.get(metaBufferPos + VAL_END) - kvBufferPos
    val toRead = math.min(bytesRemainingInRecord, len)
    kvBuffer.read(kvBufferPos, bytes, offs, toRead)
    if (toRead == bytesRemainingInRecord) {
      metaBufferPos += RECORD_SIZE
      if (metaBufferPos < metaBuffer.position) {
        kvBufferPos = metaBuffer.get(metaBufferPos + KEY_START)
      }
    } else {
      kvBufferPos += toRead
    }
    toRead
  }

  override def read(): Int = {
    throw new UnsupportedOperationException()
  }
}

private[spark] class SerializedSortDataFormat extends SortDataFormat[Int, IntBuffer] {

  private val META_BUFFER_TMP = new Array[Int](RECORD_SIZE)

  /** Return the sort key for the element at the given index. */
  override protected def getKey(metaBuffer: IntBuffer, pos: Int): Int = {
    metaBuffer.get(pos * RECORD_SIZE + PARTITION)
  }

  /** Swap two elements. */
  override def swap(metaBuffer: IntBuffer, pos0: Int, pos1: Int): Unit = {
    val iOff = pos0 * RECORD_SIZE
    val jOff = pos1 * RECORD_SIZE
    System.arraycopy(metaBuffer.array, iOff, META_BUFFER_TMP, 0, RECORD_SIZE)
    System.arraycopy(metaBuffer.array, jOff, metaBuffer.array, iOff, RECORD_SIZE)
    System.arraycopy(META_BUFFER_TMP, 0, metaBuffer.array, jOff, RECORD_SIZE)
  }

  /** Copy a single element from src(srcPos) to dst(dstPos). */
  override def copyElement(
      src: IntBuffer,
      srcPos: Int,
      dst: IntBuffer,
      dstPos: Int): Unit = {
    val srcOff = srcPos * RECORD_SIZE
    val dstOff = dstPos * RECORD_SIZE
    System.arraycopy(src.array, srcOff, dst.array, dstOff, RECORD_SIZE)
  }

  /**
   * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
   * Overlapping ranges are allowed.
   */
  override def copyRange(
      src: IntBuffer,
      srcPos: Int,
      dst: IntBuffer,
      dstPos: Int,
      length: Int): Unit = {
    val srcOff = srcPos * RECORD_SIZE
    val dstOff = dstPos * RECORD_SIZE
    System.arraycopy(src.array, srcOff, dst.array, dstOff, RECORD_SIZE * length)
  }

  /**
   * Allocates a Buffer that can hold up to 'length' elements.
   * All elements of the buffer should be considered invalid until data is explicitly copied in.
   */
  override def allocate(length: Int): IntBuffer = {
    IntBuffer.allocate(length * RECORD_SIZE)
  }
}

private[spark] object PartitionedSerializedPairBuffer {
  val KEY_START = 0
  val VAL_START = 1
  val VAL_END = 2
  val PARTITION = 3
  val RECORD_SIZE = Seq(KEY_START, VAL_START, VAL_END, PARTITION).size // num ints of metadata
}
