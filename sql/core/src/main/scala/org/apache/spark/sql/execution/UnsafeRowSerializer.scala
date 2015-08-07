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

package org.apache.spark.sql.execution

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.serializer.{SerializationStream, DeserializationStream, SerializerInstance, Serializer}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.PlatformDependent

/**
 * Serializer for serializing [[UnsafeRow]]s during shuffle. Since UnsafeRows are already stored as
 * bytes, this serializer simply copies those bytes to the underlying output stream. When
 * deserializing a stream of rows, instances of this serializer mutate and return a single UnsafeRow
 * instance that is backed by an on-heap byte array.
 *
 * Note that this serializer implements only the [[Serializer]] methods that are used during
 * shuffle, so certain [[SerializerInstance]] methods will throw UnsupportedOperationException.
 *
 * @param numFields the number of fields in the row being serialized.
 */
private[sql] class UnsafeRowSerializer(numFields: Int) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new UnsafeRowSerializerInstance(numFields)
  override private[spark] def supportsRelocationOfSerializedObjects: Boolean = true
}

private class UnsafeRowSerializerInstance(numFields: Int) extends SerializerInstance {

  /**
   * Marks the end of a stream written with [[serializeStream()]].
   */
  private[this] val EOF: Int = -1

  /**
   * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
   * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
   * The end of the stream is denoted by a record with the special length `EOF` (-1).
   */
  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] var writeBuffer: Array[Byte] = new Array[Byte](4096)
    // When `out` is backed by ChainedBufferOutputStream, we will get an
    // UnsupportedOperationException when we call dOut.writeInt because it internally calls
    // ChainedBufferOutputStream's write(b: Int), which is not supported.
    // To workaround this issue, we create an array for sorting the int value.
    // To reproduce the problem, use dOut.writeInt(row.getSizeInBytes) and
    // run SparkSqlSerializer2SortMergeShuffleSuite.
    private[this] var intBuffer: Array[Byte] = new Array[Byte](4)
    private[this] val dOut: DataOutputStream = new DataOutputStream(out)

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val row = value.asInstanceOf[UnsafeRow]
      val size = row.getSizeInBytes
      // This part is based on DataOutputStream's writeInt.
      // It is for dOut.writeInt(row.getSizeInBytes).
      intBuffer(0) = ((size >>> 24) & 0xFF).toByte
      intBuffer(1) = ((size >>> 16) & 0xFF).toByte
      intBuffer(2) = ((size >>> 8) & 0xFF).toByte
      intBuffer(3) = ((size >>> 0) & 0xFF).toByte
      dOut.write(intBuffer, 0, 4)

      row.writeToStream(out, writeBuffer)
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      writeBuffer = null
      intBuffer = null
      dOut.writeInt(EOF)
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(in)
      // 1024 is a default buffer size; this buffer will grow to accommodate larger rows
      private[this] var rowBuffer: Array[Byte] = new Array[Byte](1024)
      private[this] var row: UnsafeRow = new UnsafeRow()
      private[this] var rowTuple: (Int, UnsafeRow) = (0, row)

      override def asKeyValueIterator: Iterator[(Int, UnsafeRow)] = {
        new Iterator[(Int, UnsafeRow)] {
          private[this] var rowSize: Int = dIn.readInt()

          override def hasNext: Boolean = rowSize != EOF

          override def next(): (Int, UnsafeRow) = {
            if (rowBuffer.length < rowSize) {
              rowBuffer = new Array[Byte](rowSize)
            }
            ByteStreams.readFully(in, rowBuffer, 0, rowSize)
            row.pointTo(rowBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, numFields, rowSize)
            rowSize = dIn.readInt() // read the next row's size
            if (rowSize == EOF) { // We are returning the last row in this stream
              val _rowTuple = rowTuple
              // Null these out so that the byte array can be garbage collected once the entire
              // iterator has been consumed
              row = null
              rowBuffer = null
              rowTuple = null
              _rowTuple
            } else {
              rowTuple
            }
          }
        }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T: ClassTag](): T = {
        val rowSize = dIn.readInt()
        if (rowBuffer.length < rowSize) {
          rowBuffer = new Array[Byte](rowSize)
        }
        ByteStreams.readFully(in, rowBuffer, 0, rowSize)
        row.pointTo(rowBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, numFields, rowSize)
        row.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
