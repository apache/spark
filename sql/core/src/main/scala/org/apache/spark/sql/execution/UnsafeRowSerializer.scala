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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.unsafe.Platform

/**
 * Serializer for serializing [[UnsafeRow]]s during shuffle. Since UnsafeRows are already stored as
 * bytes, this serializer simply copies those bytes to the underlying output stream. When
 * deserializing a stream of rows, instances of this serializer mutate and return a single UnsafeRow
 * instance that is backed by an on-heap byte array.
 *
 * Note that this serializer implements only the [[Serializer]] methods that are used during
 * shuffle, so certain [[SerializerInstance]] methods will throw SparkUnsupportedOperationException.
 *
 * @param numFields the number of fields in the row being serialized.
 */
class UnsafeRowSerializer(
                           numFields: Int,
                           sortKeyFields: Option[Int] = None,
                           dataSize: SQLMetric = null) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance =
    new UnsafeRowSerializerInstance(numFields, sortKeyFields, dataSize)
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class UnsafeRowSerializerInstance(
                                           numFields: Int,
                                           sortKeyFields: Option[Int] = None,
                                           dataSize: SQLMetric = null) extends SerializerInstance {
  /**
   * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
   * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
   */
  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] var writeBuffer: Array[Byte] = new Array[Byte](4096)
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val row = value.asInstanceOf[UnsafeRow]
      if (dataSize != null) {
        dataSize.add(row.getSizeInBytes)
      }
      dOut.writeInt(row.getSizeInBytes)
      row.writeToStream(dOut, writeBuffer)
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[SqlKey])
      sortKeyFields.map(_ => {
        val row = key.asInstanceOf[RowKey].row
        if (dataSize != null) {
          dataSize.add(row.getSizeInBytes)
        }
        dOut.writeInt(row.getSizeInBytes)
        row.writeToStream(dOut, writeBuffer)
      })
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw SparkUnsupportedOperationException()
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw SparkUnsupportedOperationException()
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      writeBuffer = null
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    sortKeyFields.map(keyFields => new KeyValueDeserializationStream(in, keyFields, numFields)).
      getOrElse(new ValueOnlyDeserializationStream(in, numFields))
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw SparkUnsupportedOperationException()
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw SparkUnsupportedOperationException()
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw SparkUnsupportedOperationException()
}

class ValueOnlyDeserializationStream(in: InputStream,
                                     numFields: Int) extends DeserializationStream {
  private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))
  // 1024 is a default buffer size; this buffer will grow to accommodate larger rows
  private[this] var rowBuffer: Array[Byte] = new Array[Byte](1024)
  private[this] var row: UnsafeRow = new UnsafeRow(numFields)
  private[this] var rowTuple: (IntKey, UnsafeRow) = (IntKey(0), row)
  private[this] val EOF: Int = -1

  override def asKeyValueIterator: Iterator[(IntKey, UnsafeRow)] = {
    new Iterator[(IntKey, UnsafeRow)] {

      private[this] def readSize(): Int = try {
        dIn.readInt()
      } catch {
        case e: EOFException =>
          dIn.close()
          EOF
      }

      private[this] var rowSize: Int = readSize()
      override def hasNext: Boolean = rowSize != EOF

      override def next(): (IntKey, UnsafeRow) = {
        if (rowBuffer.length < rowSize) {
          rowBuffer = new Array[Byte](rowSize)
        }
        ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
        row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
        rowSize = readSize()
        if (rowSize == EOF) { // We are returning the last row in this stream
          dIn.close()
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
    throw SparkUnsupportedOperationException()
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
    ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
    row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
    row.asInstanceOf[T]
  }

  override def readObject[T: ClassTag](): T = {
    // This method is never called by shuffle code.
    throw SparkUnsupportedOperationException()
  }

  override def close(): Unit = {
    dIn.close()
  }
}

class KeyValueDeserializationStream(in: InputStream,
                                    sortKeyFields: Int,
                                    valNumFields: Int) extends DeserializationStream {
  private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))

  private[this] val EOF: Int = -1

  override def asKeyValueIterator: Iterator[(RowKey, UnsafeRow)] = {
    new Iterator[(RowKey, UnsafeRow)] {

      private[this] def readSize(): Int = try {
        dIn.readInt()
      } catch {
        case e: EOFException =>
          dIn.close()
          EOF
      }

      private[this] var keySize: Int = readSize()
      override def hasNext: Boolean = keySize != EOF

      override def next(): (RowKey, UnsafeRow) = {
        // read key
        val keyBuffer = new Array[Byte](keySize)
        ByteStreams.readFully(dIn, keyBuffer, 0, keySize)
        val keyRow = new UnsafeRow(sortKeyFields)
        keyRow.pointTo(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)

        // read value
        val valSize: Int = readSize()
        val valBuffer = new Array[Byte](valSize)
        ByteStreams.readFully(dIn, valBuffer, 0, valSize)
        val valRow: UnsafeRow = new UnsafeRow(valNumFields)
        valRow.pointTo(valBuffer, Platform.BYTE_ARRAY_OFFSET, valSize)
        keySize = readSize()
        if (keySize == EOF) { // We are returning the last row in this stream
          dIn.close()
        }
        (RowKey(keyRow), valRow)
      }
    }
  }

  override def asIterator: Iterator[Any] = {
    // This method is never called by shuffle code.
    throw SparkUnsupportedOperationException()
  }

  override def readKey[T: ClassTag](): T = {
    val keySize = dIn.readInt()
    val keyBuffer = new Array[Byte](keySize)
    ByteStreams.readFully(dIn, keyBuffer, 0, keySize)
    val keyRow = new UnsafeRow(sortKeyFields)
    keyRow.pointTo(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
    RowKey(keyRow).asInstanceOf[T]
  }

  override def readValue[T: ClassTag](): T = {
    val valSize: Int = dIn.readInt()
    val valBuffer = new Array[Byte](valSize)
    ByteStreams.readFully(dIn, valBuffer, 0, valSize)
    val valRow: UnsafeRow = new UnsafeRow(valNumFields)
    valRow.pointTo(valBuffer, Platform.BYTE_ARRAY_OFFSET, valSize)
    valRow.asInstanceOf[T]
  }

  override def readObject[T: ClassTag](): T = {
    // This method is never called by shuffle code.
    throw SparkUnsupportedOperationException()
  }

  override def close(): Unit = {
    dIn.close()
  }
}
