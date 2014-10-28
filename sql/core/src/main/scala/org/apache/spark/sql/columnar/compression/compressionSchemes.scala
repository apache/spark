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

package org.apache.spark.sql.columnar.compression

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.runtimeMirror

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{MutableRow, SpecificMutableRow}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.columnar._
import org.apache.spark.util.Utils

private[sql] case object PassThrough extends CompressionScheme {
  override val typeId = 0

  override def supports(columnType: ColumnType[_, _]) = true

  override def encoder[T <: NativeType](columnType: NativeColumnType[T]) = {
    new this.Encoder[T](columnType)
  }

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new this.Decoder(buffer, columnType)
  }

  class Encoder[T <: NativeType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    override def uncompressedSize = 0

    override def compressedSize = 0

    override def compress(from: ByteBuffer, to: ByteBuffer) = {
      // Writes compression type ID and copies raw contents
      to.putInt(PassThrough.typeId).put(from).rewind()
      to
    }
  }

  class Decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    override def next(row: MutableRow, ordinal: Int): Unit = {
      columnType.extract(buffer, row, ordinal)
    }

    override def hasNext = buffer.hasRemaining
  }
}

private[sql] case object RunLengthEncoding extends CompressionScheme {
  override val typeId = 1

  override def encoder[T <: NativeType](columnType: NativeColumnType[T]) = {
    new this.Encoder[T](columnType)
  }

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new this.Decoder(buffer, columnType)
  }

  override def supports(columnType: ColumnType[_, _]) = columnType match {
    case INT | LONG | SHORT | BYTE | STRING | BOOLEAN => true
    case _ => false
  }

  class Encoder[T <: NativeType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    private var _uncompressedSize = 0
    private var _compressedSize = 0

    // Using `MutableRow` to store the last value to avoid boxing/unboxing cost.
    private val lastValue = new SpecificMutableRow(Seq(columnType.dataType))
    private var lastRun = 0

    override def uncompressedSize = _uncompressedSize

    override def compressedSize = _compressedSize

    override def gatherCompressibilityStats(row: Row, ordinal: Int): Unit = {
      val value = columnType.getField(row, ordinal)
      val actualSize = columnType.actualSize(row, ordinal)
      _uncompressedSize += actualSize

      if (lastValue.isNullAt(0)) {
        columnType.copyField(row, ordinal, lastValue, 0)
        lastRun = 1
        _compressedSize += actualSize + 4
      } else {
        if (columnType.getField(lastValue, 0) == value) {
          lastRun += 1
        } else {
          _compressedSize += actualSize + 4
          columnType.copyField(row, ordinal, lastValue, 0)
          lastRun = 1
        }
      }
    }

    override def compress(from: ByteBuffer, to: ByteBuffer) = {
      to.putInt(RunLengthEncoding.typeId)

      if (from.hasRemaining) {
        val currentValue = new SpecificMutableRow(Seq(columnType.dataType))
        var currentRun = 1
        val value = new SpecificMutableRow(Seq(columnType.dataType))

        columnType.extract(from, currentValue, 0)

        while (from.hasRemaining) {
          columnType.extract(from, value, 0)

          if (value.head == currentValue.head) {
            currentRun += 1
          } else {
            // Writes current run
            columnType.append(currentValue, 0, to)
            to.putInt(currentRun)

            // Resets current run
            columnType.copyField(value, 0, currentValue, 0)
            currentRun = 1
          }
        }

        // Writes the last run
        columnType.append(currentValue, 0, to)
        to.putInt(currentRun)
      }

      to.rewind()
      to
    }
  }

  class Decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    private var run = 0
    private var valueCount = 0
    private var currentValue: T#JvmType = _

    override def next(row: MutableRow, ordinal: Int): Unit = {
      if (valueCount == run) {
        currentValue = columnType.extract(buffer)
        run = buffer.getInt()
        valueCount = 1
      } else {
        valueCount += 1
      }

      columnType.setField(row, ordinal, currentValue)
    }

    override def hasNext = valueCount < run || buffer.hasRemaining
  }
}

private[sql] case object DictionaryEncoding extends CompressionScheme {
  override val typeId = 2

  // 32K unique values allowed
  val MAX_DICT_SIZE = Short.MaxValue

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new this.Decoder(buffer, columnType)
  }

  override def encoder[T <: NativeType](columnType: NativeColumnType[T]) = {
    new this.Encoder[T](columnType)
  }

  override def supports(columnType: ColumnType[_, _]) = columnType match {
    case INT | LONG | STRING => true
    case _ => false
  }

  class Encoder[T <: NativeType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    // Size of the input, uncompressed, in bytes. Note that we only count until the dictionary
    // overflows.
    private var _uncompressedSize = 0

    // If the number of distinct elements is too large, we discard the use of dictionary encoding
    // and set the overflow flag to true.
    private var overflow = false

    // Total number of elements.
    private var count = 0

    // The reverse mapping of _dictionary, i.e. mapping encoded integer to the value itself.
    private var values = new mutable.ArrayBuffer[T#JvmType](1024)

    // The dictionary that maps a value to the encoded short integer.
    private val dictionary = mutable.HashMap.empty[Any, Short]

    // Size of the serialized dictionary in bytes. Initialized to 4 since we need at least an `Int`
    // to store dictionary element count.
    private var dictionarySize = 4

    override def gatherCompressibilityStats(row: Row, ordinal: Int): Unit = {
      val value = columnType.getField(row, ordinal)

      if (!overflow) {
        val actualSize = columnType.actualSize(row, ordinal)
        count += 1
        _uncompressedSize += actualSize

        if (!dictionary.contains(value)) {
          if (dictionary.size < MAX_DICT_SIZE) {
            val clone = columnType.clone(value)
            values += clone
            dictionarySize += actualSize
            dictionary(clone) = dictionary.size.toShort
          } else {
            overflow = true
            values.clear()
            dictionary.clear()
          }
        }
      }
    }

    override def compress(from: ByteBuffer, to: ByteBuffer) = {
      if (overflow) {
        throw new IllegalStateException(
          "Dictionary encoding should not be used because of dictionary overflow.")
      }

      to.putInt(DictionaryEncoding.typeId)
        .putInt(dictionary.size)

      var i = 0
      while (i < values.length) {
        columnType.append(values(i), to)
        i += 1
      }

      while (from.hasRemaining) {
        to.putShort(dictionary(columnType.extract(from)))
      }

      to.rewind()
      to
    }

    override def uncompressedSize = _uncompressedSize

    override def compressedSize = if (overflow) Int.MaxValue else dictionarySize + count * 2
  }

  class Decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    private val dictionary = {
      // TODO Can we clean up this mess? Maybe move this to `DataType`?
      implicit val classTag = {
        val mirror = runtimeMirror(Utils.getSparkClassLoader)
        ClassTag[T#JvmType](mirror.runtimeClass(columnType.scalaTag.tpe))
      }

      Array.fill(buffer.getInt()) {
        columnType.extract(buffer)
      }
    }

    override def next(row: MutableRow, ordinal: Int): Unit = {
      columnType.setField(row, ordinal, dictionary(buffer.getShort()))
    }

    override def hasNext = buffer.hasRemaining
  }
}

private[sql] case object BooleanBitSet extends CompressionScheme {
  override val typeId = 3

  val BITS_PER_LONG = 64

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new this.Decoder(buffer).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: NativeType](columnType: NativeColumnType[T]) = {
    (new this.Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_, _]) = columnType == BOOLEAN

  class Encoder extends compression.Encoder[BooleanType.type] {
    private var _uncompressedSize = 0

    override def gatherCompressibilityStats(row: Row, ordinal: Int): Unit = {
      _uncompressedSize += BOOLEAN.defaultSize
    }

    override def compress(from: ByteBuffer, to: ByteBuffer) = {
      to.putInt(BooleanBitSet.typeId)
        // Total element count (1 byte per Boolean value)
        .putInt(from.remaining)

      while (from.remaining >= BITS_PER_LONG) {
        var word = 0: Long
        var i = 0

        while (i < BITS_PER_LONG) {
          if (BOOLEAN.extract(from)) {
            word |= (1: Long) << i
          }
          i += 1
        }

        to.putLong(word)
      }

      if (from.hasRemaining) {
        var word = 0: Long
        var i = 0

        while (from.hasRemaining) {
          if (BOOLEAN.extract(from)) {
            word |= (1: Long) << i
          }
          i += 1
        }

        to.putLong(word)
      }

      to.rewind()
      to
    }

    override def uncompressedSize = _uncompressedSize

    override def compressedSize = {
      val extra = if (_uncompressedSize % BITS_PER_LONG == 0) 0 else 1
      (_uncompressedSize / BITS_PER_LONG + extra) * 8 + 4
    }
  }

  class Decoder(buffer: ByteBuffer) extends compression.Decoder[BooleanType.type] {
    private val count = buffer.getInt()

    private var currentWord = 0: Long

    private var visited: Int = 0

    override def next(row: MutableRow, ordinal: Int): Unit = {
      val bit = visited % BITS_PER_LONG

      visited += 1
      if (bit == 0) {
        currentWord = buffer.getLong()
      }

      row.setBoolean(ordinal, ((currentWord >> bit) & 1) != 0)
    }

    override def hasNext: Boolean = visited < count
  }
}

private[sql] case object IntDelta extends CompressionScheme {
  override def typeId: Int = 4

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new Decoder(buffer, INT).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: NativeType](columnType: NativeColumnType[T]) = {
    (new Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_, _]) = columnType == INT

  class Encoder extends compression.Encoder[IntegerType.type] {
    protected var _compressedSize: Int = 0
    protected var _uncompressedSize: Int = 0

    override def compressedSize = _compressedSize
    override def uncompressedSize = _uncompressedSize

    private var prevValue: Int = _

    override def gatherCompressibilityStats(row: Row, ordinal: Int): Unit = {
      val value = row.getInt(ordinal)
      val delta = value - prevValue

      _compressedSize += 1

      // If this is the first integer to be compressed, or the delta is out of byte range, then give
      // up compressing this integer.
      if (_uncompressedSize == 0 || delta <= Byte.MinValue || delta > Byte.MaxValue) {
        _compressedSize += INT.defaultSize
      }

      _uncompressedSize += INT.defaultSize
      prevValue = value
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      to.putInt(typeId)

      if (from.hasRemaining) {
        var prev = from.getInt()
        to.put(Byte.MinValue)
        to.putInt(prev)

        while (from.hasRemaining) {
          val current = from.getInt()
          val delta = current - prev
          prev = current

          if (Byte.MinValue < delta && delta <= Byte.MaxValue) {
            to.put(delta.toByte)
          } else {
            to.put(Byte.MinValue)
            to.putInt(current)
          }
        }
      }

      to.rewind().asInstanceOf[ByteBuffer]
    }
  }

  class Decoder(buffer: ByteBuffer, columnType: NativeColumnType[IntegerType.type])
    extends compression.Decoder[IntegerType.type] {

    private var prev: Int = _

    override def hasNext: Boolean = buffer.hasRemaining

    override def next(row: MutableRow, ordinal: Int): Unit = {
      val delta = buffer.get()
      prev = if (delta > Byte.MinValue) prev + delta else buffer.getInt()
      row.setInt(ordinal, prev)
    }
  }
}

private[sql] case object LongDelta extends CompressionScheme {
  override def typeId: Int = 5

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new Decoder(buffer, LONG).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: NativeType](columnType: NativeColumnType[T]) = {
    (new Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_, _]) = columnType == LONG

  class Encoder extends compression.Encoder[LongType.type] {
    protected var _compressedSize: Int = 0
    protected var _uncompressedSize: Int = 0

    override def compressedSize = _compressedSize
    override def uncompressedSize = _uncompressedSize

    private var prevValue: Long = _

    override def gatherCompressibilityStats(row: Row, ordinal: Int): Unit = {
      val value = row.getLong(ordinal)
      val delta = value - prevValue

      _compressedSize += 1

      // If this is the first long integer to be compressed, or the delta is out of byte range, then
      // give up compressing this long integer.
      if (_uncompressedSize == 0 || delta <= Byte.MinValue || delta > Byte.MaxValue) {
        _compressedSize += LONG.defaultSize
      }

      _uncompressedSize += LONG.defaultSize
      prevValue = value
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      to.putInt(typeId)

      if (from.hasRemaining) {
        var prev = from.getLong()
        to.put(Byte.MinValue)
        to.putLong(prev)

        while (from.hasRemaining) {
          val current = from.getLong()
          val delta = current - prev
          prev = current

          if (Byte.MinValue < delta && delta <= Byte.MaxValue) {
            to.put(delta.toByte)
          } else {
            to.put(Byte.MinValue)
            to.putLong(current)
          }
        }
      }

      to.rewind().asInstanceOf[ByteBuffer]
    }
  }

  class Decoder(buffer: ByteBuffer, columnType: NativeColumnType[LongType.type])
    extends compression.Decoder[LongType.type] {

    private var prev: Long = _

    override def hasNext: Boolean = buffer.hasRemaining

    override def next(row: MutableRow, ordinal: Int): Unit = {
      val delta = buffer.get()
      prev = if (delta > Byte.MinValue) prev + delta else buffer.getLong()
      row.setLong(ordinal, prev)
    }
  }
}
