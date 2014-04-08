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

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.sql.columnar._

private[sql] case object PassThrough extends CompressionScheme {
  override val typeId = 0

  override def supports(columnType: ColumnType[_, _]) = true

  override def encoder = new this.Encoder

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new this.Decoder(buffer, columnType)
  }

  class Encoder extends compression.Encoder {
    override def uncompressedSize = 0

    override def compressedSize = 0

    override def compress[T <: NativeType](
        from: ByteBuffer,
        to: ByteBuffer,
        columnType: ColumnType[T, T#JvmType]) = {

      // Writes compression type ID and copies raw contents
      to.putInt(PassThrough.typeId).put(from).rewind()
      to
    }
  }

  class Decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    override def next() = columnType.extract(buffer)

    override def hasNext = buffer.hasRemaining
  }
}

private[sql] case object RunLengthEncoding extends CompressionScheme {
  override def typeId = 1

  override def encoder = new this.Encoder

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new this.Decoder(buffer, columnType)
  }

  override def supports(columnType: ColumnType[_, _]) = columnType match {
    case INT | LONG | SHORT | BYTE | STRING | BOOLEAN => true
    case _ => false
  }

  class Encoder extends compression.Encoder {
    private var _uncompressedSize = 0
    private var _compressedSize = 0

    // Using `MutableRow` to store the last value to avoid boxing/unboxing cost.
    private val lastValue = new GenericMutableRow(1)
    private var lastRun = 0

    override def uncompressedSize = _uncompressedSize

    override def compressedSize = _compressedSize

    override def gatherCompressibilityStats[T <: NativeType](
        value: T#JvmType,
        columnType: ColumnType[T, T#JvmType]) {

      val actualSize = columnType.actualSize(value)
      _uncompressedSize += actualSize

      if (lastValue.isNullAt(0)) {
        columnType.setField(lastValue, 0, value)
        lastRun = 1
        _compressedSize += actualSize + 4
      } else {
        if (columnType.getField(lastValue, 0) == value) {
          lastRun += 1
        } else {
          _compressedSize += actualSize + 4
          columnType.setField(lastValue, 0, value)
          lastRun = 1
        }
      }
    }

    override def compress[T <: NativeType](
        from: ByteBuffer,
        to: ByteBuffer,
        columnType: ColumnType[T, T#JvmType]) = {

      to.putInt(RunLengthEncoding.typeId)

      if (from.hasRemaining) {
        var currentValue = columnType.extract(from)
        var currentRun = 1

        while (from.hasRemaining) {
          val value = columnType.extract(from)

          if (value == currentValue) {
            currentRun += 1
          } else {
            // Writes current run
            columnType.append(currentValue, to)
            to.putInt(currentRun)

            // Resets current run
            currentValue = value
            currentRun = 1
          }
        }

        // Writes the last run
        columnType.append(currentValue, to)
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

    override def next() = {
      if (valueCount == run) {
        currentValue = columnType.extract(buffer)
        run = buffer.getInt()
        valueCount = 1
      } else {
        valueCount += 1
      }

      currentValue
    }

    override def hasNext = buffer.hasRemaining
  }
}

private[sql] case object DictionaryEncoding extends CompressionScheme {
  override def typeId: Int = 2

  // 32K unique values allowed
  private val MAX_DICT_SIZE = Short.MaxValue - 1

  override def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]) = {
    new this.Decoder[T](buffer, columnType)
  }

  override def encoder = new this.Encoder

  override def supports(columnType: ColumnType[_, _]) = columnType match {
    case INT | LONG | STRING => true
    case _ => false
  }

  class Encoder extends compression.Encoder{
    // Size of the input, uncompressed, in bytes. Note that we only count until the dictionary
    // overflows.
    private var _uncompressedSize = 0

    // If the number of distinct elements is too large, we discard the use of dictionary encoding
    // and set the overflow flag to true.
    private var overflow = false

    // Total number of elements.
    private var count = 0

    // The reverse mapping of _dictionary, i.e. mapping encoded integer to the value itself.
    private var values = new mutable.ArrayBuffer[Any](1024)

    // The dictionary that maps a value to the encoded short integer.
    private val dictionary = mutable.HashMap.empty[Any, Short]

    // Size of the serialized dictionary in bytes. Initialized to 4 since we need at least an `Int`
    // to store dictionary element count.
    private var dictionarySize = 4

    override def gatherCompressibilityStats[T <: NativeType](
        value: T#JvmType,
        columnType: ColumnType[T, T#JvmType]) {

      if (!overflow) {
        val actualSize = columnType.actualSize(value)
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

    override def compress[T <: NativeType](
        from: ByteBuffer,
        to: ByteBuffer,
        columnType: ColumnType[T, T#JvmType]) = {

      if (overflow) {
        throw new IllegalStateException(
          "Dictionary encoding should not be used because of dictionary overflow.")
      }

      to.putInt(DictionaryEncoding.typeId)
        .putInt(dictionary.size)

      var i = 0
      while (i < values.length) {
        columnType.append(values(i).asInstanceOf[T#JvmType], to)
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
        val mirror = runtimeMirror(getClass.getClassLoader)
        ClassTag[T#JvmType](mirror.runtimeClass(columnType.scalaTag.tpe))
      }

      Array.fill(buffer.getInt()) {
        columnType.extract(buffer)
      }
    }

    override def next() = dictionary(buffer.getShort())

    override def hasNext = buffer.hasRemaining
  }
}
