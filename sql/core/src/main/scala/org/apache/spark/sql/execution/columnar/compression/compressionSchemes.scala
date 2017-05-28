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

package org.apache.spark.sql.execution.columnar.compression

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable

import org.apache.parquet.column.values.bitpacking.Packer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

private[columnar] case object PassThrough extends CompressionScheme {
  override val typeId = 0

  override def supports(columnType: ColumnType[_]): Boolean = true

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T] = {
    new this.Encoder[T](columnType)
  }

  override def decoder[T <: AtomicType](
      buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T] = {
    new this.Decoder(buffer, columnType)
  }

  class Encoder[T <: AtomicType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    override def uncompressedSize: Int = 0

    override def compressedSize: Int = 0

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      // Writes compression type ID and copies raw contents
      to.putInt(PassThrough.typeId).put(from).rewind()
      to
    }
  }

  class Decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    override def next(row: InternalRow, ordinal: Int): Unit = {
      columnType.extract(buffer, row, ordinal)
    }

    override def hasNext: Boolean = buffer.hasRemaining
  }
}

private[columnar] case object RunLengthEncoding extends CompressionScheme {
  override val typeId = 1

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T] = {
    new this.Encoder[T](columnType)
  }

  override def decoder[T <: AtomicType](
      buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T] = {
    new this.Decoder(buffer, columnType)
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType match {
    case INT | LONG | SHORT | BYTE | STRING | BOOLEAN => true
    case _ => false
  }

  class Encoder[T <: AtomicType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    private var _uncompressedSize = 0
    private var _compressedSize = 0

    // Using `MutableRow` to store the last value to avoid boxing/unboxing cost.
    private val lastValue = new SpecificInternalRow(Seq(columnType.dataType))
    private var lastRun = 0

    override def uncompressedSize: Int = _uncompressedSize

    override def compressedSize: Int = _compressedSize

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
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

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      to.putInt(RunLengthEncoding.typeId)

      if (from.hasRemaining) {
        val currentValue = new SpecificInternalRow(Seq(columnType.dataType))
        var currentRun = 1
        val value = new SpecificInternalRow(Seq(columnType.dataType))

        columnType.extract(from, currentValue, 0)

        while (from.hasRemaining) {
          columnType.extract(from, value, 0)

          if (value.get(0, columnType.dataType) == currentValue.get(0, columnType.dataType)) {
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

  class Decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    private var run = 0
    private var valueCount = 0
    private var currentValue: T#InternalType = _

    override def next(row: InternalRow, ordinal: Int): Unit = {
      if (valueCount == run) {
        currentValue = columnType.extract(buffer)
        run = ByteBufferHelper.getInt(buffer)
        valueCount = 1
      } else {
        valueCount += 1
      }

      columnType.setField(row, ordinal, currentValue)
    }

    override def hasNext: Boolean = valueCount < run || buffer.hasRemaining
  }
}

private[columnar] case object DictionaryEncoding extends CompressionScheme {
  override val typeId = 2

  // 32K unique values allowed
  val MAX_DICT_SIZE = Short.MaxValue

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : Decoder[T] = {
    new this.Decoder(buffer, columnType)
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T] = {
    new this.Encoder[T](columnType)
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType match {
    case INT | LONG | STRING => true
    case _ => false
  }

  class Encoder[T <: AtomicType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    // Size of the input, uncompressed, in bytes. Note that we only count until the dictionary
    // overflows.
    private var _uncompressedSize = 0

    // If the number of distinct elements is too large, we discard the use of dictionary encoding
    // and set the overflow flag to true.
    private var overflow = false

    // Total number of elements.
    private var count = 0

    // The reverse mapping of _dictionary, i.e. mapping encoded integer to the value itself.
    private var values = new mutable.ArrayBuffer[T#InternalType](1024)

    // The dictionary that maps a value to the encoded short integer.
    private val dictionary = mutable.HashMap.empty[Any, Short]

    // Size of the serialized dictionary in bytes. Initialized to 4 since we need at least an `Int`
    // to store dictionary element count.
    private var dictionarySize = 4

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
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

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
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

    override def uncompressedSize: Int = _uncompressedSize

    override def compressedSize: Int = if (overflow) Int.MaxValue else dictionarySize + count * 2
  }

  class Decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    private val dictionary: Array[Any] = {
      val elementNum = ByteBufferHelper.getInt(buffer)
      Array.fill[Any](elementNum)(columnType.extract(buffer).asInstanceOf[Any])
    }

    override def next(row: InternalRow, ordinal: Int): Unit = {
      columnType.setField(row, ordinal, dictionary(buffer.getShort()).asInstanceOf[T#InternalType])
    }

    override def hasNext: Boolean = buffer.hasRemaining
  }
}

private[columnar] case object BooleanBitSet extends CompressionScheme {
  override val typeId = 3

  val BITS_PER_LONG = 64

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : compression.Decoder[T] = {
    new this.Decoder(buffer).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): compression.Encoder[T] = {
    (new this.Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType == BOOLEAN

  class Encoder extends compression.Encoder[BooleanType.type] {
    private var _uncompressedSize = 0

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
      _uncompressedSize += BOOLEAN.defaultSize
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
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

    override def uncompressedSize: Int = _uncompressedSize

    override def compressedSize: Int = {
      val extra = if (_uncompressedSize % BITS_PER_LONG == 0) 0 else 1
      (_uncompressedSize / BITS_PER_LONG + extra) * 8 + 4
    }
  }

  class Decoder(buffer: ByteBuffer) extends compression.Decoder[BooleanType.type] {
    private val count = ByteBufferHelper.getInt(buffer)

    private var currentWord = 0: Long

    private var visited: Int = 0

    override def next(row: InternalRow, ordinal: Int): Unit = {
      val bit = visited % BITS_PER_LONG

      visited += 1
      if (bit == 0) {
        currentWord = ByteBufferHelper.getLong(buffer)
      }

      row.setBoolean(ordinal, ((currentWord >> bit) & 1) != 0)
    }

    override def hasNext: Boolean = visited < count
  }
}

private[columnar] case object IntDelta extends CompressionScheme {
  override def typeId: Int = 4

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : compression.Decoder[T] = {
    new Decoder(buffer, INT).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): compression.Encoder[T] = {
    (new Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType == INT

  class Encoder extends compression.Encoder[IntegerType.type] {
    protected var _compressedSize: Int = 0
    protected var _uncompressedSize: Int = 0

    override def compressedSize: Int = _compressedSize
    override def uncompressedSize: Int = _uncompressedSize

    private var prevValue: Int = _

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
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

    override def next(row: InternalRow, ordinal: Int): Unit = {
      val delta = buffer.get()
      prev = if (delta > Byte.MinValue) prev + delta else ByteBufferHelper.getInt(buffer)
      row.setInt(ordinal, prev)
    }
  }
}

private[columnar] case object LongDelta extends CompressionScheme {
  override def typeId: Int = 5

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : compression.Decoder[T] = {
    new Decoder(buffer, LONG).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): compression.Encoder[T] = {
    (new Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType == LONG

  class Encoder extends compression.Encoder[LongType.type] {
    protected var _compressedSize: Int = 0
    protected var _uncompressedSize: Int = 0

    override def compressedSize: Int = _compressedSize
    override def uncompressedSize: Int = _uncompressedSize

    private var prevValue: Long = _

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
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

    override def next(row: InternalRow, ordinal: Int): Unit = {
      val delta = buffer.get()
      prev = if (delta > Byte.MinValue) prev + delta else ByteBufferHelper.getLong(buffer)
      row.setLong(ordinal, prev)
    }
  }
}

/**
 * Writes integral-type values with delta encoding and binary packing.
 * The format is as follows:
 *
 *  delta-binary-packing := <header> <block>*
 *  header := <type id> <total value count>
 *  block := <base value> <min delta> <list of bit widths of miniblocks> <miniblocks>
 *
 *  In miniblocks, deltas are written with the fixed bit width that the maximum value has in each
 *  miniblock. If there are values 3, 1, 5, 2, and 3, this encoding writes delta values with
 *  3-bit widths in a miniblock.
 */
private[columnar] object DeltaBinaryPacking {

  val NUM_VALUES_IN_BLOCK: Int = 1024
  val NUM_VALUES_IN_MINIBLOCK: Int = 64
  val NUM_MINIBLOCKS_IN_BLOCK: Int = numMiniBlocks(NUM_VALUES_IN_BLOCK)

  def numMiniBlocks(n: Int): Int = {
    (n + NUM_VALUES_IN_MINIBLOCK - 1) / NUM_VALUES_IN_MINIBLOCK
  }
}

private[columnar] case object IntDeltaBinaryPacking extends CompressionScheme {
  import DeltaBinaryPacking._

  private val nativeOrderPacker = if (ByteOrder.nativeOrder.equals(ByteOrder.BIG_ENDIAN)) {
    Packer.BIG_ENDIAN
  } else {
    Packer.LITTLE_ENDIAN
  }

  override def typeId: Int = 6

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : compression.Decoder[T] = {
    new Decoder(buffer, INT).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): compression.Encoder[T] = {
    (new Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType == INT

  class Encoder extends compression.Encoder[IntegerType.type] {
    override def compressedSize: Int = {
      if (compressInProgress) {
        totalBlocksSize += calculateSizeForBufferingValues()
        compressInProgress = false
      }
      totalBlocksSize
    }

    override def uncompressedSize: Int = totalValueCount * 4

    private var compressInProgress: Boolean = false
    private var totalValueCount: Int = 0
    private var totalBlocksSize: Int = 4 // Initially includes header size
    private var baseValue: Int = _
    private var prevValue: Int = _
    private var minDeltaInCurrentBlock: Int = Integer.MAX_VALUE
    private var deltaValuesToFlush: Int = 0
    private val deltaValuesBuffer = new Array[Int](NUM_VALUES_IN_BLOCK)
    // Uses this buffer to pack 8 values simultaneously in a miniblock.
    // Since a maximum bit width is 32, the buffer size requires 32 bytes = 4 bytes multiplied by 8.
    private val miniBlockBufferInByte = new Array[Byte](32)
    private val bitWidths = new Array[Int](NUM_MINIBLOCKS_IN_BLOCK)

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
      val value = row.getInt(ordinal)

      totalValueCount += 1

      if (!compressInProgress) {
        prevValue = value
        compressInProgress = true
        return
      }

      val delta = value - prevValue
      deltaValuesBuffer(deltaValuesToFlush) = delta
      deltaValuesToFlush += 1

      if (delta < minDeltaInCurrentBlock) {
        minDeltaInCurrentBlock = delta
      }

      if (NUM_VALUES_IN_BLOCK == deltaValuesToFlush) {
        totalBlocksSize += calculateSizeForBufferingValues()
        minDeltaInCurrentBlock = Integer.MAX_VALUE
        deltaValuesToFlush = 0
        compressInProgress = false
      } else {
        prevValue = value
      }
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      // First, writes data in a header
      to.putInt(typeId)
      to.putInt(totalValueCount)

      minDeltaInCurrentBlock = Integer.MAX_VALUE
      deltaValuesToFlush = 0
      compressInProgress = false

      while (from.hasRemaining) {
        val value = INT.extract(from)

        if (!compressInProgress) {
          prevValue = value
          baseValue = value
          compressInProgress = true
        } else {
          val delta = value - prevValue
          deltaValuesBuffer(deltaValuesToFlush) = delta
          deltaValuesToFlush += 1

          if (delta < minDeltaInCurrentBlock) {
            minDeltaInCurrentBlock = delta
          }

          if (NUM_VALUES_IN_BLOCK == deltaValuesToFlush) {
            flushBuffer(to)
            minDeltaInCurrentBlock = Integer.MAX_VALUE
            deltaValuesToFlush = 0
            compressInProgress = false
          } else {
            prevValue = value
          }
        }
      }

      // If data left in buffer, flushes them
      if (compressInProgress) {
        flushBuffer(to)
      }

      assert(!to.hasRemaining)

      to.rewind().asInstanceOf[ByteBuffer]
    }

    private[this] def calculateSizeForBufferingValues(): Int = {
      if (compressInProgress) {
        var totalSizeInBytes = 8

        // Calculates and writes bit widths for each miniblock
        val miniBlocksToFlush = numMiniBlocks(deltaValuesToFlush)
        for (i <- 0 until miniBlocksToFlush) {
          val miniStart = i * NUM_VALUES_IN_MINIBLOCK
          val miniEnd = Math.min((i + 1) * NUM_VALUES_IN_MINIBLOCK, deltaValuesToFlush)
          var mask = 0
          for (j <- miniStart until miniEnd) {
            mask |= (deltaValuesBuffer(j) - minDeltaInCurrentBlock)
          }

          val bitWidth = 32 - Integer.numberOfLeadingZeros(mask)
          totalSizeInBytes += bitWidth * (NUM_VALUES_IN_MINIBLOCK / 8)
          totalSizeInBytes += 1
        }

        totalSizeInBytes
      } else {
        0
      }
    }

    private[this] def flushBuffer(out: ByteBuffer): Unit = {
      var writePos = out.position()

      // Converts delta values to be the difference to min delta and all positive
      for (i <- 0 until deltaValuesToFlush) {
        deltaValuesBuffer(i) -= minDeltaInCurrentBlock
      }

      // Writes data in a block header
      Platform.putInt(out.array(), Platform.BYTE_ARRAY_OFFSET + writePos, baseValue)
      Platform.putInt(out.array(), Platform.BYTE_ARRAY_OFFSET + writePos + 4,
        minDeltaInCurrentBlock)
      writePos += 8

      // Calculates and writes bit widths for each miniblock
      val miniBlocksToFlush = numMiniBlocks(deltaValuesToFlush)
      for (i <- 0 until miniBlocksToFlush) {
        val miniStart = i * NUM_VALUES_IN_MINIBLOCK
        val miniEnd = Math.min((i + 1) * NUM_VALUES_IN_MINIBLOCK, deltaValuesToFlush)
        var mask = 0
        for (j <- miniStart until miniEnd) {
          mask |= deltaValuesBuffer(j)
        }

        bitWidths(i) = 32 - Integer.numberOfLeadingZeros(mask)
        Platform.putByte(out.array(), Platform.BYTE_ARRAY_OFFSET + writePos,
          bitWidths(i).asInstanceOf[Byte])
        writePos += 1
      }

      // Finally, writes data in miniblocks
      for (i <- 0 until miniBlocksToFlush) {
        val currentBitWidth = bitWidths(i)
        val packer = nativeOrderPacker.newBytePacker(currentBitWidth)
        val miniStart = i * NUM_VALUES_IN_MINIBLOCK
        val miniEnd = (i + 1) * NUM_VALUES_IN_MINIBLOCK

        val prevWritePos = writePos
        for (j <- miniStart until miniEnd by 8) {
          // Packs integer-typed 8 deltas simultaneously and convert them into a byte array by
          // using the packer implemented in parquet.
          //
          // TODO: To support encoding LONG-typed columns, we need to implement a packer for
          // long-typed deltas by ourselves because parquet has no packer for long-typed ones.
          packer.pack8Values(deltaValuesBuffer, j, miniBlockBufferInByte, 0)
          Platform.copyMemory(miniBlockBufferInByte, Platform.BYTE_ARRAY_OFFSET, out.array,
            Platform.BYTE_ARRAY_OFFSET + writePos, currentBitWidth)
          writePos += currentBitWidth
        }
      }

      out.position(writePos)
    }
  }

  class Decoder(buffer: ByteBuffer, columnType: NativeColumnType[IntegerType.type])
    extends compression.Decoder[IntegerType.type] {

    private var currentPos: Int = 0
    private var totalValueCount = buffer.getInt
    // Buffer for one base value and delta values
    private val valuesBuffer = new Array[Int](NUM_VALUES_IN_BLOCK + 1)
    private val deltaBuffer = new Array[Int](NUM_VALUES_IN_BLOCK)
    private val miniBlockBufferInByte = new Array[Byte](32)
    private val bitWidths = new Array[Int](NUM_MINIBLOCKS_IN_BLOCK)

    if (hasNext) loadNextBatch()

    override def hasNext: Boolean = totalValueCount > 0

    override def next(row: MutableRow, ordinal: Int): Unit = {
      if (currentPos == valuesBuffer.length) {
        loadNextBatch()
        currentPos = 0
      }
      val value = valuesBuffer(currentPos)
      currentPos += 1
      totalValueCount -= 1
      row.setInt(ordinal, value)
    }

    private[this] def loadNextBatch(): Unit = {
      val baseValue = buffer.getInt()
      val minDeltaInCurrentBlock = buffer.getInt()
      val miniBlocksToRead = Math.min(NUM_MINIBLOCKS_IN_BLOCK, numMiniBlocks(totalValueCount - 1))

      for (i <- 0 until miniBlocksToRead) {
        bitWidths(i) = buffer.get().asInstanceOf[Int]
        assert(bitWidths(i) <= 32)
      }

      var writePos = 0
      for (i <- 0 until miniBlocksToRead) {
        val currentBitWidth = bitWidths(i)
        val packer = nativeOrderPacker.newBytePacker(currentBitWidth)

        for (j <- 0 until NUM_VALUES_IN_MINIBLOCK / 8) {
          buffer.get(miniBlockBufferInByte, 0, currentBitWidth)
          packer.unpack8Values(miniBlockBufferInByte, 0, deltaBuffer, writePos)
          writePos += 8
        }
      }

      valuesBuffer(0) = baseValue
      for (i <- 1 to NUM_VALUES_IN_BLOCK) {
        valuesBuffer(i) = valuesBuffer(i - 1) + deltaBuffer(i - 1) + minDeltaInCurrentBlock
      }
    }
  }
}
