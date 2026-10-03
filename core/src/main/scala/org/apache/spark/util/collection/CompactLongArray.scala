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

/**
 * An exact, paged long array for UI data. Untouched pages contain implicit zeros. Compaction
 * stores a page's minimum and bit-packed offsets, retaining a plain array when packing would
 * be larger. Updating a compacted page expands only that page. Callers provide synchronization.
 */
private[spark] final class CompactLongArray(val length: Int, val blockSize: Int = 256) {
  import CompactLongArray._

  require(length >= 0)
  require(blockSize > 0)

  private val blocks = new Array[Block](((length.toLong + blockSize - 1) / blockSize).toInt)

  def apply(index: Int): Long = {
    checkIndex(index)
    val block = blocks(index / blockSize)
    if (block == null) 0L else block(index % blockSize)
  }

  def update(index: Int, value: Long): Unit = {
    checkIndex(index)
    val blockIndex = index / blockSize
    val offset = index % blockSize
    blocks(blockIndex) match {
      case null if value == 0L =>
      case raw: RawBlock => raw.values(offset) = value
      case block =>
        val size = math.min(blockSize, length - blockIndex * blockSize)
        val values = if (block == null) new Array[Long](size) else block.toArray(size)
        values(offset) = value
        blocks(blockIndex) = new RawBlock(values)
    }
  }

  def compact(): Unit = {
    var index = 0
    while (index < blocks.length) {
      compactBlock(index)
      index += 1
    }
  }

  def compactBlock(index: Int): Unit = {
    blocks(index) match {
      case raw: RawBlock => blocks(index) = pack(raw)
      case _ =>
    }
  }

  def iterator: Iterator[Long] = Iterator.range(0, length).map(apply)

  def toArray: Array[Long] = {
    val result = new Array[Long](length)
    var index = 0
    while (index < length) {
      result(index) = apply(index)
      index += 1
    }
    result
  }

  private def checkIndex(index: Int): Unit = {
    if (index < 0 || index >= length) {
      throw new IndexOutOfBoundsException(s"Index $index outside array length $length")
    }
  }
}

private[spark] object CompactLongArray {
  private sealed trait Block {
    def apply(index: Int): Long

    def toArray(length: Int): Array[Long] = Array.tabulate(length)(apply)
  }

  private final class RawBlock(val values: Array[Long]) extends Block {
    override def apply(index: Int): Long = values(index)
  }

  private final class PackedBlock(base: Long, width: Int, words: Array[Long]) extends Block {
    override def apply(index: Int): Long = {
      if (width == 0) {
        base
      } else {
        val bit = index * width
        val word = bit >>> 6
        val shift = bit & 63
        var value = words(word) >>> shift
        if (shift + width > 64) {
          value |= words(word + 1) << (64 - shift)
        }
        base + (value & (-1L >>> (64 - width)))
      }
    }
  }

  private def pack(raw: RawBlock): Block = {
    val values = raw.values
    var minimum = values(0)
    var maximum = minimum
    var index = 1
    while (index < values.length) {
      minimum = math.min(minimum, values(index))
      maximum = math.max(maximum, values(index))
      index += 1
    }
    if (minimum == 0L && maximum == 0L) {
      return null
    }
    val width = 64 - java.lang.Long.numberOfLeadingZeros(maximum - minimum)
    val wordCount = ((values.length.toLong * width + 63) / 64).toInt
    // Overflow in maximum - minimum requires all 64 bits. Account for the packed header too.
    if (width == 64 || wordCount.toLong * 8 + 24 >= values.length.toLong * 8) {
      return raw
    }
    val words = new Array[Long](wordCount)
    if (width > 0) {
      index = 0
      while (index < values.length) {
        val value = values(index) - minimum
        val bit = index * width
        val word = bit >>> 6
        val shift = bit & 63
        words(word) |= value << shift
        if (shift + width > 64) {
          words(word + 1) |= value >>> (64 - shift)
        }
        index += 1
      }
    }
    new PackedBlock(minimum, width, words)
  }
}
