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

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils
import org.roaringbitmap.RoaringBitmap

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 */
private[spark] sealed trait MapStatus {
  /** Location where this task was run. */
  def location: BlockManagerId

  /**
   * Estimated size for the reduce block, in bytes.
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
   */
  def getSizeForBlock(reduceId: Int): Long
}


private[spark] object MapStatus {

  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): MapStatus = {
    if (uncompressedSizes.length > 2000) {
      HighlyCompressedMapStatus(loc, uncompressedSizes)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes)
    }
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * @param loc location where the task is being executed.
 * @param compressedSizes size of the blocks, indexed by reduce partition id.
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte])
  extends MapStatus with Externalizable {

  protected def this() = this(null, null.asInstanceOf[Array[Byte]])  // For deserialization only

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long]) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize))
  }

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
  }
}

/**
 * A [[MapStatus]] implementation that only stores the average size of non-empty blocks,
 * plus a bitmap for tracking which blocks are empty(isSparse)/non-empty(!isSparse).
 *
 * @param loc location where the task is being executed
 * @param numNonEmptyBlocks the number of non-empty blocks
 * @param markedBlocks a bitmap tracking which blocks are empty(isSparse)/non-empty(!isSparse)
 * @param avgSize average size of the non-empty blocks
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var markedBlocks: RoaringBitmap,
    private[this] var avgSize: Long,
    private[this] var isSparse: Boolean)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1, false)  // For deserialization only

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    if (isSparse ^ markedBlocks.contains(reduceId)) {
      0
    } else {
      avgSize
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    markedBlocks.writeExternal(out)
    out.writeLong(avgSize)
    out.writeBoolean(isSparse)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    markedBlocks = new RoaringBitmap()
    markedBlocks.readExternal(in)
    avgSize = in.readLong()
    isSparse = in.readBoolean()
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var totalSize: Long = 0
    val emptyBlocks = new RoaringBitmap()
    val nonEmptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    while (i < totalNumBlocks) {
      var size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        nonEmptyBlocks.add(i)
        totalSize += size
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numNonEmptyBlocks > 0) {
      totalSize / numNonEmptyBlocks
    } else {
      0
    }
    if (numNonEmptyBlocks < totalNumBlocks / 2) {
      // If non-empty blocks are sparse, we track non-empty blocks and set `isSparse` true.
      nonEmptyBlocks.runOptimize()
      nonEmptyBlocks.trim()
      new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, nonEmptyBlocks, avgSize, isSparse = true)
    } else {
      // If non-empty blocks are dense, we track empty blocks and set `isSparse` false.
      emptyBlocks.runOptimize()
      emptyBlocks.trim()
      new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize, isSparse = false)
    }
  }
}
