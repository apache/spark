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

import scala.collection.mutable

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.SparkEnv
import org.apache.spark.api.shuffle.MapShuffleLocations
import org.apache.spark.internal.config
import org.apache.spark.shuffle.sort.DefaultMapShuffleLocations
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 */
private[spark] sealed trait MapStatus {

  /**
   * Locations where this task stored shuffle blocks.
   *
   * May be null if the MapOutputTracker is not tracking the location of shuffle blocks, leaving it
   * up to the implementation of shuffle plugins to do so.
   */
  def mapShuffleLocations: MapShuffleLocations

  /** Location where the task was run. */
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

  /**
   * Min partition number to use [[HighlyCompressedMapStatus]]. A bit ugly here because in test
   * code we can't assume SparkEnv.get exists.
   */
  private lazy val minPartitionsToUseHighlyCompressMapStatus = Option(SparkEnv.get)
    .map(_.conf.get(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS))
    .getOrElse(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS.defaultValue.get)

  // A temporary concession to the fact that we only expect implementations of shuffle provided by
  // Spark to be storing shuffle locations in the driver, meaning we want to introduce as little
  // serialization overhead as possible in such default cases.
  //
  // If more similar cases arise, consider adding a serialization API for these shuffle locations.
  private val DEFAULT_MAP_SHUFFLE_LOCATIONS_ID: Byte = 0
  private val NON_DEFAULT_MAP_SHUFFLE_LOCATIONS_ID: Byte = 1

  /**
   * Visible for testing.
   */
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): MapStatus = {
    apply(loc, DefaultMapShuffleLocations.get(loc), uncompressedSizes)
  }

  def apply(
      loc: BlockManagerId,
      mapShuffleLocs: MapShuffleLocations,
      uncompressedSizes: Array[Long]): MapStatus = {
    if (uncompressedSizes.length > minPartitionsToUseHighlyCompressMapStatus) {
      HighlyCompressedMapStatus(
          loc, mapShuffleLocs, uncompressedSizes)
    } else {
      new CompressedMapStatus(
          loc, mapShuffleLocs, uncompressedSizes)
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

  def writeLocations(
      loc: BlockManagerId,
      mapShuffleLocs: MapShuffleLocations,
      out: ObjectOutput): Unit = {
    if (mapShuffleLocs != null) {
      out.writeBoolean(true)
      if (mapShuffleLocs.isInstanceOf[DefaultMapShuffleLocations]
          && mapShuffleLocs.asInstanceOf[DefaultMapShuffleLocations].getBlockManagerId == loc) {
        out.writeByte(MapStatus.DEFAULT_MAP_SHUFFLE_LOCATIONS_ID)
      } else {
        out.writeByte(MapStatus.NON_DEFAULT_MAP_SHUFFLE_LOCATIONS_ID)
        out.writeObject(mapShuffleLocs)
      }
    } else {
      out.writeBoolean(false)
    }
    loc.writeExternal(out)
  }

  def readLocations(in: ObjectInput): (BlockManagerId, MapShuffleLocations) = {
    if (in.readBoolean()) {
      val locId = in.readByte()
      if (locId == MapStatus.DEFAULT_MAP_SHUFFLE_LOCATIONS_ID) {
        val blockManagerId = BlockManagerId(in)
        (blockManagerId, DefaultMapShuffleLocations.get(blockManagerId))
      } else {
        val mapShuffleLocations = in.readObject().asInstanceOf[MapShuffleLocations]
        val blockManagerId = BlockManagerId(in)
        (blockManagerId, mapShuffleLocations)
      }
    } else {
      val blockManagerId = BlockManagerId(in)
      (blockManagerId, null)
    }
  }
}

/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * @param loc Location were the task is being executed.
 * @param mapShuffleLocs locations where the task stored its shuffle blocks - may be null.
 * @param compressedSizes size of the blocks, indexed by reduce partition id.
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var mapShuffleLocs: MapShuffleLocations,
    private[this] var compressedSizes: Array[Byte])
  extends MapStatus with Externalizable {

  // For deserialization only
  protected def this() = this(null, null, null.asInstanceOf[Array[Byte]])

  def this(
      loc: BlockManagerId,
      mapShuffleLocations: MapShuffleLocations,
      uncompressedSizes: Array[Long]) {
    this(
        loc,
        mapShuffleLocations,
        uncompressedSizes.map(MapStatus.compressSize))
  }

  override def location: BlockManagerId = loc

  override def mapShuffleLocations: MapShuffleLocations = mapShuffleLocs

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    MapStatus.writeLocations(loc, mapShuffleLocs, out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val (deserializedLoc, deserializedMapShuffleLocs) = MapStatus.readLocations(in)
    loc = deserializedLoc
    mapShuffleLocs = deserializedMapShuffleLocs
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
  }
}

/**
 * A [[MapStatus]] implementation that stores the accurate size of huge blocks, which are larger
 * than spark.shuffle.accurateBlockThreshold. It stores the average size of other non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.
 *
 * @param loc location where the task is being executed
 * @param mapShuffleLocs location where the task stored shuffle blocks - may be null
 * @param numNonEmptyBlocks the number of non-empty blocks
 * @param emptyBlocks a bitmap tracking which blocks are empty
 * @param avgSize average size of the non-empty and non-huge blocks
 * @param hugeBlockSizes sizes of huge blocks by their reduceId.
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var mapShuffleLocs: MapShuffleLocations,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long,
    private[this] var hugeBlockSizes: scala.collection.Map[Int, Byte])
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || hugeBlockSizes.size > 0 || numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, null, -1, null, -1, null)  // For deserialization only

  override def location: BlockManagerId = loc

  override def mapShuffleLocations: MapShuffleLocations = mapShuffleLocs

  override def getSizeForBlock(reduceId: Int): Long = {
    assert(hugeBlockSizes != null)
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      hugeBlockSizes.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size)
        case None => avgSize
      }
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    MapStatus.writeLocations(loc, mapShuffleLocs, out)
    emptyBlocks.writeExternal(out)
    out.writeLong(avgSize)
    out.writeInt(hugeBlockSizes.size)
    hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val (deserializedLoc, deserializedMapShuffleLocs) = MapStatus.readLocations(in)
    loc = deserializedLoc
    mapShuffleLocs = deserializedMapShuffleLocs
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    avgSize = in.readLong()
    val count = in.readInt()
    val hugeBlockSizesImpl = mutable.Map.empty[Int, Byte]
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesImpl(block) = size
    }
    hugeBlockSizes = hugeBlockSizesImpl
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(
      loc: BlockManagerId,
      mapShuffleLocs: MapShuffleLocations,
      uncompressedSizes: Array[Long]): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var numSmallBlocks: Int = 0
    var totalSmallBlockSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    val threshold = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.defaultValue.get)
    val hugeBlockSizes = mutable.Map.empty[Int, Byte]
    while (i < totalNumBlocks) {
      val size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        // Huge blocks are not included in the calculation for average size, thus size for smaller
        // blocks is more accurate.
        if (size < threshold) {
          totalSmallBlockSize += size
          numSmallBlocks += 1
        } else {
          hugeBlockSizes(i) = MapStatus.compressSize(uncompressedSizes(i))
        }
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numSmallBlocks > 0) {
      totalSmallBlockSize / numSmallBlocks
    } else {
      0
    }
    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    new HighlyCompressedMapStatus(
        loc,
        mapShuffleLocs,
        numNonEmptyBlocks,
        emptyBlocks,
        avgSize,
        hugeBlockSizes)
  }
}
