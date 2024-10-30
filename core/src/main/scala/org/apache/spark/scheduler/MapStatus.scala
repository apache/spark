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
import org.apache.spark.internal.config
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * A common trait between [[MapStatus]] and [[MergeStatus]]. This allows us to reuse existing
 * code to handle MergeStatus inside MapOutputTracker.
 */
private[spark] trait ShuffleOutputStatus

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task has shuffle files stored on as well as the sizes of outputs for each reducer, for passing
 * on to the reduce tasks.
 */
private[spark] sealed trait MapStatus extends ShuffleOutputStatus {
  /** Location where this task output is. */
  def location: BlockManagerId

  def updateLocation(newLoc: BlockManagerId): Unit

  /**
   * Estimated size for the reduce block, in bytes.
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
   */
  def getSizeForBlock(reduceId: Int): Long

  /**
   * The unique ID of this shuffle map task, if spark.shuffle.useOldFetchProtocol enabled we use
   * partitionId of the task or taskContext.taskAttemptId is used.
   */
  def mapId: Long
}


private[spark] object MapStatus {

  /**
   * Min partition number to use [[HighlyCompressedMapStatus]]. A bit ugly here because in test
   * code we can't assume SparkEnv.get exists.
   */
  private lazy val minPartitionsToUseHighlyCompressMapStatus = Option(SparkEnv.get)
    .map(_.conf.get(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS))
    .getOrElse(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS.defaultValue.get)

  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long): MapStatus = {
    if (uncompressedSizes.length > minPartitionsToUseHighlyCompressMapStatus) {
      HighlyCompressedMapStatus(loc, uncompressedSizes, mapTaskId)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes, mapTaskId)
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
      math.min(255, math.ceil(math.log(size.toDouble) / math.log(LOG_BASE)).toInt).toByte
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
 * @param _mapTaskId unique task id for the task
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte],
    private[this] var _mapTaskId: Long)
  extends MapStatus with Externalizable {

  // For deserialization only
  protected def this() = this(null, null.asInstanceOf[Array[Byte]], -1)

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long], mapTaskId: Long) = {
    this(loc, uncompressedSizes.map(MapStatus.compressSize), mapTaskId)
  }

  override def location: BlockManagerId = loc

  override def updateLocation(newLoc: BlockManagerId): Unit = {
    loc = newLoc
  }

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def mapId: Long = _mapTaskId

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
    out.writeLong(_mapTaskId)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
    _mapTaskId = in.readLong()
  }
}

/**
 * A [[MapStatus]] implementation that stores the accurate size of huge blocks, which are larger
 * than spark.shuffle.accurateBlockThreshold. It stores the average size of other non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.
 *
 * @param loc location where the task is being executed
 * @param numNonEmptyBlocks the number of non-empty blocks
 * @param emptyBlocks a bitmap tracking which blocks are empty
 * @param avgSize average size of the non-empty and non-huge blocks
 * @param hugeBlockSizes sizes of huge blocks by their reduceId.
 * @param _mapTaskId unique task id for the task
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long,
    private[this] var hugeBlockSizes: scala.collection.Map[Int, Byte],
    private[this] var _mapTaskId: Long)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || hugeBlockSizes.size > 0
    || numNonEmptyBlocks == 0 || _mapTaskId > 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1, null, -1)  // For deserialization only

  override def location: BlockManagerId = loc

  override def updateLocation(newLoc: BlockManagerId): Unit = {
    loc = newLoc
  }

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

  override def mapId: Long = _mapTaskId

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.serialize(out)
    out.writeLong(avgSize)
    out.writeInt(hugeBlockSizes.size)
    hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
    out.writeLong(_mapTaskId)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    numNonEmptyBlocks = -1 // SPARK-32436 Scala 2.13 doesn't initialize this during deserialization
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.deserialize(in)
    avgSize = in.readLong()
    val count = in.readInt()
    val hugeBlockSizesImpl = mutable.Map.empty[Int, Byte]
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesImpl(block) = size
    }
    hugeBlockSizes = hugeBlockSizesImpl
    _mapTaskId = in.readLong()
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long): HighlyCompressedMapStatus = {
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
    val accurateBlockSkewedFactor = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.defaultValue.get)
    val shuffleAccurateBlockThreshold =
      Option(SparkEnv.get)
        .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD))
        .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.defaultValue.get)
    val threshold =
      if (accurateBlockSkewedFactor > 0) {
        val sortedSizes = uncompressedSizes.sorted
        val medianSize: Long = Utils.median(sortedSizes, true)
        val maxAccurateSkewedBlockNumber =
          Math.min(
            Option(SparkEnv.get)
              .map(_.conf.get(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER))
              .getOrElse(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get),
            totalNumBlocks
          )
        val skewSizeThreshold =
          Math.max(
            medianSize * accurateBlockSkewedFactor,
            sortedSizes(totalNumBlocks - maxAccurateSkewedBlockNumber).toDouble
          )
        Math.min(shuffleAccurateBlockThreshold.toDouble, skewSizeThreshold)
      } else {
        // Disable skew detection if accurateBlockSkewedFactor <= 0
        shuffleAccurateBlockThreshold.toDouble
      }

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
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize,
      hugeBlockSizes, mapTaskId)
  }
}
