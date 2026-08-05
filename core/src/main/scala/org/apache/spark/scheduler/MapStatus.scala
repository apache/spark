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
import java.util.Arrays

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

  /**
   * The checksum value of this shuffle map task, which can be used to evaluate whether the
   * output data has changed across different map task retries.
   */
  def checksumValue: Long = 0
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
      mapTaskId: Long,
      checksumVal: Long = 0): MapStatus = {
    if (uncompressedSizes.length > minPartitionsToUseHighlyCompressMapStatus) {
      HighlyCompressedMapStatus(loc, uncompressedSizes, mapTaskId, checksumVal)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes, mapTaskId, checksumVal)
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
 * @param _checksumVal the checksum value for the task
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte],
    private[this] var _mapTaskId: Long,
    private[this] var _checksumVal: Long = 0)
  extends MapStatus with Externalizable {

  // For deserialization only
  protected def this() = this(null, null.asInstanceOf[Array[Byte]], -1, 0)

  def this(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long,
      checksumVal: Long) = {
    this(loc, uncompressedSizes.map(MapStatus.compressSize), mapTaskId, checksumVal)
  }

  override def location: BlockManagerId = loc

  override def updateLocation(newLoc: BlockManagerId): Unit = {
    loc = newLoc
  }

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def mapId: Long = _mapTaskId

  override def checksumValue: Long = _checksumVal

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
    out.writeLong(_mapTaskId)
    out.writeLong(_checksumVal)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
    _mapTaskId = in.readLong()
    _checksumVal = in.readLong()
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
 * @param hugeBlockIds reduceIds of the accurately recorded blocks, in increasing order
 * @param hugeBlockSizes compressed sizes of the accurately recorded blocks, aligned with
 *                       hugeBlockIds
 * @param skewedBlocks a bitmap tracking which blocks are exactly `skewedBlockSize` bytes
 * @param skewedBlockSize the compressed size shared by every block in `skewedBlocks`
 * @param _mapTaskId unique task id for the task
 * @param _checksumVal checksum value for the task
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long,
    // The driver retains one map status per map task for the lifetime of the shuffle, so the
    // accurate sizes are held as two parallel primitive arrays rather than as a boxed
    // Map[Int, Byte], whose nodes, bucket array and boxed keys cost an order of magnitude more
    // heap per entry.
    private[this] var hugeBlockIds: Array[Int],
    private[this] var hugeBlockSizes: Array[Byte],
    // Blocks tied at the skew cutoff all have the same size, so only their membership has to be
    // recorded. A bitmap costs a fraction of what an id per block would, which is what allows
    // every tied block to be recorded rather than a capped subset of them.
    private[this] var skewedBlocks: RoaringBitmap,
    private[this] var skewedBlockSize: Byte,
    private[this] var _mapTaskId: Long,
    private[this] var _checksumVal: Long = 0)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || hugeBlockIds.length > 0 || !skewedBlocks.isEmpty
    || numNonEmptyBlocks == 0 || _mapTaskId > 0,
    "Average size can only be zero for map stages that produced no output")

  // For deserialization only
  protected def this() = this(null, -1, null, -1, null, null, null, 0, -1, 0)

  override def location: BlockManagerId = loc

  override def updateLocation(newLoc: BlockManagerId): Unit = {
    loc = newLoc
  }

  override def getSizeForBlock(reduceId: Int): Long = {
    assert(hugeBlockIds != null)
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      val i = Arrays.binarySearch(hugeBlockIds, reduceId)
      if (i >= 0) {
        MapStatus.decompressSize(hugeBlockSizes(i))
      } else if (skewedBlocks.contains(reduceId)) {
        MapStatus.decompressSize(skewedBlockSize)
      } else {
        avgSize
      }
    }
  }

  override def mapId: Long = _mapTaskId

  override def checksumValue: Long = _checksumVal

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.serialize(out)
    out.writeLong(avgSize)
    out.writeInt(hugeBlockIds.length)
    var i = 0
    while (i < hugeBlockIds.length) {
      out.writeInt(hugeBlockIds(i))
      out.writeByte(hugeBlockSizes(i))
      i += 1
    }
    skewedBlocks.serialize(out)
    out.writeByte(skewedBlockSize)
    out.writeLong(_mapTaskId)
    out.writeLong(_checksumVal)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    numNonEmptyBlocks = -1 // SPARK-32436 Scala 2.13 doesn't initialize this during deserialization
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.deserialize(in)
    avgSize = in.readLong()
    val count = in.readInt()
    val blockIds = new Array[Int](count)
    val blockSizes = new Array[Byte](count)
    var isSorted = true
    var i = 0
    while (i < count) {
      blockIds(i) = in.readInt()
      blockSizes(i) = in.readByte()
      if (i > 0 && blockIds(i) <= blockIds(i - 1)) {
        isSorted = false
      }
      i += 1
    }
    if (isSorted) {
      hugeBlockIds = blockIds
      hugeBlockSizes = blockSizes
    } else {
      // writeExternal emits the entries in increasing reduceId order, but a status written by an
      // older version may be in any order, and getSizeForBlock binary searches the ids.
      val order = (0 until count).sortBy(blockIds).toArray
      hugeBlockIds = order.map(blockIds)
      hugeBlockSizes = order.map(blockSizes)
    }
    val skewed = new RoaringBitmap()
    skewed.deserialize(in)
    skewedBlocks = if (skewed.isEmpty) HighlyCompressedMapStatus.noSkewedBlocks else skewed
    skewedBlockSize = in.readByte()
    _mapTaskId = in.readLong()
    _checksumVal = in.readLong()
  }
}

private[spark] object HighlyCompressedMapStatus {
  // Most shuffles record no skewed blocks at all, and the driver retains one map status per map
  // task, so those statuses share one empty bitmap rather than each allocating their own. It is
  // never mutated, which is what makes sharing it safe.
  private val noSkewedBlocks = new RoaringBitmap()

  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long,
      checksumVal: Long = 0): HighlyCompressedMapStatus = {
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
    // Sizes at or above `threshold` are recorded accurately. At most
    // SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER of them are strictly larger than `skewCutoff`, and
    // those are held as ids and sizes. There can be arbitrarily many blocks of exactly
    // `skewCutoff`, but they all share that one size, so only their membership is recorded.
    var skewCutoff = Long.MaxValue
    var recordSkewedTies = false
    val threshold =
      if (accurateBlockSkewedFactor > 0) {
        val maxAccurateSkewedBlockNumber =
          Math.min(
            Option(SparkEnv.get)
              .map(_.conf.get(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER))
              .getOrElse(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get),
            totalNumBlocks
          )
        // Only two order statistics are needed here, so they are selected in O(totalNumBlocks)
        // instead of sorting the sizes, which every map task would otherwise pay for.
        val sizes = uncompressedSizes.clone()
        val medianSize: Long = Utils.medianInPlace(sizes)
        val firstAccurateIdx = totalNumBlocks - maxAccurateSkewedBlockNumber
        skewCutoff = Utils.nthSmallest(sizes, firstAccurateIdx)
        val skewSizeThreshold =
          Math.max(medianSize * accurateBlockSkewedFactor, skewCutoff.toDouble)
        val skewThreshold = Math.min(shuffleAccurateBlockThreshold.toDouble, skewSizeThreshold)
        // Every map task of a shuffle sees the same block size distribution, so any rule that
        // records only some of the blocks tied at the cutoff hides the same reducers in every map
        // status, and MapOutputTracker sums those. The hidden ones fall back to `avgSize`, which
        // they raise in the process, so AQE's skew threshold moves up along with the sizes it is
        // compared against. Recording all of the ties is what keeps them visible.
        //
        // The ties are held as a bitmap over the reduce ids, so recording all of them costs at
        // most a bit per reduce partition, which is the bound `emptyBlocks` above has always had.
        // The condition below also keeps the ties a minority of the blocks: it requires the
        // median times the skew factor to be at or below the cutoff, so the median block is
        // smaller than the cutoff and fewer than half of the blocks can be tied at it.
        recordSkewedTies = skewCutoff > 0 && skewThreshold <= skewCutoff.toDouble
        skewThreshold
      } else {
        // Disable skew detection if accurateBlockSkewedFactor <= 0
        shuffleAccurateBlockThreshold.toDouble
      }

    val hugeBlockIds = mutable.ArrayBuilder.make[Int]
    val hugeBlockSizes = mutable.ArrayBuilder.make[Byte]
    val skewedBlocks = new RoaringBitmap()
    while (i < totalNumBlocks) {
      val size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        // Huge blocks are not included in the calculation for average size, thus size for smaller
        // blocks is more accurate. Blocks tied at the cutoff are excluded from it as well: they
        // are recorded accurately, so folding them into the average would both lose nothing and
        // inflate the size reported for the blocks that really are average.
        val isHuge = size >= shuffleAccurateBlockThreshold ||
          (size >= threshold && size > skewCutoff)
        if (isHuge) {
          hugeBlockIds += i
          hugeBlockSizes += MapStatus.compressSize(size)
        } else if (recordSkewedTies && size == skewCutoff) {
          skewedBlocks.add(i)
        } else {
          totalSmallBlockSize += size
          numSmallBlocks += 1
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
    val hasSkewedBlocks = !skewedBlocks.isEmpty
    if (hasSkewedBlocks) {
      skewedBlocks.trim()
      skewedBlocks.runOptimize()
    }
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize,
      hugeBlockIds.result(), hugeBlockSizes.result(),
      if (hasSkewedBlocks) skewedBlocks else noSkewedBlocks,
      if (hasSkewedBlocks) MapStatus.compressSize(skewCutoff) else 0.toByte,
      mapTaskId, checksumVal)
  }
}
