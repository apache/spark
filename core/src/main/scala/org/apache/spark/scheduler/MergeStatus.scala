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

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.network.shuffle.protocol.MergeStatuses
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * The status for the result of merging shuffle partition blocks per individual shuffle partition
 * maintained by the scheduler. The scheduler would separate the
 * [[org.apache.spark.network.shuffle.protocol.MergeStatuses]] received from
 * ExternalShuffleService into individual [[MergeStatus]] which is maintained inside
 * MapOutputTracker to be served to the reducers when they start fetching shuffle partition
 * blocks. Note that, the reducers are ultimately fetching individual chunks inside a merged
 * shuffle file, as explained in [[org.apache.spark.network.shuffle.RemoteBlockPushResolver]].
 * Between the scheduler maintained MergeStatus and the shuffle service maintained per shuffle
 * partition meta file, we are effectively dividing the metadata for a push-based shuffle into
 * 2 layers. The scheduler would track the top-level metadata at the shuffle partition level
 * with MergeStatus, and the shuffle service would maintain the partition level metadata about
 * how to further divide a merged shuffle partition into multiple chunks with the per-partition
 * meta file. This helps to reduce the amount of data the scheduler needs to maintain for
 * push-based shuffle.
 */
private[spark] class MergeStatus(
    private[this] var loc: BlockManagerId,
    private[this] var _shuffleMergeId: Int,
    private[this] var mapTracker: RoaringBitmap,
    private[this] var size: Long)
  extends Externalizable with ShuffleOutputStatus {

  protected def this() = this(null, -1, null, -1) // For deserialization only

  def location: BlockManagerId = loc

  def shuffleMergeId: Int = _shuffleMergeId

  def totalSize: Long = size

  def tracker: RoaringBitmap = mapTracker

  /**
   * Get the number of missing map outputs for missing mapper partition blocks that are not merged.
   */
  def getNumMissingMapOutputs(numMaps: Int): Int = {
    (0 until numMaps).count(i => !mapTracker.contains(i))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(_shuffleMergeId)
    mapTracker.writeExternal(out)
    out.writeLong(size)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    _shuffleMergeId = in.readInt()
    mapTracker = new RoaringBitmap()
    mapTracker.readExternal(in)
    size = in.readLong()
  }
}

private[spark] object MergeStatus {
  // Dummy number of reduces for the tests where push based shuffle is not enabled
  val SHUFFLE_PUSH_DUMMY_NUM_REDUCES = 1

  /**
   * Separate a MergeStatuses received from an ExternalShuffleService into individual
   * MergeStatus. The scheduler is responsible for providing the location information
   * for the given ExternalShuffleService.
   */
  def convertMergeStatusesToMergeStatusArr(
      mergeStatuses: MergeStatuses,
      loc: BlockManagerId): Seq[(Int, MergeStatus)] = {
    assert(mergeStatuses.bitmaps.length == mergeStatuses.reduceIds.length &&
      mergeStatuses.bitmaps.length == mergeStatuses.sizes.length)
    val mergerLoc = BlockManagerId(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER, loc.host, loc.port)
    val shuffleMergeId = mergeStatuses.shuffleMergeId
    mergeStatuses.bitmaps.zipWithIndex.map {
      case (bitmap, index) =>
        val mergeStatus = new MergeStatus(mergerLoc, shuffleMergeId, bitmap,
          mergeStatuses.sizes(index))
        (mergeStatuses.reduceIds(index), mergeStatus)
    }.toImmutableArraySeq
  }

  def apply(
      loc: BlockManagerId,
      shuffleMergeId: Int,
      bitmap: RoaringBitmap,
      size: Long): MergeStatus = {
    new MergeStatus(loc, shuffleMergeId, bitmap, size)
  }
}
