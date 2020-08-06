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
import org.apache.spark.util.Utils

/**
 * The status for the result of shuffle partition merge for each individual reducer partition
 * maintained by the scheduler. The scheduler would separate the MergeStatuses received from
 * ExternalShuffleService into individual MergeStatus which is maintained inside MapOutputTracker
 * to be served to the reducers when they start fetching shuffle partition blocks.
 */
private[spark] class MergeStatus(
    private[this] var loc: BlockManagerId,
    private[this] var mapTracker: RoaringBitmap,
    private[this] var size: Long)
  extends Externalizable with OutputStatus {

  protected def this() = this(null, null, -1) // For deserialization only

  def location: BlockManagerId = loc

  def totalSize: Long = size

  def tracker: RoaringBitmap = mapTracker

  /**
   * Get the list of mapper IDs for missing mapper partition blocks that are not merged.
   * The reducer will use this information to decide which shuffle partition blocks to
   * fetch in the original way.
   */
  def getMissingMaps(numMaps: Int): Seq[Int] = {
    (0 until numMaps).filter(i => !mapTracker.contains(i))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    mapTracker.writeExternal(out)
    out.writeLong(size)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    mapTracker = new RoaringBitmap()
    mapTracker.readExternal(in)
    size = in.readLong()
  }
}

private[spark] object MergeStatus {

  /**
   * Separate a MergeStatuses received from an ExternalShuffleService into individual
   * MergeStatus. The scheduler is responsible for providing the location information
   * for the given ExternalShuffleService.
   */
  def convertMergeStatusesToMergeStatusArr(
      mergeStatuses: MergeStatuses,
      loc: BlockManagerId): Seq[(Int, MergeStatus)] = {
    val mergerLoc = BlockManagerId("", loc.host, loc.port)
    mergeStatuses.bitmaps.zipWithIndex.map {
      case (bitmap, index) =>
        val mergeStatus = new MergeStatus(mergerLoc, bitmap, mergeStatuses.sizes(index))
        (mergeStatuses.reduceIds(index), mergeStatus)
    }
  }

  def apply(loc: BlockManagerId, bitmap: RoaringBitmap, size: Long): MergeStatus = {
    new MergeStatus(loc, bitmap, size)
  }
}
