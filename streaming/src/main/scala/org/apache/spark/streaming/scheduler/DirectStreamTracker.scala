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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.streaming.{Time, StreamingContext}

/**
 * This class manages all the direct streams as well as their processed blocks. The information
 * will output to StreamingListener to better monitoring.
 */
private[streaming] class DirectStreamTracker(ssc: StreamingContext) extends Logging {
  type BlockInfos = Seq[DirectBlockInfo]

  val directInputStreams = ssc.graph.getDirectInputStreams()
  val directInputStreamIds = directInputStreams.map(_.id)

  val directStreamBlockInfos = new mutable.HashMap[Time, mutable.HashMap[Int, BlockInfos]]

  def addBlock(batchTime: Time, streamId: Int, blockInfos: BlockInfos): Unit = synchronized {
    val directStreamMap = directStreamBlockInfos.getOrElseUpdate(batchTime,
      new mutable.HashMap[Int, BlockInfos]())

    if (directStreamMap.contains(streamId)) {
      throw new IllegalStateException(s"Direct stream $streamId for batch $batchTime is already " +
        s"added into the tracker, this is a illegal state")
    }
    directStreamMap += ((streamId, blockInfos))
  }

  def getBlocksOfBatch(batchTime: Time): Map[Int, BlockInfos] = synchronized {
    val blocksOfBatch = directStreamBlockInfos.get(batchTime)
    // Convert mutable HashMap to immutable Map for the caller
    blocksOfBatch.map(_.toMap).getOrElse(Map[Int, BlockInfos]())
  }

  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): BlockInfos = synchronized {
    directStreamBlockInfos.get(batchTime).map {
      _.get(streamId).getOrElse(Seq.empty)
    }.getOrElse(Seq.empty)
  }

  def cleanupOldBatches(batchThreshTime: Time): Unit = synchronized {
    val timesToCleanup = directStreamBlockInfos.keys.filter(_ < batchThreshTime)
    logInfo(s"remove old batch metadata: ${timesToCleanup.mkString(" ")}")

    directStreamBlockInfos --= timesToCleanup
  }
}
