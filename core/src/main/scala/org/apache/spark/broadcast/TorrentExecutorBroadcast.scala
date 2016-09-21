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

package org.apache.spark.broadcast

import java.io.ObjectOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId, BlockResult, BroadcastBlockId, RDDBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * Different to [[TorrentBroadcast]], this implementation doesn't divide the object to broadcast.
 * In contrast, this implementation performs broadcast on executor side for a RDD. So the results
 * of the RDD does not need to collect first back to the driver before broadcasting.
 *
 * The mechanism is as follows:
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it doesn not exist, it then uses remote fetches to fetch the blocks of the RDD from other
 * executors if available. Once it gets the blocks, it puts the blocks in its own BlockManager,
 * ready for other executors to fetch from.
 *
 * @tparam T The type of the element of RDD to be broadcasted.
 * @tparam U The type of object transformed from the collection of elements of the RDD.
 *
 * @param numBlocks Total number of blocks this broadcast variable contains.
 * @param rddId The id of the RDD to be broadcasted on executors.
 * @param mode The [[BroadcastMode]] object used to transform the result of RDD to the object which
 *             will be stored in the [[BlockManager]].
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentExecutorBroadcast[T: ClassTag, U: ClassTag](
    numBlocks: Int,
    rddId: Int,
    mode: BroadcastMode[T],
    id: Long) extends Broadcast[U](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from other executors.
   */
  @transient private lazy val _value: U = readBroadcastBlock()

  private val broadcastId = BroadcastBlockId(id)

  override protected def getValue() = {
    _value
  }

  /** Fetch torrent blocks from other executors. */
  private def readBlocks(): Array[T] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[Array[T]](numBlocks)
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = RDDBlockId(rddId, pid)
      // First try getLocalValues because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      bm.getLocalValues(pieceId) match {
        case Some(block: BlockResult) =>
          blocks(pid) = block.data.asInstanceOf[Iterator[T]].toArray
        case None =>
          bm.get[T](pieceId) match {
            case Some(b) =>
              val data = b.data.asInstanceOf[Iterator[T]].toArray
              // We found the block from remote executors' BlockManager, so put the block
              // in this executor's BlockManager.
              if (!bm.putIterator(pieceId, data.toIterator,
                  StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }
              blocks(pid) = data
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    blocks.flatMap(x => x)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  private def readBroadcastBlock(): U = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      val blockManager = SparkEnv.get.blockManager
      blockManager.getLocalValues(broadcastId).map(_.data.next()) match {
        case Some(x) =>
          // Found broadcasted value in local [[BlockManager]]. Use it directly.
          releaseLock(broadcastId)
          x.asInstanceOf[U]

        case None =>
          // Not found. Going to fetch the chunks of the broadcasted value from executors.
          logInfo("Started reading broadcast variable " + id)
          val startTimeMs = System.currentTimeMillis()
          val rawInput = readBlocks()
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

          val obj = mode.transform(rawInput.toArray).asInstanceOf[U]
          // Store the merged copy in BlockManager so other tasks on this executor don't
          // need to re-fetch it.
          val storageLevel = StorageLevel.MEMORY_AND_DISK
          if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
            throw new SparkException(s"Failed to store $broadcastId in BlockManager")
          }
          obj
      }
    }
  }

  /**
   * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener(_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }
}
