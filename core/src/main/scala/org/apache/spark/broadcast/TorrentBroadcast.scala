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

import java.io._

import scala.math
import scala.util.Random

import org.apache.spark._
import org.apache.spark.storage.{BroadcastBlockId, BroadcastHelperBlockId, StorageLevel}
import org.apache.spark.util.Utils


private[spark] class TorrentBroadcast[T](@transient var value_ : T, isLocal: Boolean, id: Long)
extends Broadcast[T](id) with Logging with Serializable {

  def value = value_

  def broadcastId = BroadcastBlockId(id)

  TorrentBroadcast.synchronized {
    SparkEnv.get.blockManager.putSingle(broadcastId, value_, StorageLevel.MEMORY_AND_DISK, false)
  }

  @transient var arrayOfBlocks: Array[TorrentBlock] = null
  @transient var totalBlocks = -1
  @transient var totalBytes = -1
  @transient var hasBlocks = 0

  if (!isLocal) {
    sendBroadcast()
  }

  def sendBroadcast() {
    var tInfo = TorrentBroadcast.blockifyObject(value_)

    totalBlocks = tInfo.totalBlocks
    totalBytes = tInfo.totalBytes
    hasBlocks = tInfo.totalBlocks

    // Store meta-info
    val metaId = BroadcastHelperBlockId(broadcastId, "meta")
    val metaInfo = TorrentInfo(null, totalBlocks, totalBytes)
    TorrentBroadcast.synchronized {
      SparkEnv.get.blockManager.putSingle(
        metaId, metaInfo, StorageLevel.MEMORY_AND_DISK, true)
    }

    // Store individual pieces
    for (i <- 0 until totalBlocks) {
      val pieceId = BroadcastHelperBlockId(broadcastId, "piece" + i)
      TorrentBroadcast.synchronized {
        SparkEnv.get.blockManager.putSingle(
          pieceId, tInfo.arrayOfBlocks(i), StorageLevel.MEMORY_AND_DISK, true)
      }
    }
  }

  // Called by JVM when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    TorrentBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(broadcastId) match {
        case Some(x) =>
          value_ = x.asInstanceOf[T]

        case None =>
          val start = System.nanoTime
          logInfo("Started reading broadcast variable " + id)

          // Initialize @transient variables that will receive garbage values from the master.
          resetWorkerVariables()

          if (receiveBroadcast(id)) {
            value_ = TorrentBroadcast.unBlockifyObject[T](arrayOfBlocks, totalBytes, totalBlocks)

            // Store the merged copy in cache so that the next worker doesn't need to rebuild it.
            // This creates a tradeoff between memory usage and latency.
            // Storing copy doubles the memory footprint; not storing doubles deserialization cost.
            SparkEnv.get.blockManager.putSingle(
              broadcastId, value_, StorageLevel.MEMORY_AND_DISK, false)

            // Remove arrayOfBlocks from memory once value_ is on local cache
            resetWorkerVariables()
          }  else {
            logError("Reading broadcast variable " + id + " failed")
          }

          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
      }
    }
  }

  private def resetWorkerVariables() {
    arrayOfBlocks = null
    totalBytes = -1
    totalBlocks = -1
    hasBlocks = 0
  }

  def receiveBroadcast(variableID: Long): Boolean = {
    // Receive meta-info
    val metaId = BroadcastHelperBlockId(broadcastId, "meta")
    var attemptId = 10
    while (attemptId > 0 && totalBlocks == -1) {
      TorrentBroadcast.synchronized {
        SparkEnv.get.blockManager.getSingle(metaId) match {
          case Some(x) =>
            val tInfo = x.asInstanceOf[TorrentInfo]
            totalBlocks = tInfo.totalBlocks
            totalBytes = tInfo.totalBytes
            arrayOfBlocks = new Array[TorrentBlock](totalBlocks)
            hasBlocks = 0

          case None =>
            Thread.sleep(500)
        }
      }
      attemptId -= 1
    }
    if (totalBlocks == -1) {
      return false
    }

    // Receive actual blocks
    val recvOrder = new Random().shuffle(Array.iterate(0, totalBlocks)(_ + 1).toList)
    for (pid <- recvOrder) {
      val pieceId = BroadcastHelperBlockId(broadcastId, "piece" + pid)
      TorrentBroadcast.synchronized {
        SparkEnv.get.blockManager.getSingle(pieceId) match {
          case Some(x) =>
            arrayOfBlocks(pid) = x.asInstanceOf[TorrentBlock]
            hasBlocks += 1
            SparkEnv.get.blockManager.putSingle(
              pieceId, arrayOfBlocks(pid), StorageLevel.MEMORY_AND_DISK, true)

          case None =>
            throw new SparkException("Failed to get " + pieceId + " of " + broadcastId)
        }
      }
    }

    (hasBlocks == totalBlocks)
  }

}

private object TorrentBroadcast
extends Logging {

  private var initialized = false
  private var conf: SparkConf = null
  def initialize(_isDriver: Boolean, conf: SparkConf) {
    TorrentBroadcast.conf = conf //TODO: we might have to fix it in tests
    synchronized {
      if (!initialized) {
        initialized = true
      }
    }
  }

  def stop() {
    initialized = false
  }

  lazy val BLOCK_SIZE = conf.getInt("spark.broadcast.blockSize", 4096) * 1024

  def blockifyObject[T](obj: T): TorrentInfo = {
    val byteArray = Utils.serialize[T](obj)
    val bais = new ByteArrayInputStream(byteArray)

    var blockNum = (byteArray.length / BLOCK_SIZE)
    if (byteArray.length % BLOCK_SIZE != 0)
      blockNum += 1

    var retVal = new Array[TorrentBlock](blockNum)
    var blockID = 0

    for (i <- 0 until (byteArray.length, BLOCK_SIZE)) {
      val thisBlockSize = math.min(BLOCK_SIZE, byteArray.length - i)
      var tempByteArray = new Array[Byte](thisBlockSize)
      val hasRead = bais.read(tempByteArray, 0, thisBlockSize)

      retVal(blockID) = new TorrentBlock(blockID, tempByteArray)
      blockID += 1
    }
    bais.close()

    val tInfo = TorrentInfo(retVal, blockNum, byteArray.length)
    tInfo.hasBlocks = blockNum

    tInfo
  }

  def unBlockifyObject[T](arrayOfBlocks: Array[TorrentBlock],
                            totalBytes: Int,
                            totalBlocks: Int): T = {
    val retByteArray = new Array[Byte](totalBytes)
    for (i <- 0 until totalBlocks) {
      System.arraycopy(arrayOfBlocks(i).byteArray, 0, retByteArray,
        i * BLOCK_SIZE, arrayOfBlocks(i).byteArray.length)
    }
    Utils.deserialize[T](retByteArray, Thread.currentThread.getContextClassLoader)
  }

}

private[spark] case class TorrentBlock(
    blockID: Int,
    byteArray: Array[Byte])
  extends Serializable

private[spark] case class TorrentInfo(
    @transient arrayOfBlocks : Array[TorrentBlock],
    totalBlocks: Int,
    totalBytes: Int)
  extends Serializable {

  @transient var hasBlocks = 0
}

/**
 * A [[BroadcastFactory]] that creates a torrent-based implementation of broadcast.
 */
class TorrentBroadcastFactory extends BroadcastFactory {

  def initialize(isDriver: Boolean, conf: SparkConf) { TorrentBroadcast.initialize(isDriver, conf) }

  def newBroadcast[T](value_ : T, isLocal: Boolean, id: Long) =
    new TorrentBroadcast[T](value_, isLocal, id)

  def stop() { TorrentBroadcast.stop() }
}
