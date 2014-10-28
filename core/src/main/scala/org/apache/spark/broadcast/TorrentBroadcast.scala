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
import java.nio.ByteBuffer

import scala.collection.JavaConversions.asJavaEnumeration
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{Logging, SparkConf, SparkEnv, SparkException}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{ByteBufferInputStream, Utils}
import org.apache.spark.util.io.ByteArrayChunkOutputStream

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor) as done by the [[org.apache.spark.broadcast.HttpBroadcast]].
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object. On driver, this is set directly by the constructor.
   * On executors, this is reconstructed by [[readObject]], which builds this value by reading
   * blocks from the driver and/or other executors.
   */
  @transient private var _value: T = obj
  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    blockSize = conf.getInt("spark.broadcast.blockSize", 4096) * 1024
  }
  setConf(SparkEnv.get.conf)

  private val broadcastId = BroadcastBlockId(id)

  /** Total number of blocks this broadcast variable contains. */
  private val numBlocks: Int = writeBlocks()

  override protected def getValue() = _value

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   *
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(): Int = {
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    SparkEnv.get.blockManager.putSingle(broadcastId, _value, StorageLevel.MEMORY_AND_DISK,
      tellMaster = false)
    val blocks =
      TorrentBroadcast.blockifyObject(_value, blockSize, SparkEnv.get.serializer, compressionCodec)
    blocks.zipWithIndex.foreach { case (block, i) =>
      SparkEnv.get.blockManager.putBytes(
        BroadcastBlockId(id, "piece" + i),
        block,
        StorageLevel.MEMORY_AND_DISK_SER,
        tellMaster = true)
    }
    blocks.length
  }

  /** Fetch torrent blocks from the driver and/or other executors. */
  private def readBlocks(): Array[ByteBuffer] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[ByteBuffer](numBlocks)
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      def getLocal: Option[ByteBuffer] = bm.getLocalBytes(pieceId)
      def getRemote: Option[ByteBuffer] = bm.getRemoteBytes(pieceId).map { block =>
        // If we found the block from remote executors/driver's BlockManager, put the block
        // in this executor's BlockManager.
        SparkEnv.get.blockManager.putBytes(
          pieceId,
          block,
          StorageLevel.MEMORY_AND_DISK_SER,
          tellMaster = true)
        block
      }
      val block: ByteBuffer = getLocal.orElse(getRemote).getOrElse(
        throw new SparkException(s"Failed to get $pieceId of $broadcastId"))
      blocks(pid) = block
    }
    blocks
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

  /** Used by the JVM when deserializing this object. */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    TorrentBroadcast.synchronized {
      setConf(SparkEnv.get.conf)
      SparkEnv.get.blockManager.getLocal(broadcastId).map(_.data.next()) match {
        case Some(x) =>
          _value = x.asInstanceOf[T]

        case None =>
          logInfo("Started reading broadcast variable " + id)
          val start = System.nanoTime()
          val blocks = readBlocks()
          val time = (System.nanoTime() - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")

          _value =
            TorrentBroadcast.unBlockifyObject[T](blocks, SparkEnv.get.serializer, compressionCodec)
          // Store the merged copy in BlockManager so other tasks on this executor don't
          // need to re-fetch it.
          SparkEnv.get.blockManager.putSingle(
            broadcastId, _value, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
      }
    }
  }
}


private object TorrentBroadcast extends Logging {

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val bos = new ByteArrayChunkOutputStream(blockSize)
    val out: OutputStream = compressionCodec.map(c => c.compressedOutputStream(bos)).getOrElse(bos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    serOut.writeObject[T](obj).close()
    bos.toArrays.map(ByteBuffer.wrap)
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[ByteBuffer],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(
      asJavaEnumeration(blocks.iterator.map(block => new ByteBufferInputStream(block))))
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean) = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
