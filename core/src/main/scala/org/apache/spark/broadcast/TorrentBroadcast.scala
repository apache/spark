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
import java.lang.ref.{Reference, SoftReference, WeakReference}
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKey.BROADCAST_ID
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.{KeyLock, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

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
 * broadcast data (one per executor).
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 * @param serializedOnly if true, do not cache the unserialized value on the driver
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long, serializedOnly: Boolean)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager. We hold
   * a soft reference so that it can be garbage collected if required, as we can always reconstruct
   * in the future. For internal broadcast variables where `serializedOnly = true`, we hold a
   * WeakReference to allow the value to be reclaimed more aggressively.
   */
  @transient private var _value: Reference[T] = _

  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _
  /** Is the execution in local mode. */
  @transient private var isLocalMaster: Boolean = _

  /** Whether to generate checksum for blocks or not. */
  private var checksumEnabled: Boolean = false

  private def setConf(conf: SparkConf): Unit = {
    compressionCodec = if (conf.get(config.BROADCAST_COMPRESS)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.get(config.BROADCAST_BLOCKSIZE).toInt * 1024
    checksumEnabled = conf.get(config.BROADCAST_CHECKSUM)
    isLocalMaster = Utils.isLocalMaster(conf)
  }
  setConf(SparkEnv.get.conf)

  private val broadcastId = BroadcastBlockId(id)

  /** Total number of blocks this broadcast variable contains. */
  private val numBlocks: Int = writeBlocks(obj)

  /** The checksum for all the blocks. */
  private var checksums: Array[Int] = _

  override protected def getValue() = synchronized {
    val memoized: T = if (_value == null) null.asInstanceOf[T] else _value.get
    if (memoized != null) {
      memoized
    } else {
      val newlyRead = readBroadcastBlock()
      _value = if (serializedOnly) {
        new WeakReference[T](newlyRead)
      } else {
        new SoftReference[T](newlyRead)
      }
      newlyRead
    }
  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position(), block.limit()
        - block.position())
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   *
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    val blockManager = SparkEnv.get.blockManager
    if (serializedOnly && !isLocalMaster) {
      // SPARK-39983: When creating a broadcast variable internal to Spark (such as a broadcasted
      // hashed relation), don't store the broadcasted value in the driver's block manager:
      // we do not expect internal broadcast variables' values to be read on the driver, so
      // skipping the store reduces driver memory pressure because we don't add a long-lived
      // reference to the broadcasted object. However, this optimization cannot be applied for
      // local mode (since tasks might run on the driver). To guard against performance
      // regressions if an internal broadcast is accessed on the driver, we store a weak
      // reference to the broadcasted value:
      _value = new WeakReference[T](value)
    } else {
      // Store a copy of the broadcast variable in the driver so that tasks run on the driver
      // do not create a duplicate copy of the broadcast variable's value.
      if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
        throw SparkException.internalError(
          s"Failed to store $broadcastId in BlockManager", category = "BROADCAST")
      }
    }
    try {
      val blocks =
        TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
      if (checksumEnabled) {
        checksums = new Array[Int](blocks.length)
      }
      blocks.zipWithIndex.foreach { case (block, i) =>
        if (checksumEnabled) {
          checksums(i) = calcChecksum(block)
        }
        val pieceId = BroadcastBlockId(id, "piece" + i)
        val bytes = new ChunkedByteBuffer(block.duplicate())
        if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
          throw SparkException.internalError(s"Failed to store $pieceId of $broadcastId " +
            s"in local BlockManager", category = "BROADCAST")
        }
      }
      blocks.length
    } catch {
      case t: Throwable =>
        // scalastyle:off line.size.limit
        logError(log"Store broadcast ${MDC(BROADCAST_ID, broadcastId)} fail, remove all pieces of the broadcast")
        // scalastyle:on
        blockManager.removeBroadcast(id, tellMaster = true)
        throw t
    }
  }

  /** Fetch torrent blocks from the driver and/or other executors. */
  private def readBlocks(): Array[BlockData] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[BlockData](numBlocks)
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          blocks(pid) = block
          releaseBlockManagerLock(pieceId)
        case None =>
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              if (checksumEnabled) {
                val sum = calcChecksum(b.chunks(0))
                if (sum != checksums(pid)) {
                  throw SparkException.internalError(
                    s"corrupt remote block $pieceId of $broadcastId: $sum != ${checksums(pid)}",
                    category = "BROADCAST")
                }
              }
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw SparkException.internalError(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager",
                  category = "BROADCAST")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw SparkException.internalError(
                s"Failed to get $pieceId of $broadcastId", category = "BROADCAST")
          }
      }
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.torrentBroadcastLock.withLock(broadcastId) {
      // As we only lock based on `broadcastId`, whenever using `broadcastCache`, we should only
      // touch `broadcastId`.
      val broadcastCache = SparkEnv.get.broadcastManager.cachedValues

      Option(broadcastCache.get(broadcastId)).map(_.asInstanceOf[T]).getOrElse {
        setConf(SparkEnv.get.conf)
        val blockManager = SparkEnv.get.blockManager
        blockManager.getLocalValues(broadcastId) match {
          case Some(blockResult) =>
            if (blockResult.data.hasNext) {
              val x = blockResult.data.next().asInstanceOf[T]
              releaseBlockManagerLock(broadcastId)

              if (x != null) {
                broadcastCache.put(broadcastId, x)
              }

              x
            } else {
              throw SparkException.internalError(
                s"Failed to get locally stored broadcast data: $broadcastId",
                category = "BROADCAST")
            }
          case None =>
            val estimatedTotalSize = Utils.bytesToString(numBlocks.toLong * blockSize)
            logInfo(s"Started reading broadcast variable $id with $numBlocks pieces " +
              s"(estimated total size $estimatedTotalSize)")
            val startTimeNs = System.nanoTime()
            val blocks = readBlocks()
            logInfo(s"Reading broadcast variable $id took ${Utils.getUsedTimeNs(startTimeNs)}")

            try {
              val obj = TorrentBroadcast.unBlockifyObject[T](
                blocks.map(_.toInputStream()), SparkEnv.get.serializer, compressionCodec)

              if (!serializedOnly || isLocalMaster || Utils.isInRunningSparkTask) {
                // Store the merged copy in BlockManager so other tasks on this executor don't
                // need to re-fetch it.
                val storageLevel = StorageLevel.MEMORY_AND_DISK
                if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
                  throw SparkException.internalError(
                    s"Failed to store $broadcastId in BlockManager", category = "BROADCAST")
                }
              }

              if (obj != null) {
                broadcastCache.put(broadcastId, obj)
              }

              obj
            } finally {
              blocks.foreach(_.dispose())
            }
        }
      }
    }
  }

  /**
   * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseBlockManagerLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener[Unit](_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

  // Is the unserialized value cached. Exposed for testing.
  private[spark] def hasCachedValue: Boolean = {
    TorrentBroadcast.torrentBroadcastLock.withLock(broadcastId) {
      setConf(SparkEnv.get.conf)
      val blockManager = SparkEnv.get.blockManager
      blockManager.getLocalValues(broadcastId) match {
        case Some(blockResult) if (blockResult.data.hasNext) =>
          val x = blockResult.data.next().asInstanceOf[T]
          releaseBlockManagerLock(broadcastId)
          x != null
        case _ => false
      }
    }
  }
}


private object TorrentBroadcast extends Logging {

  /**
   * A [[KeyLock]] whose key is [[BroadcastBlockId]] to ensure there is only one thread fetching
   * the same [[TorrentBroadcast]] block.
   */
  private val torrentBroadcastLock = new KeyLock[BroadcastBlockId]

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    cbbos.toChunkedByteBuffer.getChunks()
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[InputStream],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
