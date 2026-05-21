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

package org.apache.spark.shard

import java.io.{InputStream => JInputStream, OutputStream => JOutputStream, SequenceInputStream}
import java.nio.ByteBuffer
import java.util.{Collections, Map => JMap}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.Adler32

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.commons.collections4.map.AbstractReferenceMap._
import org.apache.commons.collections4.map.ReferenceMap

import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{HOST_PORT, SHARD_ID, SHARD_MANAGER_ID, SHARD_SET_ID}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.ShardLookupService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockData, BlockId, ByteBufferBlockData, ShardBlockId, StorageLevel}
import org.apache.spark.util.{IdGenerator, KeyLock, ThreadUtils, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * Per-executor manager for distributed map join shard data.
 *
 * Coordinates shard installation, replication, and RPC-based lookups with the
 * driver-side [[ShardManagerMaster]]. Each executor has one instance, created
 * lazily on first use by a distributed map join operator.
 */
private[spark] class ShardManager(
    val executorId: String,
    rpcEnv: RpcEnv,
    val master: ShardManagerMaster,
    conf: SparkConf,
    val shardLookupService: ShardLookupService,
    isDriver: Boolean)
    extends Logging {

  private var shardManagerId: ShardManagerId = _

  private[shard] val cachedValues: JMap[Any, Any] =
    Collections.synchronizedMap(
      new ReferenceMap(ReferenceStrength.HARD, ReferenceStrength.WEAK)
        .asInstanceOf[JMap[Any, Any]])

  private val managerEndpoint = rpcEnv.setupEndpoint(
    "ShardManagerEndpoint" + ShardManager.ID_GENERATOR.next,
    new ShardManagerEndpoint(rpcEnv, this))

  private val cleanupCallbacks =
    new java.util.concurrent.CopyOnWriteArrayList[ShardCleanupCallback]()

  def registerCleanupCallback(cb: ShardCleanupCallback): Unit = {
    cleanupCallbacks.add(cb)
  }

  private[spark] def invokeCleanupCallbacks(setId: Long): Unit = {
    filterBytesCache.remove(setId)
    val it = cleanupCallbacks.iterator()
    while (it.hasNext) {
      try it.next().onShardSetRemoved(setId)
      catch {
        case NonFatal(e) =>
          logWarning(log"Shard cleanup callback failed for setId=${MDC(SHARD_SET_ID, setId)}", e)
      }
    }
  }

  private val filterBytesCache =
    new ConcurrentHashMap[Long, (Array[Byte], Array[Byte])]()

  private val shardLock = new KeyLock[ShardBlockId]

  private[spark] val lookupEc: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("lookup-join-future", 16))

  private val managerEc: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("shard-manager-future", 8))

  private var blockSize: Int = _
  private[shard] var compressionCodec: Option[CompressionCodec] = _
  private def setConf(conf: SparkConf): Unit = {
    compressionCodec = Some(CompressionCodec.createCodec(conf))
    blockSize = conf.getInt("spark.shard.blockSize", 4 << 20)
  }

  def initialize(appId: String): Unit = {
    setConf(conf)
    shardLookupService.init(this)
    val id =
      ShardManagerId(executorId, shardLookupService.hostName, shardLookupService.port, None)
    val idFromMaster = master.registerShardManager(id, managerEndpoint)
    shardManagerId = if (idFromMaster != null) idFromMaster else id

    logInfo(log"Initialized ShardManager: ${MDC(SHARD_MANAGER_ID, shardManagerId)}")
  }

  def newShardSet(numShards: Int, replicaCount: Int): Long = {
    master.newShardSet(numShards, replicaCount)
  }

  def installShard[T: ClassTag](
      value: T,
      setId: Long,
      id: Int,
      filterBytes: Option[(Array[Byte], Array[Byte])] = None)(
      bfOutput: JOutputStream => Unit): Unit = {
    writeShardBlock(value, setId, id, bfOutput, filterBytes)
    filterBytes.foreach(fb => filterBytesCache.putIfAbsent(setId, fb))
    installReplica(setId, id)
  }

  def installReplicaSet(setId: Long, id: Int): Unit = {
    master.installReplicaSet(setId, id)
  }

  def installReplica(setId: Long, id: Int): Unit = {
    logInfo(log"installReplica: setId=${MDC(SHARD_SET_ID, setId)}, shardId=${MDC(SHARD_ID, id)}")
    shardLookupService.onInstallReplica(this, setId, id)
    reportShardInstalled(setId, id)
  }

  def getFilterBytes(setId: Long, shardId: Int): Option[(Array[Byte], Array[Byte])] = {
    Option(filterBytesCache.get(setId)).orElse {
      val metaId = ShardBlockId(setId, shardId, "meta")
      val bm = SparkEnv.get.blockManager
      bm.getLocalBytes(metaId).flatMap { block =>
        try {
          val meta = SparkEnv.get.serializer.newInstance()
            .deserialize[ShardBlockMeta](block.toByteBuffer())
          meta.filterExprBytes.map { exprBytes =>
            val pair = (exprBytes,
              meta.filterSchemaBytes.getOrElse(Array.empty[Byte]))
            filterBytesCache.putIfAbsent(setId, pair)
            pair
          }
        } finally {
          releaseBlockManagerLock(metaId)
        }
      }
    }
  }

  def loadShardData(setId: Long, id: Int): Unit = {
    readShardBlock[AnyRef](setId, id)
  }

  def reportShardInstalled(setId: Long, id: Int): Unit = {
    master.reportShard(shardManagerId, setId, id)
    logInfo(log"Shard (${MDC(SHARD_SET_ID, setId)}, ${MDC(SHARD_ID, id)}) reported to master")
  }

  def mergeBloomFilter(setId: Long, shardIds: Array[Int], acc: BloomAccumulator): Unit =
    Utils.tryOrIOException {
      val setBloomId = ShardBlockId(setId, -1, "bloom")
      shardLock.withLock(setBloomId) {
        val bm = SparkEnv.get.blockManager
        def openBloomInput(shardId: Int): JInputStream = {
          val bloomId = ShardBlockId(setId, shardId, "bloom")
          val block =
            bm.getLocalBytes(bloomId)
              .map(x => { releaseBlockManagerLock(bloomId); x })
              .getOrElse {
                bm.getRemoteBytes(bloomId) match {
                  case Some(b) =>
                    new ByteBufferBlockData(b, true)
                  case None =>
                    throw new SparkException(s"Failed to get bloom $bloomId")
                }
              }

          try {
            compressionCodec.map(_.compressedInputStream(block.toInputStream()))
              .getOrElse(block.toInputStream())
          } finally {
            block.dispose()
          }
        }

        // merge all shard blooms
        shardIds.foreach(id => acc.add(openBloomInput(id)))

        // write merged bloom
        val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
        val out = compressionCodec.map(_.compressedOutputStream(cbbos)).getOrElse(cbbos)
        try acc.finish(out)
        finally out.close()
        if (!bm.putBytes(
            setBloomId,
            cbbos.toChunkedByteBuffer,
            StorageLevel.MEMORY_AND_DISK_SER,
            tellMaster = true)) {
          throw new SparkException(s"Failed to store bloom $setBloomId in local BlockManager")
        }
      }
    }

  def fetchBloomFilter[T: ClassTag](setId: Long)(bfInput: JInputStream => T): T =
    Utils.tryOrIOException {
      val bloomId = ShardBlockId(setId, -1, "bloom")
      shardLock.withLock(bloomId) {
        Option(cachedValues.get(bloomId)).map(_.asInstanceOf[T]).getOrElse {
          val bm = SparkEnv.get.blockManager
          val block =
            bm.getLocalBytes(bloomId)
              .map(x => { releaseBlockManagerLock(bloomId); x })
              .getOrElse {
                bm.getRemoteBytes(bloomId) match {
                  case Some(b) =>
                    if (!bm
                        .putBytes(
                          bloomId,
                          b,
                          StorageLevel.MEMORY_AND_DISK_SER,
                          tellMaster = true)) {
                      throw new SparkException(
                        s"Failed to store bloom $bloomId in local BlockManager")
                    }
                    new ByteBufferBlockData(b, true)
                  case None =>
                    throw new SparkException(s"Failed to get bloom $bloomId")
                }
              }

          try {
            val input = compressionCodec.map(_.compressedInputStream(block.toInputStream()))
              .getOrElse(block.toInputStream())
            val obj =
              try { bfInput(input) }
              finally { input.close() }

            if (obj != null) {
              cachedValues.put(bloomId, obj)
            }

            obj
          } finally {
            block.dispose()
          }
        }
      }
    }

  def fetchRemoteBatch(
      setId: Long,
      shardId: Int,
      toReqMsg: () => ManagedBuffer): Future[ManagedBuffer] = {
    implicit val ec: ExecutionContextExecutorService = lookupEc
    val message: Future[ManagedBuffer] = Future {
      toReqMsg()
    }

    def locations(refresh: Boolean): List[ShardManagerId] =
      sortLocations(master.getLocations(setId, shardId, refresh)).toList

    def tryWithReqMsg(
        reqMsg: ManagedBuffer,
        locs: List[ShardManagerId],
        tried: Set[ShardManagerId] = Set.empty): Future[ManagedBuffer] =
      locs match {
        case Nil =>
          Future.failed(
            new SparkException(
              s"Remote batch not found of ($setId, $shardId) " +
                s"at locations ${locations(refresh = false)}"))
        case loc :: tail =>
          // Before each attempt, retain once and transfer ownership of that reference
          // to Netty outbound (MessageWithHeader#deallocate will call release).
          reqMsg.retain()
          shardLookupService
            .fetchBatch(loc.host, loc.port, reqMsg)
            .recoverWith { case NonFatal(e) =>
              logWarning(log"Failed to fetch remote batch" +
                log" (${MDC(SHARD_SET_ID, setId)}, ${MDC(SHARD_ID, shardId)})" +
                log" from ${MDC(HOST_PORT, loc)}", e)
              val triedLocs = tried + loc
              val recoverLocs =
                if (tail.isEmpty) locations(refresh = true).filterNot(triedLocs)
                else tail
              tryWithReqMsg(reqMsg, recoverLocs, triedLocs)
            }
      }

    message.flatMap { reqMsg =>
      tryWithReqMsg(reqMsg, locations(refresh = false))
        // Release the base reference after the whole retry chain completes.
        .andThen { case _ => reqMsg.release() }
    }
  }

  def getLocalValue[T: ClassTag](setId: Long, id: Int): T = {
    Option(cachedValues.get(ShardBlockId(setId, id)))
      .map(_.asInstanceOf[T])
      .getOrElse(readShardBlock[T](setId, id))
  }

  def unpersist(setId: Long, blocking: Boolean): Unit = {
    logDebug(log"Unpersisting shard-set ${MDC(SHARD_SET_ID, setId)}")
    SparkEnv.get.blockManager.master.removeShardSet(setId, blocking)
  }

  def stop(): Unit = {
    lookupEc.shutdown()
    managerEc.shutdown()
    shardLookupService.close()
    rpcEnv.stop(managerEndpoint)
    logInfo(log"ShardManager stopped")
  }

  private def sortLocations(locations: Seq[ShardManagerId]): Seq[ShardManagerId] = {
    Random.shuffle(locations).sortBy(loc => if (loc.host == shardManagerId.host) 0 else 1)
  }

  private def writeShardBlock[T: ClassTag](
      value: T,
      setId: Long,
      id: Int,
      bfOutput: JOutputStream => Unit,
      filterBytes: Option[(Array[Byte], Array[Byte])] = None): Unit =
    Utils.tryOrIOException {
      val shardId = ShardBlockId(setId, id)
      shardLock.withLock(shardId) {
        val bm = SparkEnv.get.blockManager
        val blocks = blockifyObject(
          value, blockSize, SparkEnv.get.serializer, compressionCodec)
        val checksums = new Array[Int](blocks.length)
        // write shard
        blocks.zipWithIndex.foreach { case (block, i) =>
          checksums(i) = calcChecksum(block)
          val pieceId = ShardBlockId(setId, id, "piece" + i)
          val bytes = new ChunkedByteBuffer(block.duplicate())
          if (!bm.putBytes(pieceId, bytes, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
            throw new SparkException(
              s"Failed to store shard piece $pieceId in local BlockManager")
          }
        }

        // write meta
        val meta = ShardBlockMeta(
          blocks.length,
          Some(checksums),
          filterBytes.map(_._1),
          filterBytes.map(_._2))
        val metaBytes = new ChunkedByteBuffer(
          SparkEnv.get.serializer.newInstance().serialize(meta))
        val metaId = ShardBlockId(setId, id, "meta")
        if (!bm.putBytes(
            metaId,
            metaBytes,
            StorageLevel.MEMORY_AND_DISK_SER,
            tellMaster = true)) {
          throw new SparkException(s"Failed to store shard meta $metaId in local BlockManager")
        }

        // write bloom
        val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
        val out = compressionCodec.map(_.compressedOutputStream(cbbos)).getOrElse(cbbos)
        try bfOutput(out)
        finally out.close()
        val bloomId = ShardBlockId(setId, id, "bloom")
        if (!bm.putBytes(
            bloomId,
            cbbos.toChunkedByteBuffer,
            StorageLevel.MEMORY_AND_DISK_SER,
            tellMaster = true)) {
          throw new SparkException(s"Failed to store shard bloom $bloomId in local BlockManager")
        }
      }
    }

  private def readShardBlock[T: ClassTag](setId: Long, id: Int): T =
    Utils.tryOrIOException {
      val shardId = ShardBlockId(setId, id)
      shardLock.withLock(shardId) {
        Option(cachedValues.get(shardId)).map(_.asInstanceOf[T]).getOrElse {
          val bm = SparkEnv.get.blockManager
          bm.getLocalValues(shardId) match {
            case Some(blockResult) =>
              if (blockResult.data.hasNext) {
                val x = blockResult.data.next().asInstanceOf[T]
                releaseBlockManagerLock(shardId)

                if (x != null) {
                  cachedValues.put(shardId, x)
                }

                x
              } else {
                throw new SparkException(s"Failed to get locally stored shard: $shardId")
              }

            case None =>
              val blocks = readBlocks(setId, id)
              try {
                val obj = unBlockifyObject[T](
                  blocks.map(_.toInputStream()),
                  SparkEnv.get.serializer,
                  compressionCodec)

                if (obj != null) {
                  cachedValues.put(shardId, obj)
                }

                obj
              } finally {
                blocks.foreach(_.dispose())
              }
          }
        }
      }
    }

  private def readBlocks(setId: Long, id: Int): Array[BlockData] = {
    val bm = SparkEnv.get.blockManager
    // read meta
    val metaId = ShardBlockId(setId, id, "meta")
    val metaBuf = bm.getLocalBytes(metaId) match {
      case Some(block) =>
        val x = block.toByteBuffer()
        releaseBlockManagerLock(metaId)
        x
      case None =>
        bm.getRemoteBytes(metaId) match {
          case Some(b) =>
            if (!bm.putBytes(metaId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
              throw new SparkException(s"Failed to store meta $metaId in local BlockManager")
            }
            b.toByteBuffer
          case None =>
            throw new SparkException(s"Failed to get meta $metaId")
        }
    }
    val meta = SparkEnv.get.serializer.newInstance().deserialize[ShardBlockMeta](metaBuf)

    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[BlockData](meta.numBlocks)
    for (pid <- Random.shuffle(Seq.range(0, meta.numBlocks))) {
      val pieceId = ShardBlockId(setId, id, "piece" + pid)
      logDebug(log"Reading piece ${MDC(SHARD_ID, pieceId)}")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // shard blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          blocks(pid) = block
          releaseBlockManagerLock(pieceId)
        case None =>
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              meta.checksums.foreach { c =>
                val sum = calcChecksum(b.chunks(0))
                if (sum != c(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId: $sum != ${c(pid)}")
                }
              }

              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(s"Failed to store piece $pieceId in local BlockManager")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get piece $pieceId")
          }
      }
    }
    blocks
  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(
        block.array,
        block.arrayOffset + block.position(),
        block.limit()
          - block.position())
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
   * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseBlockManagerLock(blockId: BlockId): Unit = {
    val bm = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener[Unit](_ => bm.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where shard variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // shard variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because shard variables aren't
        // stored off-heap.
        bm.releaseLock(blockId)
    }
  }

  private def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    val out = compressionCodec.map(_.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    cbbos.toChunkedByteBuffer.getChunks()
  }

  private def unBlockifyObject[T: ClassTag](
      blocks: Array[JInputStream],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    val in: JInputStream = compressionCodec.map(_.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }
}

private[spark] case class ShardBlockMeta(
    numBlocks: Int,
    checksums: Option[Array[Int]],
    filterExprBytes: Option[Array[Byte]] = None,
    filterSchemaBytes: Option[Array[Byte]] = None)

private[spark] trait BloomAccumulator extends Serializable {
  def add(in: java.io.InputStream): Unit
  def isEmpty: Boolean
  def finish(out: java.io.OutputStream): Unit
}

private[spark] trait ShardCleanupCallback {
  def onShardSetRemoved(setId: Long): Unit
}

private[spark] trait ShardLookupAdapter extends Serializable {
  def lookup(manager: ShardManager, reqMsg: ManagedBuffer): ManagedBuffer
}

private[spark] object ShardManager extends Logging {
  // Distinguished from local mode
  private val ID_GENERATOR = new IdGenerator
}
