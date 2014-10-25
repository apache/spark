package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.{existentials, postfixOps}

import WriteAheadLogBasedBlockHandler._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkException, Logging, SparkConf}
import org.apache.spark.storage.{BlockManager, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.util.{Clock, SystemClock, WriteAheadLogManager}
import org.apache.spark.util.Utils

private[streaming] sealed trait ReceivedBlock
private[streaming] case class ArrayBufferBlock(arrayBuffer: ArrayBuffer[_]) extends ReceivedBlock
private[streaming] case class IteratorBlock(iterator: Iterator[_]) extends ReceivedBlock
private[streaming] case class ByteBufferBlock(byteBuffer: ByteBuffer) extends ReceivedBlock


/** Trait that represents a class that handles the storage of blocks received by receiver */
private[streaming] trait ReceivedBlockHandler {

  /** Store a received block with the given block id */
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef]

  /** Cleanup old blocks older than the given threshold time */
  def cleanupOldBlock(threshTime: Long)
}

/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks into a block manager with the specified storage level.
 */
private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager, storageLevel: StorageLevel)
  extends ReceivedBlockHandler with Logging {
  
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef] = {
    val putResult = receivedBlock match {
      case ArrayBufferBlock(arrayBuffer) =>
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel, tellMaster = true)
      case IteratorBlock(iterator) =>
        blockManager.putIterator(blockId, iterator, storageLevel, tellMaster = true)
      case ByteBufferBlock(byteBuffer) =>
        blockManager.putBytes(blockId, byteBuffer, storageLevel, tellMaster = true)
      case _ =>
        throw new SparkException(s"Could not store $blockId to block manager, unexpected block type")
    }
    if (!putResult.map { _._1 }.contains(blockId)) {
      throw new SparkException(
        s"Could not store $blockId to block manager with storage level $storageLevel")
    }
    None
  }

  def cleanupOldBlock(threshTime: Long) {
    // this is not used as blocks inserted into the BlockManager are cleared by DStream's clearing
    // of BlockRDDs.
  }
}

/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks in both, a write ahead log and a block manager.
 */
private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String,
    clock: Clock = new SystemClock
  ) extends ReceivedBlockHandler with Logging {

  private val blockStoreTimeout = conf.getInt(
    "spark.streaming.receiver.blockStoreTimeout", 30).seconds
  private val rollingInterval = conf.getInt(
    "spark.streaming.receiver.writeAheadLog.rollingInterval", 60)
  private val maxFailures = conf.getInt(
    "spark.streaming.receiver.writeAheadLog.maxFailures", 3)

  private val logManager = new WriteAheadLogManager(
    checkpointDirToLogDir(checkpointDir, streamId),
    hadoopConf, rollingInterval, maxFailures,
    callerName = "WriteAheadLogBasedBlockHandler",
    clock = clock
  )

  // For processing futures used in parallel block storing into block manager and write ahead log
  implicit private val executionContext = ExecutionContext.fromExecutorService(
    Utils.newDaemonFixedThreadPool(1, "WriteAheadLogBasedBlockHandler"))
  
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef] = {

    // Serialize the block so that it can be inserted into both
    val serializedBlock = receivedBlock match {
      case ArrayBufferBlock(arrayBuffer) =>
        blockManager.dataSerialize(blockId, arrayBuffer.iterator)
      case IteratorBlock(iterator) =>
        blockManager.dataSerialize(blockId, iterator)
      case ByteBufferBlock(byteBuffer) =>
        byteBuffer
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, unexpected block type")
    }

    val pushToBlockManagerFuture = Future {
      val putResult =
        blockManager.putBytes(blockId, serializedBlock, storageLevel, tellMaster = true)
      if (!putResult.map { _._1 }.contains(blockId)) {
        throw new SparkException(
          s"Could not store $blockId to block manager with storage level $storageLevel")
      }
    }
    val pushToLogFuture = Future {
      logManager.writeToLog(serializedBlock)
    }
    val combinedFuture = for {
      _ <- pushToBlockManagerFuture
      fileSegment <- pushToLogFuture
    } yield fileSegment

    Some(Await.result(combinedFuture, blockStoreTimeout))
  }

  def cleanupOldBlock(threshTime: Long) {
    logManager.cleanupOldLogs(threshTime)
  }

  def stop() {
    logManager.stop()
  }
}

private[streaming] object WriteAheadLogBasedBlockHandler {
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
}
