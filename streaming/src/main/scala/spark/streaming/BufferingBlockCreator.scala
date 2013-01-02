package spark.streaming

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import spark.Logging
import spark.streaming.util.{RecurringTimer, SystemClock}
import spark.storage.StorageLevel

/**
 * Batches objects created by a [[spark.streaming.NetworkReceiver]] and puts them into
 * appropriately named blocks at regular intervals. This class starts two threads,
 * one to periodically start a new batch and prepare the previous batch of as a block,
 * the other to push the blocks into the block manager.
 */
class BufferingBlockCreator[T](receiver: NetworkReceiver[T], storageLevel: StorageLevel)
  extends Serializable with Logging {

  case class Block(id: String, iterator: Iterator[T], metadata: Any = null)

  val clock = new SystemClock()
  val blockInterval = 200L
  val blockIntervalTimer = new RecurringTimer(clock, blockInterval, updateCurrentBuffer)
  val blockStorageLevel = storageLevel
  val blocksForPushing = new ArrayBlockingQueue[Block](1000)
  val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

  var currentBuffer = new ArrayBuffer[T]

  def start() {
    blockIntervalTimer.start()
    blockPushingThread.start()
    logInfo("Data handler started")
  }

  def stop() {
    blockIntervalTimer.stop()
    blockPushingThread.interrupt()
    logInfo("Data handler stopped")
  }

  def += (obj: T) {
    currentBuffer += obj
  }

  private def createBlock(blockId: String, iterator: Iterator[T]) : Block = {
    new Block(blockId, iterator)
  }

  private def updateCurrentBuffer(time: Long) {
    try {
      val newBlockBuffer = currentBuffer
      currentBuffer = new ArrayBuffer[T]
      if (newBlockBuffer.size > 0) {
        val blockId = "input-" + receiver.streamId + "- " + (time - blockInterval)
        val newBlock = createBlock(blockId, newBlockBuffer.toIterator)
        blocksForPushing.add(newBlock)
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block interval timer thread interrupted")
      case e: Exception =>
        receiver.stop()
    }
  }

  private def keepPushingBlocks() {
    logInfo("Block pushing thread started")
    try {
      while(true) {
        val block = blocksForPushing.take()
        receiver.pushBlock(block.id, block.iterator, block.metadata, storageLevel)
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block pushing thread interrupted")
      case e: Exception =>
        receiver.stop()
    }
  }
}