package spark.streaming

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import spark.Logging
import spark.streaming.util.{RecurringTimer, SystemClock}
import spark.storage.StorageLevel


/**
   * This is a helper object that manages the data received from the socket. It divides
   * the object received into small batches of 100s of milliseconds, pushes them as
   * blocks into the block manager and reports the block IDs to the network input
   * tracker. It starts two threads, one to periodically start a new batch and prepare
   * the previous batch of as a block, the other to push the blocks into the block
   * manager.
   */
  class DataHandler[T](receiver: NetworkReceiver[T], storageLevel: StorageLevel) 
    extends Serializable with Logging {
    
    case class Block(id: String, iterator: Iterator[T], metadata: Any = null)

    val clock = new SystemClock()
    val blockInterval = 200L
    val blockIntervalTimer = new RecurringTimer(clock, blockInterval, updateCurrentBuffer)
    val blockStorageLevel = storageLevel
    val blocksForPushing = new ArrayBlockingQueue[Block](1000)
    val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

    var currentBuffer = new ArrayBuffer[T]

    def createBlock(blockId: String, iterator: Iterator[T]) : Block = {
      new Block(blockId, iterator)
    }

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

    def updateCurrentBuffer(time: Long) {
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

    def keepPushingBlocks() {
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