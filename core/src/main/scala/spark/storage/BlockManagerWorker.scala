package spark.storage

import java.nio.ByteBuffer

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import spark.{Logging, Utils, SparkEnv}
import spark.network._

/**
 * A network interface for BlockManager. Each slave should have one
 * BlockManagerWorker.
 *
 * TODO: Use event model.
 */
private[spark] class BlockManagerWorker(val blockManager: BlockManager) extends Logging {
  initLogging()

  blockManager.connectionManager.onReceiveMessage(onBlockMessageReceive)

  def onBlockMessageReceive(msg: Message, id: ConnectionManagerId): Option[Message] = {
    logDebug("Handling message " + msg)
    msg match {
      case bufferMessage: BufferMessage => {
        try {
          logDebug("Handling as a buffer message " + bufferMessage)
          val blockMessages = BlockMessageArray.fromBufferMessage(bufferMessage)
          logDebug("Parsed as a block message array")
          val responseMessages = blockMessages.map(processBlockMessage).filter(_ != None).map(_.get)
          return Some(new BlockMessageArray(responseMessages).toBufferMessage)
        } catch {
          case e: Exception => logError("Exception handling buffer message", e)
          return None
        }
      }
      case otherMessage: Any => {
        logError("Unknown type message received: " + otherMessage)
        return None
      }
    }
  }

  def processBlockMessage(blockMessage: BlockMessage): Option[BlockMessage] = {
    blockMessage.getType match {
      case BlockMessage.TYPE_PUT_BLOCK => {
        val pB = PutBlock(blockMessage.getId, blockMessage.getData, blockMessage.getLevel)
        logDebug("Received [" + pB + "]")
        putBlock(pB.id, pB.data, pB.level)
        return None
      }
      case BlockMessage.TYPE_GET_BLOCK => {
        val gB = new GetBlock(blockMessage.getId)
        logDebug("Received [" + gB + "]")
        val buffer = getBlock(gB.id)
        if (buffer == null) {
          return None
        }
        return Some(BlockMessage.fromGotBlock(GotBlock(gB.id, buffer)))
      }
      case _ => return None
    }
  }

  private def putBlock(id: String, bytes: ByteBuffer, level: StorageLevel) {
    val startTimeMs = System.currentTimeMillis()
    logDebug("PutBlock " + id + " started from " + startTimeMs + " with data: " + bytes)
    blockManager.putBytes(id, bytes, level)
    logDebug("PutBlock " + id + " used " + Utils.getUsedTimeMs(startTimeMs)
        + " with data size: " + bytes.limit)
  }

  private def getBlock(id: String): ByteBuffer = {
    val startTimeMs = System.currentTimeMillis()
    logDebug("GetBlock " + id + " started from " + startTimeMs)
    val buffer = blockManager.getLocalBytes(id) match {
      case Some(bytes) => bytes
      case None => null
    }
    logDebug("GetBlock " + id + " used " + Utils.getUsedTimeMs(startTimeMs)
        + " and got buffer " + buffer)
    return buffer
  }
}

private[spark] object BlockManagerWorker extends Logging {
  private var blockManagerWorker: BlockManagerWorker = null
  private val DATA_TRANSFER_TIME_OUT_MS: Long = 500
  private val REQUEST_RETRY_INTERVAL_MS: Long = 1000

  initLogging()

  def startBlockManagerWorker(manager: BlockManager) {
    blockManagerWorker = new BlockManagerWorker(manager)
  }

  def syncPutBlock(msg: PutBlock, toConnManagerId: ConnectionManagerId): Boolean = {
    val blockManager = blockManagerWorker.blockManager
    val connectionManager = blockManager.connectionManager
    val blockMessage = BlockMessage.fromPutBlock(msg)
    val blockMessageArray = new BlockMessageArray(blockMessage)
    val resultMessage = connectionManager.sendMessageReliablySync(
        toConnManagerId, blockMessageArray.toBufferMessage)
    return (resultMessage != None)
  }

  def syncGetBlock(msg: GetBlock, toConnManagerId: ConnectionManagerId): ByteBuffer = {
    val blockManager = blockManagerWorker.blockManager
    val connectionManager = blockManager.connectionManager
    val blockMessage = BlockMessage.fromGetBlock(msg)
    val blockMessageArray = new BlockMessageArray(blockMessage)
    val responseMessage = connectionManager.sendMessageReliablySync(
        toConnManagerId, blockMessageArray.toBufferMessage)
    responseMessage match {
      case Some(message) => {
        val bufferMessage = message.asInstanceOf[BufferMessage]
        logDebug("Response message received " + bufferMessage)
        BlockMessageArray.fromBufferMessage(bufferMessage).foreach(blockMessage => {
            logDebug("Found " + blockMessage)
            return blockMessage.getData
          })
      }
      case None => logDebug("No response message received"); return null
    }
    return null
  }
}
