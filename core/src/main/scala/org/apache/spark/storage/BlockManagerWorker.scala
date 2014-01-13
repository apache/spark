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

package org.apache.spark.storage

import java.nio.ByteBuffer

import org.apache.spark.{Logging}
import org.apache.spark.network._
import org.apache.spark.util.Utils

/**
 * A network interface for BlockManager. Each slave should have one
 * BlockManagerWorker.
 *
 * TODO: Use event model.
 */
private[spark] class BlockManagerWorker(val blockManager: BlockManager) extends Logging {

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
          Some(new BlockMessageArray(responseMessages).toBufferMessage)
        } catch {
          case e: Exception => logError("Exception handling buffer message", e)
          None
        }
      }
      case otherMessage: Any => {
        logError("Unknown type message received: " + otherMessage)
        None
      }
    }
  }

  def processBlockMessage(blockMessage: BlockMessage): Option[BlockMessage] = {
    blockMessage.getType match {
      case BlockMessage.TYPE_PUT_BLOCK => {
        val pB = PutBlock(blockMessage.getId, blockMessage.getData, blockMessage.getLevel)
        logDebug("Received [" + pB + "]")
        putBlock(pB.id, pB.data, pB.level)
        None
      }
      case BlockMessage.TYPE_GET_BLOCK => {
        val gB = new GetBlock(blockMessage.getId)
        logDebug("Received [" + gB + "]")
        val buffer = getBlock(gB.id)
        if (buffer == null) {
          return None
        }
        Some(BlockMessage.fromGotBlock(GotBlock(gB.id, buffer)))
      }
      case _ => None
    }
  }

  private def putBlock(id: BlockId, bytes: ByteBuffer, level: StorageLevel) {
    val startTimeMs = System.currentTimeMillis()
    logDebug("PutBlock " + id + " started from " + startTimeMs + " with data: " + bytes)
    blockManager.putBytes(id, bytes, level)
    logDebug("PutBlock " + id + " used " + Utils.getUsedTimeMs(startTimeMs)
        + " with data size: " + bytes.limit)
  }

  private def getBlock(id: BlockId): ByteBuffer = {
    val startTimeMs = System.currentTimeMillis()
    logDebug("GetBlock " + id + " started from " + startTimeMs)
    val buffer = blockManager.getLocalBytes(id) match {
      case Some(bytes) => bytes
      case None => null
    }
    logDebug("GetBlock " + id + " used " + Utils.getUsedTimeMs(startTimeMs)
        + " and got buffer " + buffer)
    buffer
  }
}

private[spark] object BlockManagerWorker extends Logging {
  private var blockManagerWorker: BlockManagerWorker = null

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
    resultMessage != None
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
      case None => logDebug("No response message received")
    }
    null
  }
}
