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

package org.apache.spark.network.nio

import java.nio.ByteBuffer

import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}

import scala.concurrent.Future


/**
 * A [[BlockTransferService]] implementation based on [[ConnectionManager]], a custom
 * implementation using Java NIO.
 */
final class NioBlockTransferService(conf: SparkConf, securityManager: SecurityManager)
  extends BlockTransferService with Logging {

  private var cm: ConnectionManager = _

  private var blockDataManager: BlockDataManager = _

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  override def port: Int = {
    checkInit()
    cm.id.port
  }

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  override def hostName: String = {
    checkInit()
    cm.id.host
  }

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
   */
  override def init(blockDataManager: BlockDataManager): Unit = {
    this.blockDataManager = blockDataManager
    cm = new ConnectionManager(
      conf.getInt("spark.blockManager.port", 0),
      conf,
      securityManager,
      "Connection manager for block manager")
    cm.onReceiveMessage(onBlockMessageReceive)
  }

  /**
   * Tear down the transfer service.
   */
  override def close(): Unit = {
    if (cm != null) {
      cm.stop()
    }
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit = {
    checkInit()

    val cmId = new ConnectionManagerId(host, port)
    val blockMessageArray = new BlockMessageArray(blockIds.map { blockId =>
      BlockMessage.fromGetBlock(GetBlock(BlockId(blockId)))
    })

    val future = cm.sendMessageReliably(cmId, blockMessageArray.toBufferMessage)

    // Register the listener on success/failure future callback.
    future.onSuccess { case message =>
      val bufferMessage = message.asInstanceOf[BufferMessage]
      val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)

      // SPARK-4064: In some cases(eg. Remote block was removed) blockMessageArray may be empty.
      if (blockMessageArray.isEmpty) {
        blockIds.foreach { id =>
          listener.onBlockFetchFailure(id, new SparkException(s"Received empty message from $cmId"))
        }
      } else {
        for (blockMessage: BlockMessage <- blockMessageArray) {
          val msgType = blockMessage.getType
          if (msgType != BlockMessage.TYPE_GOT_BLOCK) {
            if (blockMessage.getId != null) {
              listener.onBlockFetchFailure(blockMessage.getId.toString,
                new SparkException(s"Unexpected message $msgType received from $cmId"))
            }
          } else {
            val blockId = blockMessage.getId
            val networkSize = blockMessage.getData.limit()
            listener.onBlockFetchSuccess(
              blockId.toString, new NioManagedBuffer(blockMessage.getData))
          }
        }
      }
    }(cm.futureExecContext)

    future.onFailure { case exception =>
      blockIds.foreach { blockId =>
        listener.onBlockFetchFailure(blockId, exception)
      }
    }(cm.futureExecContext)
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This call blocks until the upload completes, or throws an exception upon failures.
   */
  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel)
    : Future[Unit] = {
    checkInit()
    val msg = PutBlock(blockId, blockData.nioByteBuffer(), level)
    val blockMessageArray = new BlockMessageArray(BlockMessage.fromPutBlock(msg))
    val remoteCmId = new ConnectionManagerId(hostName, port)
    val reply = cm.sendMessageReliably(remoteCmId, blockMessageArray.toBufferMessage)
    reply.map(x => ())(cm.futureExecContext)
  }

  private def checkInit(): Unit = if (cm == null) {
    throw new IllegalStateException(getClass.getName + " has not been initialized")
  }

  private def onBlockMessageReceive(msg: Message, id: ConnectionManagerId): Option[Message] = {
    logDebug("Handling message " + msg)
    msg match {
      case bufferMessage: BufferMessage =>
        try {
          logDebug("Handling as a buffer message " + bufferMessage)
          val blockMessages = BlockMessageArray.fromBufferMessage(bufferMessage)
          logDebug("Parsed as a block message array")
          val responseMessages = blockMessages.map(processBlockMessage).filter(_ != None).map(_.get)
          Some(new BlockMessageArray(responseMessages).toBufferMessage)
        } catch {
          case e: Exception =>
            logError("Exception handling buffer message", e)
            Some(Message.createErrorMessage(e, msg.id))
        }

      case otherMessage: Any =>
        val errorMsg = s"Received unknown message type: ${otherMessage.getClass.getName}"
        logError(errorMsg)
        Some(Message.createErrorMessage(new UnsupportedOperationException(errorMsg), msg.id))
    }
  }

  private def processBlockMessage(blockMessage: BlockMessage): Option[BlockMessage] = {
    blockMessage.getType match {
      case BlockMessage.TYPE_PUT_BLOCK =>
        val msg = PutBlock(blockMessage.getId, blockMessage.getData, blockMessage.getLevel)
        logDebug("Received [" + msg + "]")
        putBlock(msg.id, msg.data, msg.level)
        None

      case BlockMessage.TYPE_GET_BLOCK =>
        val msg = new GetBlock(blockMessage.getId)
        logDebug("Received [" + msg + "]")
        val buffer = getBlock(msg.id)
        if (buffer == null) {
          return None
        }
        Some(BlockMessage.fromGotBlock(GotBlock(msg.id, buffer)))

      case _ => None
    }
  }

  private def putBlock(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel) {
    val startTimeMs = System.currentTimeMillis()
    logDebug("PutBlock " + blockId + " started from " + startTimeMs + " with data: " + bytes)
    blockDataManager.putBlockData(blockId, new NioManagedBuffer(bytes), level)
    logDebug("PutBlock " + blockId + " used " + Utils.getUsedTimeMs(startTimeMs)
      + " with data size: " + bytes.limit)
  }

  private def getBlock(blockId: BlockId): ByteBuffer = {
    val startTimeMs = System.currentTimeMillis()
    logDebug("GetBlock " + blockId + " started from " + startTimeMs)
    val buffer = blockDataManager.getBlockData(blockId)
    logDebug("GetBlock " + blockId + " used " + Utils.getUsedTimeMs(startTimeMs)
      + " and got buffer " + buffer)
    buffer.nioByteBuffer()
  }
}
