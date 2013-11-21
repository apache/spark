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

package org.apache.spark.network.netty

import java.util.concurrent.Executors

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.util.CharsetUtil

import org.apache.spark.Logging
import org.apache.spark.network.ConnectionManagerId

import scala.collection.JavaConverters._
import org.apache.spark.storage.BlockId


private[spark] class ShuffleCopier extends Logging {

  def getBlock(host: String, port: Int, blockId: BlockId,
      resultCollectCallback: (BlockId, Long, ByteBuf) => Unit) {

    val handler = new ShuffleCopier.ShuffleClientHandler(resultCollectCallback)
    val connectTimeout = System.getProperty("spark.shuffle.netty.connect.timeout", "60000").toInt
    val fc = new FileClient(handler, connectTimeout)

    try {
      fc.init()
      fc.connect(host, port)
      fc.sendRequest(blockId.name)
      fc.waitForClose()
      fc.close()
    } catch {
      // Handle any socket-related exceptions in FileClient
      case e: Exception => {
        logError("Shuffle copy of block " + blockId + " from " + host + ":" + port + " failed", e)
        handler.handleError(blockId)
      }
    }
  }

  def getBlock(cmId: ConnectionManagerId, blockId: BlockId,
      resultCollectCallback: (BlockId, Long, ByteBuf) => Unit) {
    getBlock(cmId.host, cmId.port, blockId, resultCollectCallback)
  }

  def getBlocks(cmId: ConnectionManagerId,
    blocks: Seq[(BlockId, Long)],
    resultCollectCallback: (BlockId, Long, ByteBuf) => Unit) {

    for ((blockId, size) <- blocks) {
      getBlock(cmId, blockId, resultCollectCallback)
    }
  }
}


private[spark] object ShuffleCopier extends Logging {

  private class ShuffleClientHandler(resultCollectCallBack: (BlockId, Long, ByteBuf) => Unit)
    extends FileClientHandler with Logging {

    override def handle(ctx: ChannelHandlerContext, in: ByteBuf, header: FileHeader) {
      logDebug("Received Block: " + header.blockId + " (" + header.fileLen + "B)")
      resultCollectCallBack(header.blockId, header.fileLen.toLong, in.readBytes(header.fileLen))
    }

    override def handleError(blockId: BlockId) {
      if (!isComplete) {
        resultCollectCallBack(blockId, -1, null)
      }
    }
  }

  def echoResultCollectCallBack(blockId: BlockId, size: Long, content: ByteBuf) {
    if (size != -1) {
      logInfo("File: " + blockId + " content is : \" " + content.toString(CharsetUtil.UTF_8) + "\"")
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ShuffleCopier <host> <port> <shuffle_block_id> <threads>")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt
    val blockId = BlockId(args(2))
    val threads = if (args.length > 3) args(3).toInt else 10

    val copiers = Executors.newFixedThreadPool(80)
    val tasks = (for (i <- Range(0, threads)) yield { 
      Executors.callable(new Runnable() {
        def run() {
          val copier = new ShuffleCopier()
          copier.getBlock(host, port, blockId, echoResultCollectCallBack)
        }
      })
    }).asJava
    copiers.invokeAll(tasks)
    copiers.shutdown()
    System.exit(0)
  }
}
