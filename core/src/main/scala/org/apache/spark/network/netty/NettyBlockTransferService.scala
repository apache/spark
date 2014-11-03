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

import scala.concurrent.{Future, Promise}

import org.apache.spark.SparkConf
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientFactory}
import org.apache.spark.network.netty.NettyMessages.{OpenBlocks, UploadBlock}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{BlockFetchingListener, OneForOneBlockFetcher}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 */
class NettyBlockTransferService(conf: SparkConf) extends BlockTransferService {
  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  val serializer = new JavaSerializer(conf)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    val rpcHandler = new NettyBlockRpcServer(serializer, blockDataManager)
    transportContext = new TransportContext(SparkTransportConf.fromSparkConf(conf), rpcHandler)
    clientFactory = transportContext.createClientFactory()
    server = transportContext.createServer()
    logInfo("Server created on " + server.getPort)
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val client = clientFactory.createClient(host, port)
      new OneForOneBlockFetcher(client, blockIds.toArray, listener)
        .start(OpenBlocks(blockIds.map(BlockId.apply)))
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def hostName: String = Utils.localHostName()

  override def port: Int = server.getPort

  override def uploadBlock(
      hostname: String,
      port: Int,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Future[Unit] = {
    val result = Promise[Unit]()
    val client = clientFactory.createClient(hostname, port)

    // Convert or copy nio buffer into array in order to serialize it.
    val nioBuffer = blockData.nioByteBuffer()
    val array = if (nioBuffer.hasArray) {
      nioBuffer.array()
    } else {
      val data = new Array[Byte](nioBuffer.remaining())
      nioBuffer.get(data)
      data
    }

    val ser = serializer.newInstance()
    client.sendRpc(ser.serialize(new UploadBlock(blockId, array, level)).array(),
      new RpcResponseCallback {
        override def onSuccess(response: Array[Byte]): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          result.success()
        }
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          result.failure(e)
        }
      })

    result.future
  }

  override def close(): Unit = {
    server.close()
    clientFactory.close()
  }
}
