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

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ChunkedByteBuffer, ManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.sasl.{SaslClientBootstrap, SaslServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{BlockFetchingListener, OneForOneBlockFetcher, RetryingBlockFetcher}
import org.apache.spark.network.shuffle.protocol.UploadBlock
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 */
private[spark] class NettyBlockTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    override val hostName: String,
    numCores: Int)
  extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = new JavaSerializer(conf)
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    if (authEnabled) {
      serverBootstrap = Some(new SaslServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new SaslClientBootstrap(transportConf, conf.getAppId, securityManager,
        securityManager.isSaslEncryptionEnabled()))
    }
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    server = createServer(serverBootstrap.toList)
    appId = conf.getAppId
    logInfo(s"Server created on ${hostName}:${server.getPort}")
  }

  /** Creates and binds the TransportServer, possibly trying multiple ports. */
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(hostName, port, bootstraps.asJava)
      (server, server.getPort)
    }

    val portToTry = conf.getInt("spark.blockManager.port", 0)
    Utils.startServiceOnPort(portToTry, startService, conf, getClass.getName)._1
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          val client = clientFactory.createClient(host, port)
          new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener).start()
        }
      }

      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def port: Int = server.getPort

  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit] = {
    val result = Promise[Unit]()
    val client = clientFactory.createClient(hostname, port)
    // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
    // Everything else is encoded using our binary protocol.
    val metadata = serializer.newInstance().serialize((level, classTag))
    val uploadBlock = new UploadBlock(appId, execId, blockId.toString,
      metadata.toArray, blockData)
    val encodedLength = uploadBlock.encodedLength() + 1
    val inputStream = uploadBlock.toInputStream
    client.sendRpc(inputStream, encodedLength, new RpcResponseCallback {
      override def onSuccess(response: ChunkedByteBuffer): Unit = {
        logTrace(s"Successfully uploaded block $blockId")
        result.success((): Unit)
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"Error while uploading block $blockId", e)
        result.failure(e)
      }
    })

    result.future
  }

  override def close(): Unit = {
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
  }
}
