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

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{TransportClientBootstrap, RpcResponseCallback, TransportClientFactory}
import org.apache.spark.network.sasl.{SaslRpcHandler, SaslClientBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{RetryingBlockFetcher, BlockFetchingListener, OneForOneBlockFetcher}
import org.apache.spark.network.shuffle.protocol.{UploadPartialBlock, UploadBlock}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

import scala.util.{Failure, Success}

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 */
class NettyBlockTransferService(conf: SparkConf, securityManager: SecurityManager, numCores: Int)
  extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = new JavaSerializer(conf)
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    val (rpcHandler: RpcHandler, bootstrap: Option[TransportClientBootstrap]) = {
      val nettyRpcHandler = new NettyBlockRpcServer(serializer, blockDataManager)
      if (!authEnabled) {
        (nettyRpcHandler, None)
      } else {
        (new SaslRpcHandler(nettyRpcHandler, securityManager),
          Some(new SaslClientBootstrap(transportConf, conf.getAppId, securityManager)))
      }
    }
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(bootstrap.toList)
    server = transportContext.createServer(conf.getInt("spark.blockManager.port", 0))
    appId = conf.getAppId
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

  override def hostName: String = Utils.localHostName()

  override def port: Int = server.getPort

  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Future[Unit] = {
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel is serialized as bytes using our JavaSerializer. Everything else is encoded
    // using our binary protocol.
    val levelBytes = serializer.newInstance().serialize(level).array()

    // Convert or copy nio buffer into array in order to serialize it.
    val largeByteBuffer = blockData.nioByteBuffer()
    val bufferParts = largeByteBuffer.nioBuffers().asScala
    val chunkOffsets: Seq[Long] = bufferParts.scanLeft(0l){case(offset, buf) => offset + buf.limit()}

    performSequentially(bufferParts.zipWithIndex){case (buf,idx) =>
      val partialBlockArray = if (buf.hasArray) {
        buf.array()
      } else {
        val arr = new Array[Byte](buf.limit())
        buf.get(arr)
        arr
      }
      //Note: one major shortcoming of this is that it expects the incoming LargeByteBuffer to
      // already be reasonably chunked -- in particular, the chunks cannot get too close to 2GB
      // or else we'll still run into problems b/c there is some more overhead in the transfer
      val msg = new UploadPartialBlock(appId, execId, blockId.toString, bufferParts.size, idx,
        chunkOffsets(idx), levelBytes, partialBlockArray)

      val result = Promise[Unit]()
      client.sendRpc(msg.toByteArray,
        new RpcResponseCallback {
          override def onSuccess(response: Array[Byte]): Unit = {
            logTrace(s"Successfully uploaded partial block $blockId, part $idx (out of ${bufferParts.size})")
            result.success()
          }

          override def onFailure(e: Throwable): Unit = {
            logError(s"Error while uploading partial block $blockId, part $idx (out of ${bufferParts.size})", e)
            result.failure(e)
          }
        })
      result.future
    }
  }

  //thanks to our old friend @ryanlecompte: https://gist.github.com/squito/242f82ad6345e3f85a5b
  private def performSequentially[A](items: Seq[A])(f: A => Future[Unit]): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    items.headOption match {
      case Some(nextItem) =>
        val fut = f(nextItem)
        fut.flatMap { _ =>
            performSequentially(items.tail)(f)
        }
      case None =>
        // nothing left to process
        Future.successful(())
    }
  }


  override def close(): Unit = {
    server.close()
    clientFactory.close()
  }
}
