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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.network.{ShardLookupService, TransportContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.ManagedRpcResponseCallback
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shard.ShardLookupListener
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shard.{ShardLookupAdapter, ShardManager}
import org.apache.spark.util.Utils

private[spark] class NettyShardLookupService(
    conf: SparkConf,
    bindAddress: String,
    val hostName: String,
    _port: Int,
    numCores: Int,
    masterEndpoint: RpcEndpointRef = null)
    extends ShardLookupService {

  private val serializer = new JavaSerializer(conf)
  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var rpcHandler: NettyShardRpcServer = _

  private val lookupAdapter: ShardLookupAdapter = {
    Utils
      .classForName("org.apache.spark.sql.execution.joins.HashedRelationAdapter")
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[ShardLookupAdapter]
  }

  override def init(shardManager: ShardManager): Unit = {
    rpcHandler = new NettyShardRpcServer(conf.getAppId, serializer, shardManager, lookupAdapter)
    val cloned = conf.clone
    cloned.setIfMissing("spark.shard.io.mode", "NIO")
    cloned.setIfMissing("spark.shard.io.clientThreads", "8")
    cloned.setIfMissing("spark.shard.io.serverThreads", "8")
    cloned.setIfMissing("spark.shard.io.numConnectionsPerPeer", "8")
    cloned.setIfMissing("spark.shard.io.connectionCreationTimeout", "2s")
    cloned.setIfMissing("spark.shard.io.retryWait", "300ms")
    cloned.setIfMissing("spark.shard.io.maxRetries", "3")
    cloned.setIfMissing("spark.network.waitForReachable", "false")
    cloned.setIfMissing("spark.network.sharedByteBufAllocators.enabled", "true")
    cloned.setIfMissing("spark.network.io.preferDirectBufs", "true")
    transportConf = SparkTransportConf.fromSparkConf(cloned, "shard", numCores)
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory()
    server = createNonAuthServer()
    appId = conf.getAppId
    logger.info(s"Server created on $hostName $bindAddress:${server.getPort}")
  }

  override def port: Int = server.getPort

  private def createNonAuthServer(): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server =
        transportContext.createServer(
          bindAddress,
          port,
          List.empty[TransportServerBootstrap].asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  override def fetchBatch(
      host: String,
      port: Int,
      reqMsg: ManagedBuffer,
      listener: ShardLookupListener): Unit = try {
    val client = clientFactory.createClient(host, port, true)
    client.sendManagedRpc(
      reqMsg,
      new ManagedRpcResponseCallback() {

        override def onSuccess(response: ManagedBuffer): Unit = {
          listener.onBatchFetchSuccess(response)
        }

        override def onFailure(e: Throwable): Unit = {
          listener.onBatchFetchFailure(e)
        }
      })
  } catch {
    case e: Exception =>
      listener.onBatchFetchFailure(e)
  }

  override def close(): Unit = {
    if (server != null) {
      server.close()
    }

    if (rpcHandler != null) {
      rpcHandler.close()
    }

    if (clientFactory != null) {
      clientFactory.close()
    }

    if (transportContext != null) {
      transportContext.close()
    }
  }
}
