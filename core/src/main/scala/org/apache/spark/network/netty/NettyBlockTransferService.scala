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

import org.apache.spark.SparkConf
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{SluiceClient, SluiceClientFactory}
import org.apache.spark.network.server.{DefaultStreamManager, SluiceServer}
import org.apache.spark.network.util.{ConfigProvider, SluiceConfig}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

import scala.concurrent.Future

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 */
class NettyBlockTransferService(conf: SparkConf) extends BlockTransferService {
  var client: SluiceClient = _

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  val serializer = new JavaSerializer(conf)

  // Create a SluiceConfig using SparkConf.
  private[this] val sluiceConf = new SluiceConfig(
    new ConfigProvider { override def get(name: String) = conf.get(name) })

  private[this] var server: SluiceServer = _
  private[this] var clientFactory: SluiceClientFactory = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    val streamManager = new DefaultStreamManager
    val rpcHandler = new NettyBlockRpcServer(serializer, streamManager, blockDataManager)
    server = new SluiceServer(sluiceConf, streamManager, rpcHandler)
    clientFactory = new SluiceClientFactory(sluiceConf)
  }

  override def fetchBlocks(
      hostName: String,
      port: Int,
      blockIds: Seq[String],
      listener: BlockFetchingListener): Unit = {
    val client = clientFactory.createClient(hostName, port)
    new NettyBlockFetcher(serializer, client, blockIds, listener)
  }

  override def hostName: String = Utils.localHostName()

  override def port: Int = server.getPort

  // TODO: Implement
  override def uploadBlock(
      hostname: String,
      port: Int,
      blockId: String,
      blockData: ManagedBuffer,
      level: StorageLevel): Future[Unit] = ???

  override def close(): Unit = server.close()
}
