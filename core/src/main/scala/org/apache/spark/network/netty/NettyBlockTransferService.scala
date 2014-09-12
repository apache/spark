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

import scala.concurrent.Future

import org.apache.spark.SparkConf
import org.apache.spark.network._
import org.apache.spark.storage.StorageLevel


/**
 * A [[BlockTransferService]] implementation based on Netty.
 *
 * See protocol.scala for the communication protocol between server and client
 */
private[spark]
final class NettyBlockTransferService(conf: SparkConf) extends BlockTransferService {

  private[this] val nettyConf: NettyConfig = new NettyConfig(conf)

  private[this] var server: BlockServer = _
  private[this] var clientFactory: BlockClientFactory = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    server = new BlockServer(nettyConf, blockDataManager)
    clientFactory = new BlockClientFactory(nettyConf)
  }

  override def stop(): Unit = {
    if (server != null) {
      server.stop()
    }
    if (clientFactory != null) {
      clientFactory.stop()
    }
  }

  override def fetchBlocks(
      hostName: String,
      port: Int,
      blockIds: Seq[String],
      listener: BlockFetchingListener): Unit = {
    clientFactory.createClient(hostName, port).fetchBlocks(blockIds, listener)
  }

  override def uploadBlock(
      hostname: String,
      port: Int,
      blockId: String,
      blockData: ManagedBuffer, level: StorageLevel): Future[Unit] = {
    // TODO(rxin): Implement uploadBlock.
    ???
  }

  override def hostName: String = {
    if (server == null) {
      throw new IllegalStateException("Server has not been started")
    }
    server.hostName
  }

  override def port: Int = {
    if (server == null) {
      throw new IllegalStateException("Server has not been started")
    }
    server.port
  }
}
