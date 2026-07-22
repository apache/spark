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

import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future => SFuture}

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{ManagedRpcResponseCallback, RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shard.{ShardLookupAdapter, ShardManager}
import org.apache.spark.util.ThreadUtils

private[spark] class NettyShardRpcServer(
    appId: String,
    serializer: Serializer,
    shardManager: ShardManager,
    lookupAdapter: ShardLookupAdapter)
    extends RpcHandler
    with RpcHandler.ManagedRpcHandler
    with Logging {

  private val streamManager = new OneForOneStreamManager()

  private implicit val workerEc: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("shard-rpc-worker", 16))

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    throw new UnsupportedOperationException()
  }

  override def receive(
      client: TransportClient,
      message: ManagedBuffer,
      callback: ManagedRpcResponseCallback): Unit = {
    SFuture {
      val respMsg =
        try {
          lookupAdapter.lookup(shardManager, message)
        } catch {
          case t: Throwable =>
            callback.onFailure(t)
            null
        } finally {
          message.release()
        }
      if (respMsg != null) {
        callback.onSuccess(respMsg)
      }
    }
  }

  override def getStreamManager: StreamManager = streamManager

  def close(): Unit = {
    workerEc.shutdown()
  }
}
