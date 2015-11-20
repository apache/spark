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

package org.apache.spark.rpc.netty

import scala.concurrent.Promise

import org.apache.spark.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}

private[netty] abstract class NettyRpcCallContext(
    endpointRef: NettyRpcEndpointRef,
    override val senderAddress: RpcAddress,
    needReply: Boolean)
  extends RpcCallContext with Logging {

  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    if (needReply) {
      send(AskResponse(endpointRef, response))
    } else {
      throw new IllegalStateException(
        s"Cannot send $response to the sender because the sender does not expect a reply")
    }
  }

  override def sendFailure(e: Throwable): Unit = {
    if (needReply) {
      send(AskResponse(endpointRef, RpcFailure(e)))
    } else {
      logError(e.getMessage, e)
      throw new IllegalStateException(
        "Cannot send reply to the sender because the sender won't handle it")
    }
  }

  def finish(): Unit = {
    if (!needReply) {
      send(Ack(endpointRef))
    }
  }
}

/**
 * If the sender and the receiver are in the same process, the reply can be sent back via `Promise`.
 */
private[netty] class LocalNettyRpcCallContext(
    endpointRef: NettyRpcEndpointRef,
    senderAddress: RpcAddress,
    needReply: Boolean,
    p: Promise[Any])
  extends NettyRpcCallContext(endpointRef, senderAddress, needReply) {

  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}

/**
 * A [[RpcCallContext]] that will call [[RpcResponseCallback]] to send the reply back.
 */
private[netty] class RemoteNettyRpcCallContext(
    nettyEnv: NettyRpcEnv,
    endpointRef: NettyRpcEndpointRef,
    callback: RpcResponseCallback,
    senderAddress: RpcAddress,
    needReply: Boolean)
  extends NettyRpcCallContext(endpointRef, senderAddress, needReply) {

  override protected def send(message: Any): Unit = {
    val reply = nettyEnv.serialize(message)
    callback.onSuccess(reply)
  }
}
