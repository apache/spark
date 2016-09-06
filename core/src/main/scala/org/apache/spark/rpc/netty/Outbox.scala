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

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ChunkedByteBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}

private[netty] sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

private[netty] case class OneWayOutboxMessage(content: ChunkedByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logWarning(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}

private[netty] case class RpcOutboxMessage(
    content: ChunkedByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ChunkedByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }

  def onTimeout(): Unit = {
    require(client != null, "TransportClient has not yet been set.")
    client.removeRpcRequest(requestId)
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ChunkedByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}

private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {

  outbox => // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  @GuardedBy("this")
  private var client: TransportClient = null

  /**
   * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
   */
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  @GuardedBy("this")
  private var stopped = false

  /**
   * If there is any thread draining the message queue
   */
  @GuardedBy("this")
  private var draining = false

  /**
   * Send a message. If there is no active connection, cache it and launch a new connection. If
   * [[Outbox]] is stopped, the sender will be notified with a [[SparkException]].
   */
  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        messages.add(message)
        false
      }
    }
    if (dropped) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox()
    }
  }

  /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        launchConnectTask()
        return
      }
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      message = messages.poll()
      if (message == null) {
        return
      }
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        drainOutbox()
      }
    })
  }

  /**
   * Stop [[Inbox]] and notify the waiting messages with the cause.
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
   * [[SparkException]].
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
