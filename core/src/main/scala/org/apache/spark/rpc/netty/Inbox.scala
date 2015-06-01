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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.rpc.{ThreadSafeRpcEndpoint, RpcAddress, RpcEndpoint}

private[netty] sealed trait InboxMessage

private[netty] case class ContentMessage(
    senderAddress: RpcAddress,
    content: Any,
    needReply: Boolean,
    context: NettyRpcCallContext) extends InboxMessage

/**
 * A message type that will be posted to all registered [[RpcEndpoint]]
 */
private[netty] sealed trait BroadcastMessage extends InboxMessage

private[netty] case object OnStart extends InboxMessage

private[netty] case object OnStop extends InboxMessage

private[netty] case class Associated(remoteAddress: RpcAddress) extends BroadcastMessage

/**
 * A broadcast message that indicates
 */
private[netty] case class Disassociated(remoteAddress: RpcAddress) extends BroadcastMessage

/**
 * A broadcast message that indicates a network error
 */
private[netty] case class AssociationError(cause: Throwable, remoteAddress: RpcAddress)
  extends BroadcastMessage

private[netty] abstract class Inbox(
    val endpointRef: NettyRpcEndpointRef, val endpoint: RpcEndpoint) extends Logging {

  protected val messages = new ConcurrentLinkedQueue[InboxMessage]()

  /**
   * Process stored messages. Return `true` if the `Inbox` is already stopped, and the caller will
   * release all resources used by the `Inbox`.
   */
  def process(dispatcher: Dispatcher): Boolean

  def post(message: InboxMessage): Unit

  protected def onDrop(message: Any): Unit = {
    logWarning(s"Drop ${message} because $endpointRef is stopped")
  }

  def stop(): Unit

  protected def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try {
      action
    } catch {
      case NonFatal(e) => {
        try {
          endpoint.onError(e)
        } catch {
          case NonFatal(e) => logError(s"Ignore error: ${e.getMessage}", e)
        }
      }
    }
  }

  def isEmpty: Boolean = messages.isEmpty

}

/**
 * A inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 * @param endpointRef
 * @param endpoint
 */
private[netty] class ThreadSafeInbox(
    endpointRef: NettyRpcEndpointRef,
    override val endpoint: ThreadSafeRpcEndpoint) extends Inbox(endpointRef, endpoint) {

  private val _endpoint = endpoint.asInstanceOf[ThreadSafeRpcEndpoint]

  // protected by "this"
  private var stopped = false

  // OnStart should be the first message to process
  messages.add(OnStart)

  override def process(dispatcher: Dispatcher): Boolean = {
    var exit = false
    var message = messages.poll()
    while (message != null) {
      safelyCall(endpoint) {
        message match {
          case ContentMessage(_sender, content, needReply, context) =>
            val pf: PartialFunction[Any, Unit] =
              if (needReply) {
                endpoint.receiveAndReply(context)
              } else {
                endpoint.receive
              }
            try {
              pf.applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unmatched message $message from ${_sender}")
              })
              if (!needReply) {
                context.finish()
              }
            } catch {
              case NonFatal(e) =>
                if (needReply) {
                  // If the sender asks a reply, we should send the error back to the sender
                  context.sendFailure(e)
                } else {
                  context.finish()
                  throw e
                }
            }

          case OnStart => {
            _endpoint.asInstanceOf[ThreadSafeRpcEndpoint].onStart()
          }
          case OnStop =>
            dispatcher.unregisterRpcEndpoint(endpointRef.name)
            _endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")
            exit = true
          case Associated(remoteAddress) =>
            endpoint.onConnected(remoteAddress)
          case Disassociated(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)
          case AssociationError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }
      message = messages.poll()
    }
    exit
  }

  override def post(message: InboxMessage): Unit = {
    val dropped =
      synchronized {
        if (stopped) {
          // We already put "OnStop" into "messages", so we should drop further messages
          true
        } else {
          messages.add(message)
          false
        }
      }
    if (dropped) {
      onDrop()
    }
  }

  override def stop(): Unit = synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      stopped = true
      messages.add(OnStop)
    }
  }
}

/**
 * A inbox that stores messages for an [[RpcEndpoint]] and posts messages to it concurrently.
 * @param endpointRef
 * @param endpoint
 */
private[netty] class ConcurrentInbox(
    endpointRef: NettyRpcEndpointRef,
    endpoint: RpcEndpoint) extends Inbox(endpointRef, endpoint) {

  @volatile private var stopped = false

  /**
   * Process stored messages. Return `true` if the `Inbox` is already stopped, and the caller will
   * release all resources used by the `Inbox`.
   */
  override def process(dispatcher: Dispatcher): Boolean = {
    var message = messages.poll()
    while (!stopped && message != null) {
      safelyCall(endpoint) {
        message match {
          case ContentMessage(_sender, content, needReply, context) =>
            val pf: PartialFunction[Any, Unit] =
              if (needReply) {
                endpoint.receiveAndReply(context)
              } else {
                endpoint.receive
              }
            try {
              pf.applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unmatched message $message from ${_sender}")
              })
              if (!needReply) {
                context.finish()
              }
            } catch {
              case NonFatal(e) =>
                if (needReply) {
                  // If the sender asks a reply, we should send the error back to the sender
                  context.sendFailure(e)
                } else {
                  context.finish()
                  throw e
                }
            }
          case Associated(remoteAddress) =>
            endpoint.onConnected(remoteAddress)
          case Disassociated(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)
          case AssociationError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }
      message = messages.poll()
    }
    stopped
  }

  override def post(message: InboxMessage): Unit = {
    if (stopped) {
      onDrop()
    } else {
      messages.add(message)
    }
  }

  override def stop(): Unit = {
    stopped = true
  }

}
