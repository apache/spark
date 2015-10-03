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

import java.util.LinkedList
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}

private[netty] sealed trait InboxMessage

private[netty] case class ContentMessage(
    senderAddress: RpcAddress,
    content: Any,
    needReply: Boolean,
    context: NettyRpcCallContext) extends InboxMessage

private[netty] case object OnStart extends InboxMessage

private[netty] case object OnStop extends InboxMessage

/**
 * A broadcast message that indicates connecting to a remote node.
 */
private[netty] case class Associated(remoteAddress: RpcAddress) extends InboxMessage

/**
 * A broadcast message that indicates a remote connection is lost.
 */
private[netty] case class Disassociated(remoteAddress: RpcAddress) extends InboxMessage

/**
 * A broadcast message that indicates a network error
 */
private[netty] case class AssociationError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * A inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 * @param endpointRef
 * @param endpoint
 */
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint) extends Logging {

  inbox =>

  @GuardedBy("this")
  protected val messages = new LinkedList[InboxMessage]()

  @GuardedBy("this")
  private var stopped = false

  @GuardedBy("this")
  private var enableConcurrent = false

  @GuardedBy("this")
  private var workerCount = 0

  // OnStart should be the first message to process
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized {
      if (!enableConcurrent && workerCount != 0) {
        return
      }
      message = messages.poll()
      if (message != null) {
        workerCount += 1
      } else {
        return
      }
    }
    while (true) {
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
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }
          }

          case OnStop =>
            val _workCount = inbox.synchronized {
              workerCount
            }
            assert(_workCount == 1, s"There should be only one worker but was ${_workCount}")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case Associated(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case Disassociated(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case AssociationError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && workerCount != 1) {
          // If we are not the only one worker, exit
          workerCount -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          workerCount -= 1
          return
        }
      }
    }
  }

  def post(message: InboxMessage): Unit = {
    val dropped =
      inbox.synchronized {
        if (stopped) {
          // We already put "OnStop" into "messages", so we should drop further messages
          true
        } else {
          messages.add(message)
          false
        }
      }
    if (dropped) {
      onDrop(message)
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  // Visible for testing.
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop ${message} because $endpointRef is stopped")
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try {
      action
    } catch {
      case NonFatal(e) => {
        try {
          endpoint.onError(e)
        } catch {
          case NonFatal(e) => logWarning(s"Ignore error", e)
        }
      }
    }
  }

}
