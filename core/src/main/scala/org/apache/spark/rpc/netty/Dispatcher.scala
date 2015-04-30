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

import java.util.concurrent.{TimeUnit, Executors, LinkedBlockingQueue, ConcurrentHashMap}

import org.apache.spark.network.client.RpcResponseCallback

import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.rpc.{RpcCallContext, RpcAddress, RpcEndpointRef, RpcEndpoint}
import org.apache.spark.util.ThreadUtils

private class RpcEndpointPair(val endpoint: RpcEndpoint, val endpointRef: NettyRpcEndpointRef)

private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) extends Logging {

  // the inboxes that are not being used
  private val idleInboxes = new ConcurrentHashMap[RpcEndpoint, Inbox]()

  private val endpointToInbox = new ConcurrentHashMap[RpcEndpoint, Inbox]()

  // need a name to RpcEndpoint mapping so that we can delivery the messages
  private val nameToEndpoint = new ConcurrentHashMap[String, RpcEndpointPair]()

  private val endpointToEndpointRef = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]()

  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[RpcEndpoint]()

  @volatile private var stopped = false

  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = new NettyRpcAddress(nettyEnv.address.host, nettyEnv.address.port, name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    nameToEndpoint.put(name, new RpcEndpointPair(endpoint,endpointRef))
    endpointToEndpointRef.put(endpoint, endpointRef)
    val inbox = new Inbox(endpointRef, endpoint)
    endpointToInbox.put(endpoint, inbox)
    idleInboxes.put(endpoint, inbox)
    afterUpdateInbox(inbox)
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointToEndpointRef.get(endpoint)

  def getRpcEndpointRef(name: String): RpcEndpointRef = nameToEndpoint.get(name).endpointRef

  // Should be idempotent
  def unregisterRpcEndpoint(name: String): Unit = {
    val endpointPair = nameToEndpoint.remove(name)
    if (endpointPair != null) {
      val inbox = endpointToInbox.remove(endpointPair.endpoint)
      if (inbox != null) {
        inbox.stop()
        afterUpdateInbox(inbox)
      }
      endpointToEndpointRef.remove(endpointPair.endpoint)
    }
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    unregisterRpcEndpoint(rpcEndpointRef.name)
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s.
   * @param message
   */
  def broadcastMessage(message: BroadcastMessage): Unit = {
    val iter = endpointToInbox.values().iterator()
    while(iter.hasNext) {
      val inbox = iter.next()
      postMessageToInbox(inbox, message)
    }
  }

  def postMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val receiver = nameToEndpoint.get(message.receiver.name)
    if (receiver != null) {
      val inbox = endpointToInbox.get(receiver.endpoint)
      if (inbox != null) {
        val rpcCallContext =
          new RemoteNettyRpcCallContext(
            nettyEnv, inbox.endpointRef, callback, message.senderAddress, message.needReply)
        postMessageToInbox(inbox,
          ContentMessage(message.senderAddress, message.content, message.needReply, rpcCallContext))
      }
    }
  }

  def postMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val receiver = nameToEndpoint.get(message.receiver.name)
    if (receiver != null) {
      val inbox = endpointToInbox.get(receiver.endpoint)
      if (inbox != null) {
        val rpcCallContext =
          new LocalNettyRpcCallContext(
            inbox.endpointRef, message.senderAddress, message.needReply, p)
        postMessageToInbox(inbox,
          ContentMessage(message.senderAddress, message.content, message.needReply, rpcCallContext))
      }
    }
  }

  private def postMessageToInbox(inbox: Inbox, message: InboxMessage): Unit = {
    inbox.post(message)
    afterUpdateInbox(inbox)
  }

  private def afterUpdateInbox(inbox: Inbox): Unit = {
    // Do some work to trigger processing messages in the inbox
    val endpoint = inbox.endpoint
    // Replacing unsuccessfully means someone is processing it
    idleInboxes.replace(endpoint, inbox, inbox)
    receivers.put(endpoint)
  }

  class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (!stopped) {
          try {
            val endpoint = receivers.take()
            val inbox = idleInboxes.remove(endpoint)
            if (inbox != null) {
              val inboxStopped = inbox.process(Dispatcher.this)
              if (!inboxStopped) {
                idleInboxes.put(endpoint, inbox)
                if (!inbox.isEmpty) {
                  receivers.add(endpoint)
                }
              }
            } else {
              // other thread is processing endpoint's Inbox
            }
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  private val parallelism = Runtime.getRuntime.availableProcessors()

  private val executor = ThreadUtils.newDaemonFixedThreadPool(parallelism, "dispatcher-event-loop")
  (0 until parallelism) foreach { _ =>
    executor.execute(new MessageLoop)
  }

  def stop(): Unit = {
    stopped = true
    executor.shutdownNow()
  }

  def awaitTermination(): Unit = {
    executor.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
   */
  def verify(name: String): Boolean = {
    nameToEndpoint.containsKey(name)
  }

}
