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

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

private class RpcEndpointPair(val endpoint: RpcEndpoint, val endpointRef: NettyRpcEndpointRef)

private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) extends Logging {

  private val endpointToInbox = new ConcurrentHashMap[RpcEndpoint, Inbox]()

  // need a name to RpcEndpoint mapping so that we can delivery the messages
  private val nameToEndpoint = new ConcurrentHashMap[String, RpcEndpointPair]()

  private val endpointToEndpointRef = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]()

  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[RpcEndpoint]()

  @GuardedBy("this")
  private var stopped = false

  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = new NettyRpcAddress(nettyEnv.address.host, nettyEnv.address.port, name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      if (nameToEndpoint.putIfAbsent(name, new RpcEndpointPair(endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      endpointToEndpointRef.put(endpoint, endpointRef)
      val inbox = new Inbox(endpointRef, endpoint)
      endpointToInbox.put(endpoint, inbox)
      receivers.put(inbox.endpoint)
    }
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointToEndpointRef.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointToEndpointRef.remove(endpoint)

  def getRpcEndpointRef(name: String): RpcEndpointRef = nameToEndpoint.get(name).endpointRef

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    val endpointPair = nameToEndpoint.remove(name)
    if (endpointPair != null) {
      val inbox = endpointToInbox.get(endpointPair.endpoint)
      if (inbox != null) {
        inbox.stop()
        receivers.put(inbox.endpoint)
      }
    }
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Distpatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s.
   * @param message
   */
  def broadcastMessage(message: BroadcastMessage): Unit = {
    val iter = endpointToInbox.values().iterator()
    while (iter.hasNext) {
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
        return
      }
    }
    callback.onFailure(
      new SparkException(s"Could not find ${message.receiver.name} or it has been stopped"))
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
        return
      }
    }
    p.tryFailure(
      new SparkException(s"Could not find ${message.receiver.name} or it has been stopped"))
  }

  private def postMessageToInbox(inbox: Inbox, message: InboxMessage): Unit = synchronized {
    if (stopped) {
      logWarning(s"Drop ${message} because RpcEnv has been stopped")
      return
    }
    inbox.post(message)
    receivers.put(inbox.endpoint)
  }

  private[netty] class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val endpoint = receivers.take()
            if (endpoint == DummyEndpoint) {
              // Put DummyEndpoint back so that other MessageLoops can see it.
              receivers.put(DummyEndpoint)
              return
            }
            val inbox = endpointToInbox.get(endpoint)
            if (inbox != null) {
              val inboxStopped = inbox.process(Dispatcher.this)
              if (inboxStopped) {
                endpointToInbox.remove(endpoint)
              }
            } else {
              // The endpoint has been stopped
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

  private val parallelism = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.parallelism",
    Runtime.getRuntime.availableProcessors())

  private val executor = ThreadUtils.newDaemonFixedThreadPool(parallelism, "dispatcher-event-loop")

  (0 until parallelism) foreach { _ =>
    executor.execute(new MessageLoop)
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // When we reach here, other threads won't update `nameToEndpoint`. So we can guarantee all
    // registered endpoints will be stopped correctly.
    for (name <- nameToEndpoint.keySet().asScala) {
      unregisterRpcEndpoint(name)
    }
    // When we reach here, the new items put into receivers will always be DummyEndpoint, others
    // will be rejected. So that we can make sure we will process all messages that have already in
    // the Inboxes.
    receivers.put(DummyEndpoint)
    executor.shutdown()
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

/**
 * A dummy endpoint that indicates MessageLoop should exit its loop.
 */
private[netty] object DummyEndpoint extends RpcEndpoint {
  override val rpcEnv: RpcEnv = null
}
