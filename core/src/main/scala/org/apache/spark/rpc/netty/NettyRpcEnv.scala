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

import java.io._
import java.lang.{Boolean => JBoolean}
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success}
import scala.util.control.NonFatal

import com.google.common.base.Preconditions
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.sasl.{SaslClientBootstrap, SaslServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance}
import org.apache.spark.util.{ThreadUtils, Utils}

private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    securityManager: SecurityManager) extends RpcEnv(conf) with Logging {

  private val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set("spark.shuffle.io.numConnectionsPerPeer", "1"),
    conf.getInt("spark.rpc.io.threads", 0))

  private val dispatcher: Dispatcher = new Dispatcher(this)

  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this))

  private val clientFactory = {
    val bootstraps: java.util.List[TransportClientBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
        java.util.Arrays.asList(new SaslClientBootstrap(transportConf, "", securityManager,
          securityManager.isSaslEncryptionEnabled()))
      } else {
        java.util.Collections.emptyList[TransportClientBootstrap]
      }
    transportContext.createClientFactory(bootstraps)
  }

  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 64))

  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
   * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
   */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
   * Remove the address's Outbox and stop it.
   */
  private[netty] def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  def startServer(port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
        java.util.Arrays.asList(new SaslServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList()
      }
    server = transportContext.createServer(port, bootstraps)
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      receiver.client.sendRpc(message.content, message.createCallback(receiver.client));
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }

  private[netty] def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      // Message to a local RPC endpoint.
      val promise = Promise[Any]()
      dispatcher.postLocalMessage(message, promise)
      promise.future.onComplete {
        case Success(response) =>
          val ack = response.asInstanceOf[Ack]
          logTrace(s"Received ack from ${ack.sender}")
        case Failure(e) =>
          logWarning(s"Exception when sending $message", e)
      }(ThreadUtils.sameThread)
    } else {
      // Message to a remote RPC endpoint.
      postToOutbox(message.receiver, OutboxMessage(serialize(message),
        (e) => {
          logWarning(s"Exception when sending $message", e)
        },
        (client, response) => {
          val ack = deserialize[Ack](client, response)
          logDebug(s"Receive ack from ${ack.sender}")
        }))
    }
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  private[netty] def ask(message: RequestMessage): Future[Any] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      val p = Promise[Any]()
      dispatcher.postLocalMessage(message, p)
      p.future.onComplete {
        case Success(response) =>
          val reply = response.asInstanceOf[AskResponse]
          if (reply.reply.isInstanceOf[RpcFailure]) {
            if (!promise.tryFailure(reply.reply.asInstanceOf[RpcFailure].e)) {
              logWarning(s"Ignore failure: ${reply.reply}")
            }
          } else if (!promise.trySuccess(reply.reply)) {
            logWarning(s"Ignore message: ${reply}")
          }
        case Failure(e) =>
          if (!promise.tryFailure(e)) {
            logWarning("Ignore Exception", e)
          }
      }(ThreadUtils.sameThread)
    } else {
      postToOutbox(message.receiver, OutboxMessage(serialize(message),
        (e) => {
          if (!promise.tryFailure(e)) {
            logWarning("Ignore Exception", e)
          }
        },
        (client, response) => {
          val reply = deserialize[AskResponse](client, response)
          if (reply.reply.isInstanceOf[RpcFailure]) {
            if (!promise.tryFailure(reply.reply.asInstanceOf[RpcFailure].e)) {
              logWarning(s"Ignore failure: ${reply.reply}")
            }
          } else if (!promise.trySuccess(reply.reply)) {
            logWarning(s"Ignore message: ${reply}")
          }
        }))
    }
    promise.future
  }

  private[netty] def serialize(content: Any): Array[Byte] = {
    val buffer = javaSerializerInstance.serialize(content)
    java.util.Arrays.copyOfRange(
      buffer.array(), buffer.arrayOffset + buffer.position, buffer.arrayOffset + buffer.limit)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: Array[Byte]): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](ByteBuffer.wrap(bytes))
      }
    }
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def uriOf(systemName: String, address: RpcAddress, endpointName: String): String =
    new RpcEndpointAddress(address, endpointName).toString

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
  }

  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

}

private[netty] object NettyRpcEnv extends Logging {

  /**
   * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
   * Use `currentEnv` to wrap the deserialization codes. E.g.,
   *
   * {{{
   *   NettyRpcEnv.currentEnv.withValue(this) {
   *     your deserialization codes
   *   }
   * }}}
   */
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
   * Similar to `currentEnv`, this variable references the client instance associated with an
   * RPC, in case it's needed to find out the remote address during deserialization.
   */
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)

}

private[netty] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {

  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.host, config.securityManager)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(actualPort)
        (nettyEnv, actualPort)
      }
      try {
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, "NettyRpcEnv")._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}

/**
 * The NettyRpcEnv version of RpcEndpointRef.
 *
 * This class behaves differently depending on where it's created. On the node that "owns" the
 * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
 *
 * On other machines that receive a serialized version of the reference, the behavior changes. The
 * instance will keep track of the TransportClient that sent the reference, so that messages
 * to the endpoint are sent over the client connection, instead of needing a new connection to
 * be opened.
 *
 * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
 * a client connection, since the process hosting the endpoint is not listening for incoming
 * connections. These refs should not be shared with 3rd parties, since they will not be able to
 * send messages to the endpoint.
 *
 * @param conf Spark configuration.
 * @param endpointAddress The address where the endpoint is listening.
 * @param nettyEnv The RpcEnv associated with this ref.
 * @param local Whether the referenced endpoint lives in the same process.
 */
private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv)
  extends RpcEndpointRef(conf) with Serializable with Logging {

  @transient @volatile var client: TransportClient = _

  private val _address = if (endpointAddress.rpcAddress != null) endpointAddress else null
  private val _name = endpointAddress.name

  override def address: RpcAddress = if (_address != null) _address.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = _name

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    val promise = Promise[Any]()
    val timeoutCancelable = nettyEnv.timeoutScheduler.schedule(new Runnable {
      override def run(): Unit = {
        promise.tryFailure(new TimeoutException("Cannot receive any reply in " + timeout.duration))
      }
    }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
    val f = nettyEnv.ask(RequestMessage(nettyEnv.address, this, message, true))
    f.onComplete { v =>
      timeoutCancelable.cancel(true)
      if (!promise.tryComplete(v)) {
        logWarning(s"Ignore message $v")
      }
    }(ThreadUtils.sameThread)
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(RequestMessage(nettyEnv.address, this, message, false))
  }

  override def toString: String = s"NettyRpcEndpointRef(${_address})"

  def toURI: URI = new URI(s"spark://${_address}")

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => _address == other._address
    case _ => false
  }

  final override def hashCode(): Int = if (_address == null) 0 else _address.hashCode()
}

/**
 * The message that is sent from the sender to the receiver.
 */
private[netty] case class RequestMessage(
    senderAddress: RpcAddress, receiver: NettyRpcEndpointRef, content: Any, needReply: Boolean)

/**
 * The base trait for all messages that are sent back from the receiver to the sender.
 */
private[netty] trait ResponseMessage

/**
 * The reply for `ask` from the receiver side.
 */
private[netty] case class AskResponse(sender: NettyRpcEndpointRef, reply: Any)
  extends ResponseMessage

/**
 * A message to send back to the receiver side. It's necessary because [[TransportClient]] only
 * clean the resources when it receives a reply.
 */
private[netty] case class Ack(sender: NettyRpcEndpointRef) extends ResponseMessage

/**
 * A response that indicates some failure happens in the receiver side.
 */
private[netty] case class RpcFailure(e: Throwable)

/**
 * Dispatches incoming RPCs to registered endpoints.
 *
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
 *
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher, nettyEnv: NettyRpcEnv) extends RpcHandler with Logging {

  // TODO: Can we add connection callback (channel registered) to the underlying framework?
  // A variable to track whether we should dispatch the RemoteProcessConnected message.
  private val clients = new ConcurrentHashMap[TransportClient, JBoolean]()

  override def receive(
      client: TransportClient,
      message: Array[Byte],
      callback: RpcResponseCallback): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostName, addr.getPort)
    if (clients.putIfAbsent(client, JBoolean.TRUE) == null) {
      dispatcher.postToAll(RemoteProcessConnected(clientAddr))
    }
    val requestMessage = nettyEnv.deserialize[RequestMessage](client, message)
    val messageToDispatch = if (requestMessage.senderAddress == null) {
        // Create a new message with the socket address of the client as the sender.
        RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content,
          requestMessage.needReply)
      } else {
        requestMessage
      }
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def getStreamManager: StreamManager = new OneForOneStreamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostName, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      logError("Exception before connecting to the client", cause)
    }
  }

  override def connectionTerminated(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostName, addr.getPort)
      clients.remove(client)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}
