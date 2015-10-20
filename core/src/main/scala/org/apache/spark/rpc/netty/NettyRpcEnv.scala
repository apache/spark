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
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success}
import scala.util.control.NonFatal

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

  // Override numConnectionsPerPeer to 1 for RPC.
  private val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set("spark.shuffle.io.numConnectionsPerPeer", "1"),
    conf.getInt("spark.rpc.io.threads", 0))

  private val dispatcher: Dispatcher = new Dispatcher(this)

  private val transportContext =
    new TransportContext(transportConf, new NettyRpcHandler(dispatcher, this))

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
  private val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 64))

  @volatile private var server: TransportServer = _

  def start(port: Int): Unit = {
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

  override lazy val address: RpcAddress = {
    require(server != null, "NettyRpcEnv has not yet started")
    RpcAddress(host, server.getPort)
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.host, addr.port, RpcEndpointVerifier.NAME), this)
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
          logError(s"Exception when sending $message", e)
      }(ThreadUtils.sameThread)
    } else {
      // Message to a remote RPC endpoint.
      try {
        // `createClient` will block if it cannot find a known connection, so we should run it in
        // clientConnectionExecutor
        clientConnectionExecutor.execute(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            val client = clientFactory.createClient(remoteAddr.host, remoteAddr.port)
            client.sendRpc(serialize(message), new RpcResponseCallback {

              override def onFailure(e: Throwable): Unit = {
                logError(s"Exception when sending $message", e)
              }

              override def onSuccess(response: Array[Byte]): Unit = {
                val ack = deserialize[Ack](response)
                logDebug(s"Receive ack from ${ack.sender}")
              }
            })
          }
        })
      } catch {
        case e: RejectedExecutionException =>
          // `send` after shutting clientConnectionExecutor down, ignore it
          logWarning(s"Cannot send $message because RpcEnv is stopped")
      }
    }
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
      try {
        // `createClient` will block if it cannot find a known connection, so we should run it in
        // clientConnectionExecutor
        clientConnectionExecutor.execute(new Runnable {
          override def run(): Unit = {
            val client = clientFactory.createClient(remoteAddr.host, remoteAddr.port)
            client.sendRpc(serialize(message), new RpcResponseCallback {

              override def onFailure(e: Throwable): Unit = {
                if (!promise.tryFailure(e)) {
                  logWarning("Ignore Exception", e)
                }
              }

              override def onSuccess(response: Array[Byte]): Unit = {
                val reply = deserialize[AskResponse](response)
                if (reply.reply.isInstanceOf[RpcFailure]) {
                  if (!promise.tryFailure(reply.reply.asInstanceOf[RpcFailure].e)) {
                    logWarning(s"Ignore failure: ${reply.reply}")
                  }
                } else if (!promise.trySuccess(reply.reply)) {
                  logWarning(s"Ignore message: ${reply}")
                }
              }
            })
          }
        })
      } catch {
        case e: RejectedExecutionException =>
          if (!promise.tryFailure(e)) {
            logWarning(s"Ignore failure", e)
          }
      }
    }
    promise.future
  }

  private[netty] def serialize(content: Any): Array[Byte] = {
    val buffer = javaSerializerInstance.serialize(content)
    java.util.Arrays.copyOfRange(
      buffer.array(), buffer.arrayOffset + buffer.position, buffer.arrayOffset + buffer.limit)
  }

  private[netty] def deserialize[T: ClassTag](bytes: Array[Byte]): T = {
    deserialize { () =>
      javaSerializerInstance.deserialize[T](ByteBuffer.wrap(bytes))
    }
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def uriOf(systemName: String, address: RpcAddress, endpointName: String): String =
    new RpcEndpointAddress(address.host, address.port, endpointName).toString

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
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
    val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
      nettyEnv.start(actualPort)
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
}

private[netty] class NettyRpcEndpointRef(@transient private val conf: SparkConf)
  extends RpcEndpointRef(conf) with Serializable with Logging {

  @transient @volatile private var nettyEnv: NettyRpcEnv = _

  @transient @volatile private var _address: RpcEndpointAddress = _

  def this(conf: SparkConf, _address: RpcEndpointAddress, nettyEnv: NettyRpcEnv) {
    this(conf)
    this._address = _address
    this.nettyEnv = nettyEnv
  }

  override def address: RpcAddress = _address.toRpcAddress

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    _address = in.readObject().asInstanceOf[RpcEndpointAddress]
    nettyEnv = NettyRpcEnv.currentEnv.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    out.writeObject(_address)
  }

  override def name: String = _address.name

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
 * Maintain the mapping relations between client addresses and [[RpcEnv]] addresses, broadcast
 * network events and forward messages to [[Dispatcher]].
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher, nettyEnv: NettyRpcEnv) extends RpcHandler with Logging {

  private type ClientAddress = RpcAddress
  private type RemoteEnvAddress = RpcAddress

  // Store all client addresses and their NettyRpcEnv addresses.
  // TODO: Is this even necessary?
  @GuardedBy("this")
  private val remoteAddresses = new mutable.HashMap[ClientAddress, RemoteEnvAddress]()

  override def receive(
      client: TransportClient, message: Array[Byte], callback: RpcResponseCallback): Unit = {
    val requestMessage = nettyEnv.deserialize[RequestMessage](message)
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val remoteEnvAddress = requestMessage.senderAddress
    val clientAddr = RpcAddress(addr.getHostName, addr.getPort)

    // TODO: Can we add connection callback (channel registered) to the underlying framework?
    // A variable to track whether we should dispatch the RemoteProcessConnected message.
    var dispatchRemoteProcessConnected = false
    synchronized {
      if (remoteAddresses.put(clientAddr, remoteEnvAddress).isEmpty) {
        // clientAddr connects at the first time, fire "RemoteProcessConnected"
        dispatchRemoteProcessConnected = true
      }
    }
    if (dispatchRemoteProcessConnected) {
      dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
    }
    dispatcher.postRemoteMessage(requestMessage, callback)
  }

  override def getStreamManager: StreamManager = new OneForOneStreamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostName, addr.getPort)
      val broadcastMessage =
        synchronized {
          remoteAddresses.get(clientAddr).map(RemoteProcessConnectionError(cause, _))
        }
      if (broadcastMessage.isEmpty) {
        logError(cause.getMessage, cause)
      } else {
        dispatcher.postToAll(broadcastMessage.get)
      }
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
      val messageOpt: Option[RemoteProcessDisconnected] =
      synchronized {
        remoteAddresses.get(clientAddr).flatMap { remoteEnvAddress =>
          remoteAddresses -= clientAddr
          Some(RemoteProcessDisconnected(remoteEnvAddress))
        }
      }
      messageOpt.foreach(dispatcher.postToAll)
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}
