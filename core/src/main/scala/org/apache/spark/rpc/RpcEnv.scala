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

package org.apache.spark.rpc

import java.net.URI

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import org.apache.spark.{Logging, SparkException, SecurityManager, SparkConf}
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = AkkaUtils.lookupTimeout(conf)

  /**
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Return the address that [[RpcEnv]] is listening to.
   */
  def address: RpcAddress

  /**
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    Await.result(asyncSetupEndpointRefByURI(uri), defaultLookupTimeout)
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`
   * asynchronously.
   */
  def asyncSetupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): Future[RpcEndpointRef] = {
    asyncSetupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   *
   * TODO do we need a timeout parameter?
   */
  def awaitTermination(): Unit

  /**
   * Create a URI used to create a [[RpcEndpointRef]]. Use this one to create the URI instead of
   * creating it manually because different [[RpcEnv]] may have different formats.
   */
  def uriOf(systemName: String, address: RpcAddress, endpointName: String): String
}

private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    host: String,
    port: Int,
    securityManager: SecurityManager)

/**
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
 */
private[spark] object RpcEnv {

  private def getRpcEnvFactory(conf: SparkConf): RpcEnvFactory = {
    // Add more RpcEnv implementations here
    val rpcEnvNames = Map("akka" -> "org.apache.spark.rpc.akka.AkkaRpcEnvFactory")
    val rpcEnvName = conf.get("spark.rpc", "akka")
    val rpcEnvFactoryClassName = rpcEnvNames.getOrElse(rpcEnvName.toLowerCase, rpcEnvName)
    Class.forName(rpcEnvFactoryClassName, true, Utils.getContextOrSparkClassLoader).
      newInstance().asInstanceOf[RpcEnvFactory]
  }

  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager): RpcEnv = {
    // Using Reflection to create the RpcEnv to avoid to depend on Akka directly
    val config = RpcEnvConfig(conf, name, host, port, securityManager)
    getRpcEnvFactory(conf).create(config)
  }

}

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The lift-cycle will be:
 *
 * constructor onStart receive* onStop
 *
 * Note: `receive` can be called concurrently. If you want `receive` is thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */
  val rpcEnv: RpcEnv

  /**
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * Process messages from [[RpcEndpointRef.send]] or [[RpcCallContext.reply)]]. If receiving a
   * unmatched message, [[SparkException]] will be thrown and sent to `onError`.
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * Process messages from [[RpcEndpointRef.sendWithReply]]. If receiving a unmatched message,
   * [[SparkException]] will be thrown and sent to `onError`.
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * Call onError when any exception is thrown during handling messages.
   *
   * @param cause
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping.
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is connected to the current node.
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is lost.
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * A convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(self)
    }
  }
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 */
trait ThreadSafeRpcEndpoint extends RpcEndpoint

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] abstract class RpcEndpointRef(@transient conf: SparkConf)
  extends Serializable with Logging {

  private[this] val maxRetries = conf.getInt("spark.akka.num.retries", 3)
  private[this] val retryWaitMs = conf.getLong("spark.akka.retry.wait", 3000)
  private[this] val defaultTimeout = conf.getLong("spark.akka.lookupTimeout", 30) seconds

  /**
   * return the address for the [[RpcEndpointRef]]
   */
  def address: RpcAddress

  def name: String

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a `Future` to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def sendWithReply[T: ClassTag](message: Any): Future[T] = sendWithReply(message, defaultTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a `Future` to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   */
  def sendWithReply[T: ClassTag](message: Any, timeout: FiniteDuration): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint]] and get its result within a default
   * timeout, or throw a SparkException if this fails even after the default number of retries.
   * The default `timeout` will be used in every trial of calling `sendWithReply`. Because this
   * method retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithReply[T: ClassTag](message: Any): T = askWithReply(message, defaultTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receive]] and get its result within a
   * specified timeout, throw a SparkException if this fails even after the specified number of
   * retries. `timeout` will be used in every trial of calling `sendWithReply`. Because this method
   * retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithReply[T: ClassTag](message: Any, timeout: FiniteDuration): T = {
    // TODO: Consider removing multiple attempts
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxRetries) {
      attempts += 1
      try {
        val future = sendWithReply[T](message, timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new SparkException("Actor returned null")
        }
        return result
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }
      Thread.sleep(retryWaitMs)
    }

    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

}

/**
 * Represent a host with a port
 */
private[spark] case class RpcAddress(host: String, port: Int) {
  // TODO do we need to add the type of RpcEnv in the address?

  val hostPort: String = host + ":" + port

  override val toString: String = hostPort
}

private[spark] object RpcAddress {

  /**
   * Return the [[RpcAddress]] represented by `uri`.
   */
  def fromURI(uri: URI): RpcAddress = {
    RpcAddress(uri.getHost, uri.getPort)
  }

  /**
   * Return the [[RpcAddress]] represented by `uri`.
   */
  def fromURIString(uri: String): RpcAddress = {
    fromURI(new java.net.URI(uri))
  }

  def fromSparkURL(sparkUrl: String): RpcAddress = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    RpcAddress(host, port)
  }
}

/**
 * A callback that [[RpcEndpoint]] can use it to send back a message or failure. It's thread-safe
 * and can be called in any thread.
 */
private[spark] trait RpcCallContext {

  /**
   * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]]
   * will be called.
   */
  def reply(response: Any): Unit

  /**
   * Report a failure to the sender.
   */
  def sendFailure(e: Throwable): Unit

  /**
   * The sender of this message.
   */
  def sender: RpcEndpointRef
}
