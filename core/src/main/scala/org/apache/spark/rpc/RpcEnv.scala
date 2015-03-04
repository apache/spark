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

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import org.apache.spark.{SparkException, SecurityManager, SparkConf}
import org.apache.spark.util.Utils

/**
 * An RPC environment.
 */
private[spark] trait RpcEnv {

  /**
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]].
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Return an ActionScheduler for the caller to run long-time actions out of the current thread.
   */
  def scheduler: ActionScheduler

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
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] should
   * make sure thread-safely sending messages to [[RpcEndpoint]].
   */
  def setupThreadSafeEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Retrieve a [[RpcEndpointRef]] which is located in the driver via its name.
   */
  def setupDriverEndpointRef(name: String): RpcEndpointRef

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `url`.
   */
  def setupEndpointRefByUrl(url: String): RpcEndpointRef

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`
   */
  def setupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef

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
 * A RpcEnv implementation must have a companion object with an
 * `apply(config: RpcEnvConfig): RpcEnv` method so that it can be created via Reflection.
 *
 * {{{
 * object MyCustomRpcEnv {
 *   def apply(config: RpcEnvConfig): RpcEnv = {
 *     ...
 *   }
 * }
 * }}}
 */
private[spark] object RpcEnv {

  private def getRpcEnvCompanion(conf: SparkConf): AnyRef = {
    // Add more RpcEnv implementations here
    val rpcEnvNames = Map("akka" -> "org.apache.spark.rpc.akka.AkkaRpcEnv")
    val rpcEnvName = conf.get("spark.rpc", "akka")
    val rpcEnvClassName = rpcEnvNames.getOrElse(rpcEnvName.toLowerCase, rpcEnvName)
    val companion = Class.forName(
      rpcEnvClassName + "$", true, Utils.getContextOrSparkClassLoader).getField("MODULE$").get(null)
    companion
  }

  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager): RpcEnv = {
    // Using Reflection to create the RpcEnv to avoid to depend on Akka directly
    val config = RpcEnvConfig(conf, name, host, port, securityManager)
    val companion = getRpcEnvCompanion(conf)
    companion.getClass.getMethod("apply", classOf[RpcEnvConfig]).
      invoke(companion, config).asInstanceOf[RpcEnv]
  }

}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * RpcEndpoint will be guaranteed that `onStart`, `receive` and `onStop` will
 * be called in sequence.
 *
 * The lift-cycle will be:
 *
 * constructor onStart receive* onStop
 *
 * Note: `receive` can be called concurrently. If you want `receive` is thread-safe, please use
 * [[RpcEnv.setupThreadSafeEndpoint]]
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
   * called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * Process messages from [[RpcEndpointRef.send]] or [[RpcResponse.reply)]]
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ =>
      // network events will be passed here by default, so do nothing by default to avoid noise.
  }

  /**
   * Process messages from [[RpcEndpointRef.sendWithReply]] or [[RpcResponse.replyWithSender)]]
   */
  def receiveAndReply(response: RpcResponse): PartialFunction[Any, Unit] = {
    case _ => response.fail(new SparkException(self + " won't reply anything"))
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
   * An convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(self)
    }
  }
}

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] trait RpcEndpointRef {

  /**
   * return the address for the [[RpcEndpointRef]]
   */
  def address: RpcAddress

  def name: String

  /**
   * Send a message to the corresponding [[RpcEndpoint]] and get its result within a default
   * timeout, or throw a SparkException if this fails even after the default number of retries.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithReply[T: ClassTag](message: Any): T

  /**
   * Send a message to the corresponding [[RpcEndpoint.receive]] and get its result within a
   * specified timeout, throw a SparkException if this fails even after the specified number of
   * retries.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithReply[T: ClassTag](message: Any, timeout: FiniteDuration): T

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] asynchronously.
   * Fire-and-forget semantics.
   *
   * The receiver will reply to sender's [[RpcEndpoint.receive]] or [[RpcEndpoint.receiveAndReply]]
   * depending on which one of [[RpcResponse.reply]]s is called.
   */
  def sendWithReply(message: Any, sender: RpcEndpointRef): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a `Future` to
   * receive the reply within a default timeout.
   */
  def sendWithReply[T: ClassTag](message: Any): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a `Future` to
   * receive the reply within the specified timeout.
   */
  def sendWithReply[T: ClassTag](message: Any, timeout: FiniteDuration): Future[T]

  def toURI: URI
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
  def fromURIString(uri: String): RpcAddress = {
    val u = new java.net.URI(uri)
    RpcAddress(u.getHost, u.getPort)
  }

  def fromSparkURL(sparkUrl: String): RpcAddress = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    RpcAddress(host, port)
  }
}

/**
 * Indicate that a new connection is established.
 *
 * @param address the remote address of the connection
 */
private[spark] case class AssociatedEvent(address: RpcAddress)

/**
 * Indicate a disconnection from a remote address.
 *
 * @param address the remote address of the connection
 */
private[spark] case class DisassociatedEvent(address: RpcAddress)

/**
 * Indicate a network error.
 * @param address the remote address of the connection which this error happens on.
 * @param cause the cause of the network error.
 */
private[spark] case class NetworkErrorEvent(address: RpcAddress, cause: Throwable)

/**
 * A callback that [[RpcEndpoint]] can use it to send back a message or failure.
 */
private[spark] trait RpcResponse {

  /**
   * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]]
   * will be called.
   */
  def reply(response: Any): Unit

  /**
   * Reply a message to the corresponding [[RpcEndpoint.receiveAndReply]]. If you use this one to
   * reply, it means you expect the target [[RpcEndpoint]] should reply you something.
   *
   * TODO better method name?
   *
   * @param response the response message
   * @param sender who replies this message. The target [[RpcEndpoint]] will use `sender` to send
   *               back something.
   */
  def replyWithSender(response: Any, sender: RpcEndpointRef): Unit

  /**
   * Report a failure to the sender.
   */
  def fail(e: Throwable): Unit
}
