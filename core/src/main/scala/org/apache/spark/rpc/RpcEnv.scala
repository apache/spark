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

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import com.google.common.annotations.VisibleForTesting

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.Utils

/**
 * An RPC environment.
 */
trait RpcEnv {

  /**
   * Need this map to set up the `sender` for the send method.
   */
  private val endpointToRef = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]()

  /**
   * Need this map to remove `RpcEndpoint` from `endpointToRef` via a `RpcEndpointRef`
   */
  private val refToEndpoint = new ConcurrentHashMap[RpcEndpointRef, RpcEndpoint]()


  protected def registerEndpoint(endpoint: RpcEndpoint, endpointRef: RpcEndpointRef): Unit = {
    refToEndpoint.put(endpointRef, endpoint)
    endpointToRef.put(endpoint, endpointRef)
  }

  protected def unregisterEndpoint(endpointRef: RpcEndpointRef): Unit = {
    val endpoint = refToEndpoint.remove(endpointRef)
    if (endpoint != null) {
      endpointToRef.remove(endpoint)
    }
  }

  def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    val endpointRef = endpointToRef.get(endpoint)
    require(endpointRef != null, s"Cannot find RpcEndpointRef of ${endpoint} in ${this}")
    endpointRef
  }

  def boundPort: Int

  def setupEndpoint(name: String, endpointCreator: => RpcEndpoint): RpcEndpointRef

  def setupDriverEndpointRef(name: String): RpcEndpointRef

  def setupEndpointRefByUrl(url: String): RpcEndpointRef

  def stop(endpoint: RpcEndpointRef): Unit

  def stopAll(): Unit

  def awaitTermination(): Unit
}

object RpcEnv {

  private def getRpcEnvCompanion(conf: SparkConf): AnyRef = {
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
    val companion = getRpcEnvCompanion(conf)
    companion.getClass.getMethod("apply",
        classOf[String],
        classOf[String],
        java.lang.Integer.TYPE,
        classOf[SparkConf],
        classOf[SecurityManager]).
      invoke(companion, name, host, port: java.lang.Integer, conf, securityManager).
      asInstanceOf[RpcEnv]
  }

  @VisibleForTesting
  def create(name: String, conf: SparkConf): RpcEnv = {
    val companion = getRpcEnvCompanion(conf)
    companion.getClass.getMethod("apply", classOf[String], classOf[SparkConf]).
      invoke(companion, name, conf).asInstanceOf[RpcEnv]
  }

}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * RpcEndpoint will be guaranteed that `preStart`, `receive` and `remoteConnectionTerminated` will
 * be called in sequence.
 *
 * Happen before relation:
 *
 * constructor preStart receive* remoteConnectionTerminated
 *
 * ?? Need to guarantee that no message will be delivered after remoteConnectionTerminated ??
 */
trait RpcEndpoint {

  val rpcEnv: RpcEnv

  /**
   * Provide the implicit sender. `self` will become valid when `preStart` is called.
   */
  implicit final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  def onStart(): Unit = {}

  /**
   * Same assumption like Actor: messages sent to a RpcEndpoint will be delivered in sequence, and
   * messages from the same RpcEndpoint will be delivered in order.
   *
   * @param sender
   * @return
   */
  def receive(sender: RpcEndpointRef): PartialFunction[Any, Unit]

  /**
   * Call onError when any exception is thrown during handling messages.
   *
   * @param cause
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  def onStop(): Unit = {}

  final def stop(): Unit = {
    rpcEnv.stop(self)
  }
}

/**
 * A RpcEndoint interested in network events.
 */
trait NetworkRpcEndpoint extends RpcEndpoint {

  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, throw e and let RpcEnv handle it
  }
}

object RpcEndpoint {
  final val noSender: RpcEndpointRef = null
}

/**
 * A reference for a remote [[RpcEndpoint]].
 */
trait RpcEndpointRef {
  Master

  def address: RpcAddress

  def ask[T: ClassTag](message: Any): Future[T]

  def ask[T: ClassTag](message: Any, timeout: FiniteDuration): Future[T]

  def askWithReply[T](message: Any): T

  def askWithReply[T](message: Any, timeout: FiniteDuration): T

  /**
   * Send a message to the remote endpoint asynchronously. No delivery guarantee is provided.
   */
  def send(message: Any)(implicit sender: RpcEndpointRef = RpcEndpoint.noSender): Unit
}

case class RpcAddress(host: String, port: Int) {

  val hostPort: String = host + ":" + port

  override val toString: String = hostPort
}

object RpcAddress {

  def fromURIString(uri: String): RpcAddress = {
    val u = new java.net.URI(uri)
    RpcAddress(u.getHost, u.getPort)
  }

}
