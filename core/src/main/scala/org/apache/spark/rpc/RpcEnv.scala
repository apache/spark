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

import scala.concurrent.Future

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.util.{RpcUtils, Utils}


/**
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
 */
private[spark] object RpcEnv {

  private def getRpcEnvFactory(conf: SparkConf): RpcEnvFactory = {
    val rpcEnvNames = Map(
      "akka" -> "org.apache.spark.rpc.akka.AkkaRpcEnvFactory",
      "netty" -> "org.apache.spark.rpc.netty.NettyRpcEnvFactory")
    val rpcEnvName = conf.get("spark.rpc", "netty")
    val rpcEnvFactoryClassName = rpcEnvNames.getOrElse(rpcEnvName.toLowerCase, rpcEnvName)
    Utils.classForName(rpcEnvFactoryClassName).newInstance().asInstanceOf[RpcEnvFactory]
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
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

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
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
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

  /**
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T
}


private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    host: String,
    port: Int,
    securityManager: SecurityManager)
