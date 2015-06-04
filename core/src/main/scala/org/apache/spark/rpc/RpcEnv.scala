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
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.concurrent.{Awaitable, Await, Future}
import scala.language.postfixOps

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.util.{ThreadUtils, RpcUtils, Utils}


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
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupTimeout(conf)

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
 * Represents a host and port.
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
 * An exception thrown if RpcTimeout modifies a [[TimeoutException]].
 */
private[rpc] class RpcTimeoutException(message: String)
  extends TimeoutException(message)


/**
 * Associates a timeout with a description so that a when a TimeoutException occurs, additional
 * context about the timeout can be amended to the exception message.
 * @param timeout timeout duration in seconds
 * @param description description to be displayed in a timeout exception
 */
private[spark] class RpcTimeout(timeout: FiniteDuration, description: String) {

  /** Get the timeout duration */
  def duration: FiniteDuration = timeout

  /** Get the message associated with this timeout */
  def message: String = description

  /** Amends the standard message of TimeoutException to include the description */
  def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage() + " " + description)
  }

  /**
   * Add a callback to the given Future so that if it completes as failed with a TimeoutException
   * then the timeout description is added to the message
   */
  def addMessageIfTimeout[T](future: Future[T]): Future[T] = {
    future.recover {
      // Add a warning message if Future is passed to addMessageIfTimeoutTest more than once
      case rte: RpcTimeoutException => throw new RpcTimeoutException(rte.getMessage() +
        " (Future has multiple calls to RpcTimeout.addMessageIfTimeoutTest)")
      // Any other TimeoutException get converted to a RpcTimeoutException with modified message
      case te: TimeoutException => throw createRpcTimeoutException(te)
    }(ThreadUtils.sameThread)
  }

  /** Applies the duration to create future before calling addMessageIfTimeout*/
  def addMessageIfTimeout[T](f: FiniteDuration => Future[T]): Future[T] = {
    addMessageIfTimeout(f(duration))
  }

  /**
   * Waits for a completed result to catch and amend a TimeoutException message
   * @param  awaitable  the `Awaitable` to be awaited
   * @throws RpcTimeoutException if after waiting for the specified time `awaitable`
   *         is still not ready
   */
  def awaitResult[T](awaitable: Awaitable[T]): T = {
    try {
      Await.result(awaitable, duration)
    }
    catch {
      // The exception has already been converted to a RpcTimeoutException so just raise it
      case rte: RpcTimeoutException => throw rte
      // Any other TimeoutException get converted to a RpcTimeoutException with modified message
      case te: TimeoutException => throw createRpcTimeoutException(te)
    }
  }
}


/**
 * Create an RpcTimeout using a configuration property that controls the timeout duration so when
 * a TimeoutException is thrown, the property key will be indicated in the message.
 */
object RpcTimeout {

  private[this] val messagePrefix = "This timeout is controlled by "

  /**
   * Lookup the timeout property in the configuration and create
   * a RpcTimeout with the property key in the description.
   * @param conf configuration properties containing the timeout
   * @param timeoutProp property key for the timeout in seconds
   * @throws NoSuchElementException if property is not set
   */
  def apply(conf: SparkConf, timeoutProp: String): RpcTimeout = {
    val timeout = { conf.getTimeAsSeconds(timeoutProp) seconds }
    new RpcTimeout(timeout, messagePrefix + timeoutProp)
  }

  /**
   * Lookup the timeout property in the configuration and create
   * a RpcTimeout with the property key in the description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutProp property key for the timeout in seconds
   * @param defaultValue default timeout value in seconds if property not found
   */
  def apply(conf: SparkConf, timeoutProp: String, defaultValue: String): RpcTimeout = {
    val timeout = { conf.getTimeAsSeconds(timeoutProp, defaultValue) seconds }
    new RpcTimeout(timeout, messagePrefix + timeoutProp)
  }

  /**
   * Lookup prioritized list of timeout properties in the configuration
   * and create a RpcTimeout with the first set property key in the
   * description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutPropList prioritized list of property keys for the timeout in seconds
   * @param defaultValue default timeout value in seconds if no properties found
   */
  def apply(conf: SparkConf, timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
    require(timeoutPropList.nonEmpty)

    // Find the first set property or use the default value with the first property
    val itr = timeoutPropList.iterator
    var foundProp =  None: Option[(String, String)]
    while (itr.hasNext && foundProp.isEmpty){
      val propKey = itr.next()
      conf.getOption(propKey) match {
        case Some(prop) => foundProp = Some(propKey,prop)
        case None =>
      }
    }
    val finalProp = foundProp.getOrElse(timeoutPropList.head, defaultValue)
    val timeout = { Utils.timeStringAsSeconds(finalProp._2) seconds }
    new RpcTimeout(timeout, messagePrefix + finalProp._1)
  }
}
