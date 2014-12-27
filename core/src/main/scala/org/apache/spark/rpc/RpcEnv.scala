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

  def setupEndpoint(name: String, endpointCreator: => RpcEndpoint): RpcEndpointRef

  def setupDriverEndpointRef(name: String): RpcEndpointRef

  def setupEndpointRefByUrl(url: String): RpcEndpointRef

  def stop(endpoint: RpcEndpointRef): Unit

  def stopAll(): Unit
}


/**
 * An end point for the RPC that defines what functions to trigger given a message.
 */
trait RpcEndpoint {

  val rpcEnv: RpcEnv

  /**
   * Provide the implicit sender.
   */
  implicit final def self: RpcEndpointRef = rpcEnv.endpointRef(this)

  /**
   * Same assumption like Actor: messages sent to a RpcEndpoint will be delivered in sequence, and
   * messages from the same RpcEndpoint will be delivered in order.
   *
   * @param sender
   * @return
   */
  def receive(sender: RpcEndpointRef): PartialFunction[Any, Unit]

  def remoteConnectionTerminated(remoteAddress: String): Unit = {
    // By default, do nothing.
  }

  final def stop(): Unit = {
    rpcEnv.stop(self)
  }
}


object RpcEndpoint {
  final val noSender: RpcEndpointRef = null
}

/**
 * A reference for a remote [[RpcEndpoint]].
 */
trait RpcEndpointRef {

  def address: String

  def askWithReply[T](message: Any): T

  /**
   * Send a message to the remote endpoint asynchronously. No delivery guarantee is provided.
   */
  def send(message: Any)(implicit sender: RpcEndpointRef = RpcEndpoint.noSender): Unit
}
