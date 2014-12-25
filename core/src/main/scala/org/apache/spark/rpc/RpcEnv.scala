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

import org.slf4j.Logger

/**
 * An RPC environment.
 */
trait RpcEnv {
  def setupEndPoint(name: String, endpoint: RpcEndPoint): RpcEndPointRef

  def setupDriverEndPointRef(name: String): RpcEndPointRef

  def setupEndPointRefByUrl(url: String): RpcEndPointRef

  def stop(endpoint: RpcEndPointRef): Unit

  def stopAll(): Unit
}


/**
 * An end point for the RPC that defines what functions to trigger given a message.
 */
trait RpcEndPoint {

  def receive(sender: RpcEndPointRef): PartialFunction[Any, Unit]

  def remoteConnectionTerminated(remoteAddress: String): Unit = {
    // By default, do nothing.
  }

  protected def log: Logger

  private[rpc] def logMessage = log
}


/**
 * A reference for a remote [[RpcEndPoint]].
 */
trait RpcEndPointRef {

  def address: String

  def askWithReply[T](message: Any): T

  /**
   * Send a message to the remote end point asynchronously. No delivery guarantee is provided.
   */
  def send(message: Any): Unit
}
