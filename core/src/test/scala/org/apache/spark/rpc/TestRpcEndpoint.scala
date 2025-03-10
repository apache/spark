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

import scala.collection.mutable.ArrayBuffer

import org.scalactic.TripleEquals
import org.scalatest.Assertions._

class TestRpcEndpoint extends ThreadSafeRpcEndpoint with TripleEquals {

  override val rpcEnv: RpcEnv = null

  @volatile private var receiveMessages = ArrayBuffer[Any]()

  @volatile private var receiveAndReplyMessages = ArrayBuffer[Any]()

  @volatile private var onConnectedMessages = ArrayBuffer[RpcAddress]()

  @volatile private var onDisconnectedMessages = ArrayBuffer[RpcAddress]()

  @volatile private var onNetworkErrorMessages = ArrayBuffer[(Throwable, RpcAddress)]()

  @volatile private var started = false

  @volatile private var stopped = false

  override def receive: PartialFunction[Any, Unit] = {
    case message: Any => receiveMessages += message
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message: Any => receiveAndReplyMessages += message
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    onConnectedMessages += remoteAddress
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    onNetworkErrorMessages += cause -> remoteAddress
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    onDisconnectedMessages += remoteAddress
  }

  def numReceiveMessages: Int = receiveMessages.size

  override def onStart(): Unit = {
    started = true
  }

  override def onStop(): Unit = {
    stopped = true
  }

  def verifyStarted(): Unit = {
    assert(started, "RpcEndpoint is not started")
  }

  def verifyStopped(): Unit = {
    assert(stopped, "RpcEndpoint is not stopped")
  }

  def verifyReceiveMessages(expected: Seq[Any]): Unit = {
    assert(receiveMessages === expected)
  }

  def verifySingleReceiveMessage(message: Any): Unit = {
    verifyReceiveMessages(List(message))
  }

  def verifyReceiveAndReplyMessages(expected: Seq[Any]): Unit = {
    assert(receiveAndReplyMessages === expected)
  }

  def verifySingleReceiveAndReplyMessage(message: Any): Unit = {
    verifyReceiveAndReplyMessages(List(message))
  }

  def verifySingleOnConnectedMessage(remoteAddress: RpcAddress): Unit = {
    verifyOnConnectedMessages(List(remoteAddress))
  }

  def verifyOnConnectedMessages(expected: Seq[RpcAddress]): Unit = {
    assert(onConnectedMessages === expected)
  }

  def verifySingleOnDisconnectedMessage(remoteAddress: RpcAddress): Unit = {
    verifyOnDisconnectedMessages(List(remoteAddress))
  }

  def verifyOnDisconnectedMessages(expected: Seq[RpcAddress]): Unit = {
    assert(onDisconnectedMessages === expected)
  }

  def verifySingleOnNetworkErrorMessage(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    verifyOnNetworkErrorMessages(List(cause -> remoteAddress))
  }

  def verifyOnNetworkErrorMessages(expected: Seq[(Throwable, RpcAddress)]): Unit = {
    assert(onNetworkErrorMessages === expected)
  }
}
