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

package org.apache.spark.network

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder

import org.apache.spark._
import org.apache.spark.network._

/**
 * SecurityMessage is class that contains the connectionId and sasl token
 * used in SASL negotiation. SecurityMessage has routines for converting
 * it to and from a BufferMessage so that it can be sent by the ConnectionManager
 * and easily consumed by users when received.
 * The api was modeled after BlockMessage.
 *
 * The connectionId is the connectionId of the client side. Since
 * message passing is asynchronous and its possible for the server side (receiving)
 * to get multiple different types of messages on the same connection the connectionId
 * is used to know which connnection the security message is intended for.
 *
 * For instance, lets say we are node_0. We need to send data to node_1. The node_0 side
 * is acting as a client and connecting to node_1. SASL negotiation has to occur
 * between node_0 and node_1 before node_1 trusts node_0 so node_0 sends a security message.
 * node_1 receives the message from node_0 but before it can process it and send a response,
 * some thread on node_1 decides it needs to send data to node_0 so it connects to node_0
 * and sends a security message of its own to authenticate as a client. Now node_0 gets
 * the message and it needs to decide if this message is in response to it being a client
 * (from the first send) or if its just node_1 trying to connect to it to send data.  This
 * is where the connectionId field is used. node_0 can lookup the connectionId to see if
 * it is in response to it being a client or if its in response to someone sending other data.
 *
 * The format of a SecurityMessage as its sent is:
 *   - Length of the ConnectionId
 *   - ConnectionId
 *   - Length of the token
 *   - Token
 */
private[spark] class SecurityMessage() extends Logging {

  private var connectionId: String = null
  private var token: Array[Byte] = null

  def set(byteArr: Array[Byte], newconnectionId: String) {
    if (byteArr == null) {
      token = new Array[Byte](0)
    } else {
      token = byteArr
    }
    connectionId = newconnectionId
  }

  /**
   * Read the given buffer and set the members of this class.
   */
  def set(buffer: ByteBuffer) {
    val idLength = buffer.getInt()
    val idBuilder = new StringBuilder(idLength)
    for (i <- 1 to idLength) {
        idBuilder += buffer.getChar()
    }
    connectionId  = idBuilder.toString()

    val tokenLength = buffer.getInt()
    token = new Array[Byte](tokenLength)
    if (tokenLength > 0) {
      buffer.get(token, 0, tokenLength)
    }
  }

  def set(bufferMsg: BufferMessage) {
    val buffer = bufferMsg.buffers.apply(0)
    buffer.clear()
    set(buffer)
  }

  def getConnectionId: String = {
    return connectionId
  }

  def getToken: Array[Byte] = {
    return token
  }

  /**
   * Create a BufferMessage that can be sent by the ConnectionManager containing
   * the security information from this class.
   * @return BufferMessage
   */
  def toBufferMessage: BufferMessage = {
    val buffers = new ArrayBuffer[ByteBuffer]()

    // 4 bytes for the length of the connectionId
    // connectionId is of type char so multiple the length by 2 to get number of bytes
    // 4 bytes for the length of token
    // token is a byte buffer so just take the length
    var buffer = ByteBuffer.allocate(4 + connectionId.length() * 2 + 4 + token.length)
    buffer.putInt(connectionId.length())
    connectionId.foreach((x: Char) => buffer.putChar(x))
    buffer.putInt(token.length)

    if (token.length > 0) {
      buffer.put(token)
    }
    buffer.flip()
    buffers += buffer

    var message = Message.createBufferMessage(buffers)
    logDebug("message total size is : " + message.size)
    message.isSecurityNeg = true
    return message
  }

  override def toString: String = {
    "SecurityMessage [connId= " + connectionId + ", Token = " + token + "]"
  }
}

private[spark] object SecurityMessage {

  /**
   * Convert the given BufferMessage to a SecurityMessage by parsing the contents
   * of the BufferMessage and populating the SecurityMessage fields.
   * @param bufferMessage is a BufferMessage that was received
   * @return new SecurityMessage
   */
  def fromBufferMessage(bufferMessage: BufferMessage): SecurityMessage = {
    val newSecurityMessage = new SecurityMessage()
    newSecurityMessage.set(bufferMessage)
    newSecurityMessage
  }

  /**
   * Create a SecurityMessage to send from a given saslResponse.
   * @param response is the response to a challenge from the SaslClient or Saslserver
   * @param connectionId the client connectionId we are negotiation authentication for
   * @return a new SecurityMessage
   */
  def fromResponse(response : Array[Byte], connectionId : String) : SecurityMessage = {
    val newSecurityMessage = new SecurityMessage()
    newSecurityMessage.set(response, connectionId)
    newSecurityMessage
  }
}
