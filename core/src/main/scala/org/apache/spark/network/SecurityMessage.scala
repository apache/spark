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
  
  def toBufferMessage: BufferMessage = {
    val startTime = System.currentTimeMillis
    val buffers = new ArrayBuffer[ByteBuffer]()

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
 
  def fromBufferMessage(bufferMessage: BufferMessage): SecurityMessage = {
    val newSecurityMessage = new SecurityMessage()
    newSecurityMessage.set(bufferMessage)
    newSecurityMessage
  }

  def fromResponse(response : Array[Byte], newConnectionId : String) : SecurityMessage = {
    val newSecurityMessage = new SecurityMessage()
    newSecurityMessage.set(response, newConnectionId)
    newSecurityMessage
  }
}
