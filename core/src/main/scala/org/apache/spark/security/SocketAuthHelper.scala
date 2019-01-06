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

package org.apache.spark.security

import java.io.{DataInputStream, DataOutputStream, InputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

/**
 * A class that can be used to add a simple authentication protocol to socket-based communication.
 *
 * The protocol is simple: an auth secret is written to the socket, and the other side checks the
 * secret and writes either "ok" or "err" to the output. If authentication fails, the socket is
 * not expected to be valid anymore.
 *
 * There's no secrecy, so this relies on the sockets being either local or somehow encrypted.
 */
private[spark] class SocketAuthHelper(conf: SparkConf) {

  val secret = Utils.createSecret(conf)

  /**
   * Read the auth secret from the socket and compare to the expected value. Write the reply back
   * to the socket.
   *
   * If authentication fails or error is thrown, this method will close the socket.
   *
   * @param s The client socket.
   * @throws IllegalArgumentException If authentication fails.
   */
  def authClient(s: Socket): Unit = {
    var shouldClose = true
    try {
      // Set the socket timeout while checking the auth secret. Reset it before returning.
      val currentTimeout = s.getSoTimeout()
      try {
        s.setSoTimeout(10000)
        val clientSecret = readUtf8(s)
        if (secret == clientSecret) {
          writeUtf8("ok", s)
          shouldClose = false
        } else {
          writeUtf8("err", s)
          throw new IllegalArgumentException("Authentication failed.")
        }
      } finally {
        s.setSoTimeout(currentTimeout)
      }
    } finally {
      if (shouldClose) {
        JavaUtils.closeQuietly(s)
      }
    }
  }

  /**
   * Authenticate with a server by writing the auth secret and checking the server's reply.
   *
   * If authentication fails or error is thrown, this method will close the socket.
   *
   * @param s The socket connected to the server.
   * @throws IllegalArgumentException If authentication fails.
   */
  def authToServer(s: Socket): Unit = {
    var shouldClose = true
    try {
      writeUtf8(secret, s)

      val reply = readUtf8(s)
      if (reply != "ok") {
        throw new IllegalArgumentException("Authentication failed.")
      } else {
        shouldClose = false
      }
    } finally {
      if (shouldClose) {
        JavaUtils.closeQuietly(s)
      }
    }
  }

  protected def readUtf8(s: Socket): String = {
    val din = new DataInputStream(s.getInputStream())
    val len = din.readInt()
    val bytes = new Array[Byte](len)
    din.readFully(bytes)
    new String(bytes, UTF_8)
  }

  protected def writeUtf8(str: String, s: Socket): Unit = {
    val bytes = str.getBytes(UTF_8)
    val dout = new DataOutputStream(s.getOutputStream())
    dout.writeInt(bytes.length)
    dout.write(bytes, 0, bytes.length)
    dout.flush()
  }

}
