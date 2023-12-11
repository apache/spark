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

package org.apache.spark.api.python

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils

import java.net.{InetAddress, ServerSocket, Socket}
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.storage.{ArrowBatchBlockId, BlockId}

import java.io.{BufferedOutputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable.ArrayBuffer


class CachedArrowBatchServer extends Logging {

  val serverSocket = new ServerSocket(
    0, 1, InetAddress.getLoopbackAddress()
  )

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

  def handleConnection(sock: Socket): Unit = {
    val blockId = BlockId(readUtf8(sock))
    assert(blockId.isInstanceOf[ArrowBatchBlockId])

    val blockManager = SparkEnv.get.blockManager

    val blockData =
      blockManager.get[Array[Byte]](blockId).get.data.next().asInstanceOf[Array[Byte]]
    val out = new BufferedOutputStream(sock.getOutputStream())
    out.write(blockData)
    out.flush()
  }

  def start(): (Int, String) = {
    logTrace("Creating listening socket")

    val authHelper = new SocketAuthHelper(SparkEnv.get.conf)

    new Thread(s"CachedArrowBatchServer-listener") {
      setDaemon(true)

      override def run(): Unit = {
        var sock: Socket = null

        var connectionCount = 0
        try {
          while (true) {
            sock = serverSocket.accept()
            connectionCount += 1
            new Thread(s"CachedArrowBatchServer-connection-$connectionCount") {
              setDaemon(true)
              try {
                authHelper.authClient(sock)
                handleConnection(sock)
              } finally {
                JavaUtils.closeQuietly(sock)
              }
            }
          }
        } finally {
          logTrace("Closing server")
          JavaUtils.closeQuietly(serverSocket)
        }
      }
    }
    (serverSocket.getLocalPort, authHelper.secret)
  }

  def stop(): Unit = {
    JavaUtils.closeQuietly(serverSocket)
  }
}
