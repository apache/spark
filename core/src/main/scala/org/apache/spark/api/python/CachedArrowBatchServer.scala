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

import java.io.{BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.storage.{ArrowBatchBlockId, BlockId, BlockManager}


/**
 * A helper class to run cached arrow batch server.
 * Cached arrow batch server is for serving chunk data, when user calls
 * `pyspark.sql.chunk.readChunk` API, it creates connection to this server,
 * then sends chunk_id to the server, then the server responses chunk binary data
 * to client.
 * The server queries chuck data using chunk_id from block manager,
 * for chunk data storage logic, please refer to
 * `PersistDataFrameAsArrowBatchChunksPartitionEvaluator` class.
 */
class CachedArrowBatchServer(
  val sparkConf: SparkConf,
  val blockManager: BlockManager
) extends Logging {

  val authHelper = new SocketAuthHelper(sparkConf)

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

  private def handleConnection(sock: Socket): Unit = {
    val blockId = BlockId(readUtf8(sock))
    assert(blockId.isInstanceOf[ArrowBatchBlockId])

    var errMessage = "ok"
    var blockDataOpt: Option[Array[Byte]] = None

    try {
      val blockResult = blockManager.get[Array[Byte]](blockId)
      if (blockResult.isDefined) {
        blockDataOpt = Some(blockResult.get.data.next().asInstanceOf[Array[Byte]])
      } else {
        errMessage = s"The chunk $blockId data cache does not exist or has been removed"
      }
    } catch {
      case e: Exception =>
        errMessage = e.getMessage
    }

    writeUtf8(errMessage, sock)

    if (blockDataOpt.isDefined) {
      val out = new BufferedOutputStream(sock.getOutputStream())
      out.write(blockDataOpt.get)
      out.flush()
    }
  }

  def createConnectionThread(sock: Socket, threadName: String): Unit = {
    new Thread(threadName) {
      setDaemon(true)

      override def run(): Unit = {
        try {
          authHelper.authClient(sock)
          handleConnection(sock)
        } finally {
          JavaUtils.closeQuietly(sock)
        }
      }
    }.start()
  }

  def start(): Unit = {
    logTrace("Creating listening socket")

    new Thread("CachedArrowBatchServer-listener") {
      setDaemon(true)

      override def run(): Unit = {
        var sock: Socket = null

        var connectionCount = 0
        try {
          while (true) {
            sock = serverSocket.accept()
            connectionCount += 1
            createConnectionThread(
              sock, s"CachedArrowBatchServer-connection-$connectionCount"
            )
          }
        } catch {
          case e: SocketException =>
            // if serverSocket is closed, it means the server is shut down.
            // swallow the exception.
            if (!serverSocket.isClosed) {
              throw e
            }
        } finally {
          logTrace("Closing server")
          JavaUtils.closeQuietly(serverSocket)
        }
      }
    }.start()
  }

  def stop(): Unit = {
    JavaUtils.closeQuietly(serverSocket)
  }
}
