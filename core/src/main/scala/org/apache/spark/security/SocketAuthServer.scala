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

import java.io.{BufferedOutputStream, OutputStream}
import java.net.{InetAddress, ServerSocket, Socket}

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Try
import org.apache.spark.SparkEnv
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.{ThreadUtils, Utils}


/**
 * Creates a server in the JVM to communicate with external processes (e.g., Python and R) for
 * handling one batch of data, with authentication and error handling.
 */
private[spark] abstract class SocketAuthServer[T](
    authHelper: SocketAuthHelper,
    threadName: String) {

  def this(env: SparkEnv, threadName: String) = this(new SocketAuthHelper(env.conf), threadName)
  def this(threadName: String) = this(SparkEnv.get, threadName)

  private val promise = Promise[T]()

  private def startServer(): (Int, String) = {
    val serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))
    // Close the socket if no connection in 15 seconds
    serverSocket.setSoTimeout(15000)

    new Thread(threadName) {
      setDaemon(true)
      override def run(): Unit = {
        var sock: Socket = null
        try {
          sock = serverSocket.accept()
          authHelper.authClient(sock)
          promise.complete(Try(handleConnection(sock)))
        } finally {
          JavaUtils.closeQuietly(serverSocket)
          JavaUtils.closeQuietly(sock)
        }
      }
    }.start()
    (serverSocket.getLocalPort, authHelper.secret)
  }

  val (port, secret) = startServer()

  /**
   * Handle a connection which has already been authenticated.  Any error from this function
   * will clean up this connection and the entire server, and get propagated to [[getResult]].
   */
  def handleConnection(sock: Socket): T

  /**
   * Blocks indefinitely for [[handleConnection]] to finish, and returns that result.  If
   * handleConnection throws an exception, this will throw an exception which includes the original
   * exception as a cause.
   */
  def getResult(): T = {
    getResult(Duration.Inf)
  }

  def getResult(wait: Duration): T = {
    ThreadUtils.awaitResult(promise.future, wait)
  }

}

/**
 * Create a socket server class and run user function on the socket in a background thread.
 * This is the same as calling SocketAuthServer.setupOneConnectionServer except it creates
 * a server object that can then be synced from Python.
 */
private[spark] class SocketFuncServer(
    authHelper: SocketAuthHelper,
    threadName: String,
    func: Socket => Unit) extends SocketAuthServer[Unit](authHelper, threadName) {

  override def handleConnection(sock: Socket): Unit = {
    func(sock)
  }
}

private[spark] object SocketAuthServer {

  /**
   * Convienince function to create a socket server and run a user function in a background
   * thread to write to an output stream.
   *
   * @param threadName Name for the background serving thread.
   * @param authHelper SocketAuthHelper for authentication
   * @param writeFunc User function to write to a given OutputStream
   * @return
   */
  def serveToStream(
      threadName: String,
      authHelper: SocketAuthHelper)(writeFunc: OutputStream => Unit): Array[Any] = {
    val handleFunc = (sock: Socket) => {
      val out = new BufferedOutputStream(sock.getOutputStream())
      Utils.tryWithSafeFinally {
        writeFunc(out)
      } {
        out.close()
      }
    }

    val server = new SocketFuncServer(authHelper, threadName, handleFunc)
    Array(server.port, server.secret, server)
  }
}
