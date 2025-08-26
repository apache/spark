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

import java.io.{BufferedOutputStream, File, OutputStream}
import java.net.{InetAddress, InetSocketAddress, StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.channels.{Channels, ServerSocketChannel, SocketChannel}
import java.util.UUID

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Python.PYTHON_AUTH_SOCKET_TIMEOUT
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.{ThreadUtils, Utils}


/**
 * Creates a server in the JVM to communicate with external processes (e.g., Python and R) for
 * handling one batch of data, with authentication and error handling.
 *
 * The socket server can only accept one connection, or close if no connection
 * in configurable amount of seconds (default 15).
 */
private[spark] abstract class SocketAuthServer[T](
    authHelper: SocketAuthHelper,
    threadName: String) extends Logging {

  def this(env: SparkEnv, threadName: String) = this(new SocketAuthHelper(env.conf), threadName)
  def this(threadName: String) = this(SparkEnv.get, threadName)

  private val promise = Promise[T]()
  private val isUnixDomainSock: Boolean = authHelper.isUnixDomainSock

  private def startServer(): (Any, String) = {
    logTrace("Creating listening socket")
    lazy val sockPath = new File(authHelper.sockDir, s".${UUID.randomUUID()}.sock")

    val (serverSocketChannel, address) = if (isUnixDomainSock) {
      val address = UnixDomainSocketAddress.of(sockPath.getPath)
      val serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
      sockPath.deleteOnExit()
      serverChannel.bind(address)
      (serverChannel, address)
    } else {
      val address = InetAddress.getLoopbackAddress()
      val serverChannel = ServerSocketChannel.open()
      serverChannel.bind(new InetSocketAddress(address, 0), 1)
      (serverChannel, address)
    }

    // Close the socket if no connection in the configured seconds
    val timeout = authHelper.conf.get(PYTHON_AUTH_SOCKET_TIMEOUT).toInt
    logTrace(s"Setting timeout to $timeout sec")
    if (!isUnixDomainSock) serverSocketChannel.socket().setSoTimeout(timeout * 1000)

    new Thread(threadName) {
      setDaemon(true)
      override def run(): Unit = {
        var sock: SocketChannel = null
        try {
          if (isUnixDomainSock) {
            logTrace(s"Waiting for connection on $address.")
          } else {
            logTrace(
              s"Waiting for connection on $address with port " +
                s"${serverSocketChannel.socket().getLocalPort}")
          }
          sock = serverSocketChannel.accept()
          logTrace(s"Connection accepted from address ${sock.getRemoteAddress}")
          authHelper.authClient(sock)
          logTrace("Client authenticated")
          promise.complete(Try(handleConnection(sock)))
        } finally {
          logTrace("Closing server")
          JavaUtils.closeQuietly(serverSocketChannel)
          JavaUtils.closeQuietly(sock)
          if (isUnixDomainSock) sockPath.delete()
        }
      }
    }.start()
    if (isUnixDomainSock) {
      (sockPath.getPath, null)
    } else {
      (serverSocketChannel.socket().getLocalPort, authHelper.secret)
    }
  }

  // connInfo is either a string (for UDS) or a port number (for TCP/IP).
  val (connInfo, secret) = startServer()

  /**
   * Handle a connection which has already been authenticated.  Any error from this function
   * will clean up this connection and the entire server, and get propagated to [[getResult]].
   */
  def handleConnection(sock: SocketChannel): T

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
 * Create a socket server class and run user function on the socket in a background thread
 * that can read and write to the socket input/output streams. The function is passed in a
 * socket that has been connected and authenticated.
 */
private[spark] class SocketFuncServer(
    authHelper: SocketAuthHelper,
    threadName: String,
    func: SocketChannel => Unit) extends SocketAuthServer[Unit](authHelper, threadName) {

  override def handleConnection(sock: SocketChannel): Unit = {
    func(sock)
  }
}

private[spark] object SocketAuthServer {

  /**
   * Convenience function to create a socket server and run a user function in a background
   * thread to write to an output stream.
   *
   * The socket server can only accept one connection, or close if no connection
   * in 15 seconds.
   *
   * @param threadName Name for the background serving thread.
   * @param authHelper SocketAuthHelper for authentication
   * @param writeFunc User function to write to a given OutputStream
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def serveToStream(
      threadName: String,
      authHelper: SocketAuthHelper)(writeFunc: OutputStream => Unit): Array[Any] = {
    val handleFunc = (sock: SocketChannel) => {
      val out = new BufferedOutputStream(Channels.newOutputStream(sock))
      Utils.tryWithSafeFinally {
        writeFunc(out)
      } {
        out.close()
      }
    }

    val server = new SocketFuncServer(authHelper, threadName, handleFunc)
    Array(server.connInfo, server.secret, server)
  }
}
