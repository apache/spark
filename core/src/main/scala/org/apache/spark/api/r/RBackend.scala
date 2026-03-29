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

package org.apache.spark.api.r

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer, EventLoopGroup}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}
import io.netty.handler.timeout.ReadTimeoutHandler

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.R._
import org.apache.spark.network.util.{IOMode, NettyUtils}

/**
 * Netty-based backend server that is used to communicate between R and Java.
 */
private[spark] class RBackend {

  private[this] var channelFuture: ChannelFuture = null
  private[this] var bootstrap: ServerBootstrap = null
  private[this] var bossGroup: EventLoopGroup = null

  /** Tracks JVM objects returned to R for this RBackend instance. */
  private[r] val jvmObjectTracker = new JVMObjectTracker

  def init(): (Int, RAuthHelper) = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    val backendConnectionTimeout = conf.get(R_BACKEND_CONNECTION_TIMEOUT)
    bossGroup = NettyUtils.createEventLoop(IOMode.NIO, conf.get(R_NUM_BACKEND_THREADS), "RBackend")
    val workerGroup = bossGroup
    val handler = new RBackendHandler(this)
    val authHelper = new RAuthHelper(conf)
    val channelClass = NettyUtils.getServerChannelClass(IOMode.NIO)

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(channelClass)

    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline()
          .addLast("encoder", new ByteArrayEncoder())
          .addLast("frameDecoder",
            // maxFrameLength = 2G
            // lengthFieldOffset = 0
            // lengthFieldLength = 4
            // lengthAdjustment = 0
            // initialBytesToStrip = 4, i.e. strip out the length field itself
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
          .addLast("decoder", new ByteArrayDecoder())
          .addLast("readTimeoutHandler", new ReadTimeoutHandler(backendConnectionTimeout))
          .addLast(new RBackendAuthHandler(authHelper.secret))
          .addLast("handler", handler)
      }
    })

    channelFuture = bootstrap.bind(new InetSocketAddress("localhost", 0))
    channelFuture.syncUninterruptibly()

    val port = channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort()
    (port, authHelper)
  }

  def run(): Unit = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  def close(): Unit = {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.config().group() != null) {
      bootstrap.config().group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.config().childGroup() != null) {
      bootstrap.config().childGroup().shutdownGracefully()
    }
    bootstrap = null
    jvmObjectTracker.clear()
  }

}

private[spark] object RBackend extends Logging {
  initializeLogIfNecessary(true)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      // scalastyle:off println
      System.err.println("Usage: RBackend <tempFilePath>")
      // scalastyle:on println
      System.exit(-1)
    }

    val sparkRBackend = new RBackend()
    try {
      // bind to random port
      val (boundPort, authHelper) = sparkRBackend.init()
      val serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
      val listenPort = serverSocket.getLocalPort()
      // Connection timeout is set by socket client. To make it configurable we will pass the
      // timeout value to client inside the temp file
      val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
      val backendConnectionTimeout = conf.get(R_BACKEND_CONNECTION_TIMEOUT)

      // tell the R process via temporary file
      val path = args(0)
      val f = new File(path + ".tmp")
      val dos = new DataOutputStream(new FileOutputStream(f))
      dos.writeInt(boundPort)
      dos.writeInt(listenPort)
      SerDe.writeString(dos, RUtils.rPackages.getOrElse(""))
      dos.writeInt(backendConnectionTimeout)
      SerDe.writeString(dos, authHelper.secret)
      dos.close()
      f.renameTo(new File(path))

      // wait for the end of stdin, then exit
      new Thread("wait for socket to close") {
        setDaemon(true)
        override def run(): Unit = {
          // any uncaught exception will also shutdown JVM
          val buf = new Array[Byte](1024)
          // shutdown JVM if R does not connect back in 10 seconds
          serverSocket.setSoTimeout(10000)

          // Wait for the R process to connect back, ignoring any failed auth attempts. Allow
          // a max number of connection attempts to avoid looping forever.
          try {
            var remainingAttempts = 10
            var inSocket: Socket = null
            while (inSocket == null) {
              inSocket = serverSocket.accept()
              try {
                authHelper.authClient(inSocket)
              } catch {
                case e: Exception =>
                  remainingAttempts -= 1
                  if (remainingAttempts == 0) {
                    val msg = "Too many failed authentication attempts."
                    logError(msg)
                    throw new IllegalStateException(msg)
                  }
                  logInfo("Client connection failed authentication.")
                  inSocket = null
              }
            }

            serverSocket.close()

            // wait for the end of socket, closed if R process die
            inSocket.getInputStream().read(buf)
          } finally {
            serverSocket.close()
            sparkRBackend.close()
            System.exit(0)
          }
        }
      }.start()

      sparkRBackend.run()
    } catch {
      case e: IOException =>
        logError("Server shutting down: failed with exception ", e)
        sparkRBackend.close()
        System.exit(1)
    }
    System.exit(0)
  }

}
