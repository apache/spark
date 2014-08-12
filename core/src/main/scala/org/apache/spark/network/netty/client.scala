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

package org.apache.spark.network.netty

import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import io.netty.channel.socket.SocketChannel
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.oio.OioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.string.StringEncoder
import io.netty.util.CharsetUtil

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Utils


/**
 * Factory for creating [[BlockFetchingClient]] by using createClient. This factory reuses
 * the worker thread pool for Netty.
 *
 * Concurrency: It is possible to have multiple instances of this class, but we should only use
 * a single instance and use that to create multiple [[BlockFetchingClient]]s.
 */
class BlockFetchingClientFactory(conf: SparkConf) {

  /** IO mode: nio, oio, epoll, or auto (try epoll first and then nio). */
  val ioMode = conf.get("spark.shuffle.io.mode", "auto").toLowerCase

  /** Connection timeout in secs. Default 60 secs. */
  val connectionTimeoutMs = conf.getInt("spark.shuffle.io.connectionTimeout", 60) * 1000

  /** A thread factory so the threads are named (for debugging). */
  val threadFactory = Utils.namedThreadFactory("spark-shuffle-client")

  /** The following two are instantiated by the [[init]] method, depending the [[ioMode]]. */
  var socketChannelClass: Class[_ <: Channel] = _
  var workerGroup: EventLoopGroup = _

  init()

  /** Initialize [[socketChannelClass]] and [[workerGroup]] based on the value of [[ioMode]]. */
  private def init(): Unit = {
    def initOio(): Unit = {
      socketChannelClass = classOf[OioSocketChannel]
      workerGroup = new OioEventLoopGroup(0, threadFactory)
    }
    def initNio(): Unit = {
      socketChannelClass = classOf[NioSocketChannel]
      workerGroup = new NioEventLoopGroup(0, threadFactory)
    }
    def initEpoll(): Unit = {
      socketChannelClass = classOf[EpollSocketChannel]
      workerGroup = new EpollEventLoopGroup(0, threadFactory)
    }

    ioMode match {
      case "nio" => initNio()
      case "oio" => initOio()
      case "epoll" => initEpoll()
      case "auto" =>
        // For auto mode, first try epoll (only available on Linux), then nio.
        try {
          initEpoll()
        } catch {
          // TODO: Should we log the throwable? But that always happen on non-Linux systems.
          // Perhaps the right thing to do is to check whether the system is Linux, and then only
          // call initEpoll on Linux.
          case e: Throwable => initNio()
        }
    }
  }

  /**
   * Create a new BlockFetchingClient connecting to the given remote host / port.
   *
   * Concurrency: This method is safe to call from multiple threads.
   */
  def createClient(remoteHost: String, remotePort: Int): BlockFetchingClient = {
    new BlockFetchingClient(this, remoteHost, remotePort)
  }

  def stop(): Unit = {
    if (workerGroup != null) {
      workerGroup.shutdownGracefully()
    }
  }
}


/**
 * Client for fetching remote data blocks from [[BlockServer]]. Use [[BlockFetchingClientFactory]]
 * to instantiate this client.
 *
 * See [[BlockServer]] for the client/server communication protocol.
 *
 * Concurrency: [[BlockFetchingClient]] is not thread safe and should not be shared.
 */
@throws[TimeoutException]
private[spark]
class BlockFetchingClient(factory: BlockFetchingClientFactory, hostname: String, port: Int)
  extends Logging {

  val handler = new BlockFetchingClientHandler

  /** Netty Bootstrap for creating the TCP connection. */
  private val bootstrap: Bootstrap = {
    val b = new Bootstrap
    b.group(factory.workerGroup)
      .channel(factory.socketChannelClass)
      // Use pooled buffers to reduce temporary buffer allocation
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, factory.connectionTimeoutMs)

    b.handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline
          .addLast("encoder", new StringEncoder(CharsetUtil.UTF_8))
          // maxFrameLength = 2G, lengthFieldOffset = 0, lengthFieldLength = 4
          .addLast("framedLengthDecoder", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4))
          .addLast("handler", handler)
      }
    })
    b
  }

  /** Netty ChannelFuture for the connection. */
  private val cf: ChannelFuture = bootstrap.connect(hostname, port)
  if (!cf.awaitUninterruptibly(factory.connectionTimeoutMs)) {
    throw new TimeoutException(
      s"Connecting to $hostname:$port timed out (${factory.connectionTimeoutMs} ms)")
  }

  /**
   * Ask the remote server for a sequence of blocks, and execute the callback.
   *
   * Note that this is asynchronous and returns immediately. Upstream caller should throttle the
   * rate of fetching; otherwise we could run out of memory.
   *
   * @param blockIds sequence of block ids to fetch.
   * @param blockFetchSuccessCallback callback function when a block is successfully fetched.
   *                                  First argument is the block id, and second argument is the
   *                                  raw data in a ByteBuffer.
   * @param blockFetchFailureCallback callback function when we failed to fetch any of the blocks.
   *                                  First argument is the block id, and second argument is the
   *                                  error message.
   */
  def fetchBlocks(
      blockIds: Seq[String],
      blockFetchSuccessCallback: (String, ReferenceCountedBuffer) => Unit,
      blockFetchFailureCallback: (String, String) => Unit): Unit = {
    // It's best to limit the number of "write" calls since it needs to traverse the whole pipeline.
    // It's also best to limit the number of "flush" calls since it requires system calls.
    // Let's concatenate the string and then call writeAndFlush once.
    // This is also why this implementation might be more efficient than multiple, separate
    // fetch block calls.
    var startTime: Long = 0
    logTrace {
      startTime = System.nanoTime
      s"Sending request $blockIds to $hostname:$port"
    }

    // TODO: This is not the most elegant way to handle this ...
    handler.blockFetchSuccessCallback = blockFetchSuccessCallback
    handler.blockFetchFailureCallback = blockFetchFailureCallback

    val writeFuture = cf.channel().writeAndFlush(blockIds.mkString("\n") + "\n")
    writeFuture.addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) {
          logTrace {
            val timeTaken = (System.nanoTime - startTime).toDouble / 1000000
            s"Sending request $blockIds to $hostname:$port took $timeTaken ms"
          }
        } else {
          // Fail all blocks.
          logError(s"Failed to send request $blockIds to $hostname:$port", future.cause)
          blockIds.foreach(blockFetchFailureCallback(_, future.cause.getMessage))
        }
      }
    })
  }

  def waitForClose(): Unit = {
    cf.channel().closeFuture().sync()
  }

  def close(): Unit = {
    // TODO: Should we ever close the client? Probably ...
  }
}


class BlockFetchingClientHandler extends SimpleChannelInboundHandler[ByteBuf] with Logging {

  var blockFetchSuccessCallback: (String, ReferenceCountedBuffer) => Unit = _
  var blockFetchFailureCallback: (String, String) => Unit = _

  override def channelRead0(ctx: ChannelHandlerContext, in: ByteBuf) {
    val totalLen = in.readInt()
    val blockIdLen = in.readInt()
    val blockIdBytes = new Array[Byte](blockIdLen)
    in.readBytes(blockIdBytes)
    val blockId = new String(blockIdBytes)
    val blockLen = math.abs(totalLen) - blockIdLen - 4

    def server = {
      val remoteAddr = ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress]
      remoteAddr.getHostName + ":" + remoteAddr.getPort
    }

    // totalLen is negative when it is an error message.
    if (totalLen < 0) {
      val errorMessageBytes = new Array[Byte](blockLen)
      in.readBytes(errorMessageBytes)
      val errorMsg = new String(errorMessageBytes)
      logTrace(s"Received block $blockId ($blockLen B) with error $errorMsg from $server")
      blockFetchFailureCallback(blockId, errorMsg)
    } else {
      logTrace(s"Received block $blockId ($blockLen B) from $server")
      blockFetchSuccessCallback(blockId, new ReferenceCountedBuffer(in))
    }
  }
}


private[spark]
object BlockFetchingClient {
  def main(args: Array[String]): Unit = {
    val remoteHost = args(0)
    val remotePort = args(1).toInt
    val blocks = args.drop(2)

    val clientFactory = new BlockFetchingClientFactory(new SparkConf)
    val client = clientFactory.createClient(remoteHost, remotePort)

    client.fetchBlocks(
      blocks,
      (blockId, data) => {
        println("got block id " + blockId)
        val bytes = new Array[Byte](data.byteBuffer.remaining())
        data.byteBuffer.get(bytes)
        println("data in string: " + new String(bytes))
      },
      (blockId, errorMessage) => {
        println(s"failed to fetch $blockId, error message: $errorMessage")
      }
    )
    client.waitForClose()
  }
}
