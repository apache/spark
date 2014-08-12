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

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.nio.channels.FileChannel

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.oio.OioServerSocketChannel
import io.netty.handler.codec.{MessageToByteEncoder, LineBasedFrameDecoder}
import io.netty.handler.codec.string.StringDecoder
import io.netty.util.CharsetUtil

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.{TestBlockId, FileSegment, BlockId}
import org.apache.spark.util.Utils


// TODO: Remove dependency on BlockId. This layer should not be coupled with storage.

// TODO: PathResolver is not general enough. It only works for on-disk blocks.

// TODO: Allow user-configured port

/** A simple main function for testing the server. */
object BlockServer {
  def main(args: Array[String]): Unit = {
    new BlockServer(new SparkConf, new PathResolver {
      override def getBlockLocation(blockId: BlockId): FileSegment = {
        val file = new java.io.File(blockId.asInstanceOf[TestBlockId].id)
        new FileSegment(file, 0, file.length())
      }
    })
    Thread.sleep(1000000)
  }
}


/**
 * Server for serving Spark data blocks. This should be used together with [[BlockFetchingClient]].
 *
 * Protocol for requesting blocks (client to server):
 *   One block id per line, e.g. to request 3 blocks: "block1\nblock2\nblock3\n"
 *
 * Protocol for sending blocks (server to client):
 *   frame-length (4 bytes), block-id-length (4 bytes), block-id, block-data.
 *
 *   frame-length should not include the length of itself.
 *   If frame-length is negative, then this is an error message rather than block-data. The real
 *   length is the absolute value of the frame-length.
 *
 */
private[spark]
class BlockServer(conf: NettyConfig, pResolver: PathResolver) extends Logging {

  def this(sparkConf: SparkConf, pResolver: PathResolver) = {
    this(new NettyConfig(sparkConf), pResolver)
  }

  def port: Int = _port

  private var _port: Int = 0
  private var bootstrap: ServerBootstrap = _
  private var channelFuture: ChannelFuture = _

  init()

  /** Initialize the server. */
  private def init(): Unit = {
    bootstrap = new ServerBootstrap
    val bossThreadFactory = Utils.namedThreadFactory("spark-shuffle-server-boss")
    val workerThreadFactory = Utils.namedThreadFactory("spark-shuffle-server-worker")

    // Use only one thread to accept connections, and 2 * num_cores for worker.
    def initNio(): Unit = {
      val bossGroup = new NioEventLoopGroup(1, bossThreadFactory)
      val workerGroup = new NioEventLoopGroup(0, workerThreadFactory)
      workerGroup.setIoRatio(conf.ioRatio)
      bootstrap.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
    }
    def initOio(): Unit = {
      val bossGroup = new OioEventLoopGroup(1, bossThreadFactory)
      val workerGroup = new OioEventLoopGroup(0, workerThreadFactory)
      bootstrap.group(bossGroup, workerGroup).channel(classOf[OioServerSocketChannel])
    }
    def initEpoll(): Unit = {
      val bossGroup = new EpollEventLoopGroup(1, bossThreadFactory)
      val workerGroup = new EpollEventLoopGroup(0, workerThreadFactory)
      workerGroup.setIoRatio(conf.ioRatio)
      bootstrap.group(bossGroup, workerGroup).channel(classOf[EpollServerSocketChannel])
    }

    conf.ioMode match {
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

    // Use pooled buffers to reduce temporary buffer allocation
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
    bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

    // Various (advanced) user-configured settings.
    conf.backLog.foreach { backLog =>
      bootstrap.option[java.lang.Integer](ChannelOption.SO_BACKLOG, backLog)
    }
    conf.receiveBuf.foreach { receiveBuf =>
      bootstrap.option[java.lang.Integer](ChannelOption.SO_RCVBUF, receiveBuf)
    }
    conf.sendBuf.foreach { sendBuf =>
      bootstrap.option[java.lang.Integer](ChannelOption.SO_SNDBUF, sendBuf)
    }

    bootstrap.childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline
          .addLast("frameDecoder", new LineBasedFrameDecoder(1024))  // max block id length 1024
          .addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
          .addLast("blockHeaderEncoder", new BlockHeaderEncoder)
          .addLast("handler", new BlockServerHandler(pResolver))
      }
    })

    channelFuture = bootstrap.bind(new InetSocketAddress(_port))
    channelFuture.sync()

    val addr = channelFuture.channel.localAddress.asInstanceOf[InetSocketAddress]
    _port = addr.getPort
  }

  /** Shutdown the server. */
  def stop(): Unit = {
    if (channelFuture != null) {
      channelFuture.channel().close().awaitUninterruptibly()
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully()
    }
    bootstrap = null
  }
}


/** A handler that writes the content of a block to the channel. */
class BlockServerHandler(p: PathResolver)
  extends SimpleChannelInboundHandler[String] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, blockId: String): Unit = {
    // client in the form of hostname:port
    val client = {
      val remoteAddr = ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress]
      remoteAddr.getHostName + ":" + remoteAddr.getPort
    }

    // A helper function to send error message back to the client.
    def respondWithError(error: String): Unit = {
      ctx.writeAndFlush(new BlockHeader(-1, blockId, Some(error))).addListener(
        new ChannelFutureListener {
          override def operationComplete(future: ChannelFuture) {
            if (!future.isSuccess) {
              // TODO: Maybe log the success case as well.
              logError(s"Error sending error back to $client", future.cause)
              ctx.close()
            }
          }
        }
      )
    }

    logTrace(s"Received request from $client to fetch block $blockId")

    var fileChannel: FileChannel = null
    var offset: Long = 0
    var blockSize: Long = 0

    // First make sure we can find the block. If not, send error back to the user.
    try {
      val segment = p.getBlockLocation(BlockId(blockId))
      fileChannel = new FileInputStream(segment.file).getChannel
      offset = segment.offset
      blockSize = segment.length
    } catch {
      case e: Exception =>
        logError(s"Error opening block $blockId for request from $client", e)
        blockSize = -1
        respondWithError(e.getMessage)
    }

    // Send error message back if the block is too large. Even though we are capable of sending
    // large (2G+) blocks, the receiving end cannot handle it so let's fail fast.
    // Once we fixed the receiving end to be able to process large blocks, this should be removed.
    // Also make sure we update BlockHeaderEncoder to support length > 2G.
    if (blockSize > Int.MaxValue) {
      respondWithError(s"Block $blockId size ($blockSize) greater than 2G")
    }

    // Found the block. Send it back.
    if (fileChannel != null && blockSize >= 0) {
      val listener = new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            logTrace(s"Sent block $blockId ($blockSize B) back to $client")
          } else {
            logError(s"Error sending block $blockId to $client; closing connection", future.cause)
            ctx.close()
          }
        }
      }
      val region = new DefaultFileRegion(fileChannel, offset, blockSize)
      ctx.writeAndFlush(new BlockHeader(blockSize.toInt, blockId)).addListener(listener)
      ctx.writeAndFlush(region).addListener(listener)
    }
  }  // end of channelRead0
}


/**
 * Header describing a block. This is used only in the server pipeline.
 *
 * [[BlockServerHandler]] creates this, and [[BlockHeaderEncoder]] encodes it.
 *
 * @param blockSize length of the block content, excluding the length itself.
 *                 If positive, this is the header for a block (not part of the header).
 *                 If negative, this is the header and content for an error message.
 * @param blockId block id
 * @param error some error message from reading the block
 */
class BlockHeader(val blockSize: Int, val blockId: String, val error: Option[String] = None)


/**
 * A simple encoder for BlockHeader. See [[BlockServer]] for the server to client protocol.
 */
class BlockHeaderEncoder extends MessageToByteEncoder[BlockHeader] {
  override def encode(ctx: ChannelHandlerContext, msg: BlockHeader, out: ByteBuf): Unit = {
    // message = message length (4 bytes) + block id length (4 bytes) + block id + block data
    // message length = block id length (4 bytes) + size of block id + size of block data
    val blockId = msg.blockId.getBytes
    msg.error match {
      case Some(errorMsg) =>
        val errorBytes = errorMsg.getBytes
        out.writeInt(-(4 + blockId.length + errorBytes.size))
        out.writeInt(blockId.length)
        out.writeBytes(blockId)
        out.writeBytes(errorBytes)
      case None =>
        val blockId = msg.blockId.getBytes
        out.writeInt(4 + blockId.length + msg.blockSize)
        out.writeInt(blockId.length)
        out.writeBytes(blockId)
    }
  }
}
