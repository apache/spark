package edu.berkeley.cs.amplab.sparkr

import java.io.{File, FileOutputStream, DataOutputStream, IOException}
import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.bytes.ByteArrayDecoder
import io.netty.handler.codec.bytes.ByteArrayEncoder
import io.netty.handler.codec.LengthFieldBasedFrameDecoder

/**
 * Netty-based backend server that is used to communicate between R and Java.
 */
class SparkRBackend {

  var channelFuture: ChannelFuture = null  
  var bootstrap: ServerBootstrap = null
  var bossGroup: EventLoopGroup = null

  def init(): Int = {
    bossGroup = new NioEventLoopGroup(SparkRConf.numServerThreads)
    val workerGroup = bossGroup
    val handler = new SparkRBackendHandler(this)
  
    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
  
    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch: SocketChannel) = {
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
          .addLast("handler", handler)
      }
    })

    channelFuture = bootstrap.bind(new InetSocketAddress(0))
    channelFuture.syncUninterruptibly()
    channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort()
  }

  def run() = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  def close() = {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
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

object SparkRBackend {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkRBackend <tempFilePath>")
      System.exit(-1)
    }
    val sparkRBackend = new SparkRBackend()
    try {
      // bind to random port
      val boundPort = sparkRBackend.init()
      // tell the R process via temporary file
      val path = args(0)
      val f = new File(path + ".tmp")
      val dos = new DataOutputStream(new FileOutputStream(f))
      dos.writeInt(boundPort)
      dos.close()
      f.renameTo(new File(path))
      sparkRBackend.run()
    } catch {
      case e: IOException =>
        System.err.println("Server shutting down: failed with exception ", e)
        sparkRBackend.close()
        System.exit(1)
    }
    System.exit(0)
  }
}
