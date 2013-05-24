package spark.network.netty;

import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Server that accept the path of a file an echo back its content.
 */
class FileServer {

  private Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

  private ServerBootstrap bootstrap = null;
  private ChannelFuture channelFuture = null;
  private int port = 0;
  private Thread blockingThread = null;

  public FileServer(PathResolver pResolver, int port) {
    InetSocketAddress addr = new InetSocketAddress(port);

    // Configure the server.
    bootstrap = new ServerBootstrap();
    bootstrap.group(new OioEventLoopGroup(), new OioEventLoopGroup())
        .channel(OioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 100)
        .option(ChannelOption.SO_RCVBUF, 1500)
        .childHandler(new FileServerChannelInitializer(pResolver));
    // Start the server.
    channelFuture = bootstrap.bind(addr);
    this.port = addr.getPort();
  }

  /**
   * Start the file server asynchronously in a new thread.
   */
  public void start() {
    try {
      blockingThread = new Thread() {
        public void run() {
          try {
            Channel channel = channelFuture.sync().channel();
            channel.closeFuture().sync();
          } catch (InterruptedException e) {
            LOG.error("File server start got interrupted", e);
          }
        }
      };
      blockingThread.setDaemon(true);
      blockingThread.start();
    } finally {
      bootstrap.shutdown();
    }
  }

  public int getPort() {
    return port;
  }

  public void stop() {
    if (blockingThread != null) {
      blockingThread.stop();
      blockingThread = null;
    }
    if (channelFuture != null) {
      channelFuture.channel().closeFuture();
      channelFuture = null;
    }
    if (bootstrap != null) {
      bootstrap.shutdown();
      bootstrap = null;
    }
  }
}
