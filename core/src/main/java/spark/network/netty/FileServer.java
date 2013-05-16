package spark.network.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.Channel;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioServerSocketChannel;


/**
 * Server that accept the path of a file an echo back its content.
 */
class FileServer {

  private ServerBootstrap bootstrap = null;
  private Channel channel = null;
  private PathResolver pResolver;

  public FileServer(PathResolver pResolver) {
    this.pResolver = pResolver;
  }

  public void run(int port) {
    // Configure the server.
    bootstrap = new ServerBootstrap();
    try {
      bootstrap.group(new OioEventLoopGroup(), new OioEventLoopGroup())
        .channel(OioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 100)
        .option(ChannelOption.SO_RCVBUF, 1500)
        .childHandler(new FileServerChannelInitializer(pResolver));
      // Start the server.
      channel = bootstrap.bind(port).sync().channel();
      channel.closeFuture().sync();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally{
      bootstrap.shutdown();
    }
  }

  public void stop() {
    if (channel!=null) {
      channel.close();
    }
    if (bootstrap != null) {
      bootstrap.shutdown();
    }
  }
}
