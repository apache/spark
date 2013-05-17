package spark.network.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioSocketChannel;


class FileClient {

  private FileClientHandler handler = null;
  private Channel channel = null;
  private Bootstrap bootstrap = null;

  public FileClient(FileClientHandler handler) {
    this.handler = handler;
  }

  public void init() {
    bootstrap = new Bootstrap();
    bootstrap.group(new OioEventLoopGroup())
      .channel(OioSocketChannel.class)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.TCP_NODELAY, true)
      .handler(new FileClientChannelInitializer(handler));
  }

  public static final class ChannelCloseListener implements ChannelFutureListener {
    private FileClient fc = null;

    public ChannelCloseListener(FileClient fc){
      this.fc = fc;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (fc.bootstrap!=null){
        fc.bootstrap.shutdown();
        fc.bootstrap = null;
      }
    }
  }

  public void connect(String host, int port) {
    try {
      // Start the connection attempt.
      channel = bootstrap.connect(host, port).sync().channel();
      // ChannelFuture cf = channel.closeFuture();
      //cf.addListener(new ChannelCloseListener(this));
    } catch (InterruptedException e) {
      close();
    }
  }

  public void waitForClose() {
    try {
      channel.closeFuture().sync();
    } catch (InterruptedException e){
      e.printStackTrace();
    }
  }

  public void sendRequest(String file) {
    //assert(file == null);
    //assert(channel == null);
    channel.write(file + "\r\n");
  }

  public void close() {
    if(channel != null) {
      channel.close();
      channel = null;
    }
    if ( bootstrap!=null) {
      bootstrap.shutdown();
      bootstrap = null;
    }
  }
}
