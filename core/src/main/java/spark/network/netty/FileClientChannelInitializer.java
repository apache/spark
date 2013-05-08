package spark.network.netty;

import io.netty.buffer.BufType;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringEncoder;


class FileClientChannelInitializer extends ChannelInitializer<SocketChannel> {

  private FileClientHandler fhandler;

  public FileClientChannelInitializer(FileClientHandler handler) {
    fhandler = handler;
  }

  @Override
  public void initChannel(SocketChannel channel) {
    // file no more than 2G
    channel.pipeline()
      .addLast("encoder", new StringEncoder(BufType.BYTE))
      .addLast("handler", fhandler);
  }
}
