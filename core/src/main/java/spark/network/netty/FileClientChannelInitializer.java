package spark.network.netty;

import io.netty.buffer.BufType;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.logging.LogLevel;

public class FileClientChannelInitializer extends
    ChannelInitializer<SocketChannel> {

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
