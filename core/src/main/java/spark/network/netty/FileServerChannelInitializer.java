package spark.network.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;


class FileServerChannelInitializer extends ChannelInitializer<SocketChannel> {

  PathResolver pResolver;

  public FileServerChannelInitializer(PathResolver pResolver) {
    this.pResolver = pResolver;
  }

  @Override
  public void initChannel(SocketChannel channel) {
    channel.pipeline()
      .addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()))
      .addLast("strDecoder", new StringDecoder())
      .addLast("handler", new FileServerHandler(pResolver));
  }
}
