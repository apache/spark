package spark.network.netty;

import java.io.File;
import io.netty.buffer.BufType;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.util.CharsetUtil;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.logging.LogLevel;

public class FileServerChannelInitializer extends
    ChannelInitializer<SocketChannel> {

  PathResolver pResolver;  

  public FileServerChannelInitializer(PathResolver pResolver) {
    this.pResolver = pResolver;
  }

  @Override
  public void initChannel(SocketChannel channel) {
    channel.pipeline()
        .addLast("framer", new DelimiterBasedFrameDecoder(
                8192, Delimiters.lineDelimiter()))
        .addLast("strDecoder", new StringDecoder())
        .addLast("handler", new FileServerHandler(pResolver));
        
  }
}
