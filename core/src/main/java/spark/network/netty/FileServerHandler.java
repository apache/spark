package spark.network.netty;

import java.io.File;
import java.io.FileInputStream;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.DefaultFileRegion;


class FileServerHandler extends ChannelInboundMessageHandlerAdapter<String> {

  PathResolver pResolver;

  public FileServerHandler(PathResolver pResolver){
    this.pResolver = pResolver;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, String blockId) {
    String path = pResolver.getAbsolutePath(blockId);
    // if getFilePath returns null, close the channel
    if (path == null) {
      //ctx.close();
      return;
    }
    File file = new File(path);
    if (file.exists()) {
      if (!file.isFile()) {
        //logger.info("Not a file : " + file.getAbsolutePath());
        ctx.write(new FileHeader(0, blockId).buffer());
        ctx.flush();
        return;
      }
      long length = file.length();
      if (length > Integer.MAX_VALUE || length <= 0) {
        //logger.info("too large file : " + file.getAbsolutePath() + " of size "+ length);
        ctx.write(new FileHeader(0, blockId).buffer());
        ctx.flush();
        return;
      }
      int len = new Long(length).intValue();
      //logger.info("Sending block "+blockId+" filelen = "+len);
      //logger.info("header = "+ (new FileHeader(len, blockId)).buffer());
      ctx.write((new FileHeader(len, blockId)).buffer());
      try {
        ctx.sendFile(new DefaultFileRegion(new FileInputStream(file)
          .getChannel(), 0, file.length()));
      } catch (Exception e) {
        //logger.warning("Exception when sending file : " + file.getAbsolutePath());
        e.printStackTrace();
      }
    } else {
      //logger.warning("File not found: " + file.getAbsolutePath());
      ctx.write(new FileHeader(0, blockId).buffer());
    }
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}
