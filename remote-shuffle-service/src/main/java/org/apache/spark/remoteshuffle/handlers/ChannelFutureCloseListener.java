package org.apache.spark.remoteshuffle.handlers;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelFutureCloseListener implements ChannelFutureListener {
  private static final Logger logger = LoggerFactory.getLogger(ChannelFutureCloseListener.class);

  private final String connectionInfo;

  public ChannelFutureCloseListener(String connectionInfo) {
    this.connectionInfo = connectionInfo;
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception {
    logger.info("Closing connection {} {}", connectionInfo, System.nanoTime());
    future.channel().close();
  }
}
