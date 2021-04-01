/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.handlers;

import org.apache.spark.remoteshuffle.common.ServerDetailCollection;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.apache.spark.remoteshuffle.messages.ConnectRegistryRequest;
import org.apache.spark.remoteshuffle.messages.GetServersRequestMessage;
import org.apache.spark.remoteshuffle.messages.RegisterServerRequestMessage;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryChannelInboundHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger =
      LoggerFactory.getLogger(RegistryChannelInboundHandler.class);

  private String connectionInfo = "";

  private final RegistryServerHandler serverHandler;

  public RegistryChannelInboundHandler(ServerDetailCollection serverCollection, String serverId) {
    serverHandler = new RegistryServerHandler(serverCollection, serverId);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);

    processChannelActive(ctx);
  }

  public void processChannelActive(final ChannelHandlerContext ctx) {
    connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    try {
      if (logger.isTraceEnabled()) {
        logger.trace("Got incoming message: " + msg + ", " + connectionInfo);
      }

      if (msg instanceof ConnectRegistryRequest) {
        serverHandler.handleMessage(ctx, (ConnectRegistryRequest) msg);
      } else if (msg instanceof RegisterServerRequestMessage) {
        serverHandler.handleMessage(ctx, (RegisterServerRequestMessage) msg);
      } else if (msg instanceof GetServersRequestMessage) {
        serverHandler.handleMessage(ctx, (GetServersRequestMessage) msg);
      } else {
        throw new RssInvalidDataException(
            String.format("Unsupported message: %s, %s", msg, connectionInfo));
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    M3Stats.addException(cause, this.getClass().getSimpleName());
    String msg = "Got exception " + connectionInfo;
    logger.warn(msg, cause);
    ctx.close();
  }

}
