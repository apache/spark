/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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

package org.apache.spark.remoteshuffle.decoders;

import org.apache.spark.remoteshuffle.common.ServerDetailCollection;
import org.apache.spark.remoteshuffle.execution.ShuffleExecutor;
import org.apache.spark.remoteshuffle.handlers.*;
import org.apache.spark.remoteshuffle.messages.MessageConstants;
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/***
 * The sole purpose of this class is to replace itself with the appropriate decoder(s) for the requested protocol version
 */
public class StreamServerVersionDecoder extends ByteToMessageDecoder {
  private static final Logger logger = LoggerFactory.getLogger(StreamServerVersionDecoder.class);

  private final String serverId;
  private final long idleTimeoutMillis;
  private final ShuffleExecutor executor;
  private final UploadChannelManager channelManager;

  // this is used when the shuffle server could serve as a registry server
  private final ServerDetailCollection serverDetailCollection;

  public StreamServerVersionDecoder(String serverId,
                                    long idleTimeoutMillis,
                                    ShuffleExecutor executor,
                                    UploadChannelManager channelManager,
                                    ServerDetailCollection serverDetailCollection) {
    this.serverId = serverId;
    this.idleTimeoutMillis = idleTimeoutMillis;
    this.executor = executor;
    this.channelManager = channelManager;
    this.serverDetailCollection = serverDetailCollection;
  }

  private void addVersionDecoder(ChannelHandlerContext ctx, byte type, byte version) {
    ByteToMessageDecoder newDecoder;
    String decoderName = "decoder";
    ChannelInboundHandlerAdapter newHandler;
    String handlerName = "handler";

    if (type == MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE &&
        version == MessageConstants.UPLOAD_UPLINK_VERSION_3) {
      ByteBuf shuffleDataBuffer =
          ctx.alloc().buffer(MessageConstants.DEFAULT_SHUFFLE_DATA_MESSAGE_SIZE);
      newDecoder = new StreamServerMessageDecoder(shuffleDataBuffer);
      UploadChannelInboundHandler channelInboundHandler = new UploadChannelInboundHandler(
          serverId, idleTimeoutMillis, executor, channelManager);
      channelInboundHandler.processChannelActive(ctx);
      newHandler = channelInboundHandler;
    } else if (type == MessageConstants.DOWNLOAD_UPLINK_MAGIC_BYTE &&
        version == MessageConstants.DOWNLOAD_UPLINK_VERSION_3) {
      newDecoder = new StreamServerMessageDecoder(null);
      DownloadChannelInboundHandler channelInboundHandler = new DownloadChannelInboundHandler(
          serverId, idleTimeoutMillis, executor);
      channelInboundHandler.processChannelActive(ctx);
      newHandler = channelInboundHandler;
    } else if (type == MessageConstants.NOTIFY_UPLINK_MAGIC_BYTE &&
        version == MessageConstants.NOTIFY_UPLINK_VERSION_3) {
      newDecoder = new StreamServerMessageDecoder(null);
      NotifyChannelInboundHandler channelInboundHandler =
          new NotifyChannelInboundHandler(serverId);
      channelInboundHandler.processChannelActive(ctx);
      newHandler = channelInboundHandler;
    } else if (type == MessageConstants.REGISTRY_UPLINK_MAGIC_BYTE &&
        version == MessageConstants.REGISTRY_UPLINK_VERSION_3) {
      newDecoder = new StreamServerMessageDecoder(null);
      RegistryChannelInboundHandler channelInboundHandler = new RegistryChannelInboundHandler(
          serverDetailCollection, serverId);
      channelInboundHandler.processChannelActive(ctx);
      newHandler = channelInboundHandler;
    } else {
      String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
      logger.error(String.format(
          "Invalid upload version %d for link type %s from client %s",
          version, type, clientInfo));
      ctx.close();
      logger.info(String.format("Closed connection to client %s", clientInfo));
      return;
    }
    logger.debug(String.format("Using version %d protocol for client %s",
        version, NettyUtils.getServerConnectionInfo(ctx)));
    ctx.pipeline().replace(this, decoderName, newDecoder);
    ctx.pipeline().addAfter(decoderName, handlerName, newHandler);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx,
                        ByteBuf in,
                        List<Object> out) throws Exception {
    if (in.readableBytes() < 2 * Byte.BYTES) {
      return;
    }
    in.markReaderIndex();
    byte magicByte = in.readByte();
    byte version = in.readByte();
    in.resetReaderIndex();  // rewind so that the newly added decoder can re-read it

    switch (magicByte) {
      case MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE:
      case MessageConstants.DOWNLOAD_UPLINK_MAGIC_BYTE:
      case MessageConstants.NOTIFY_UPLINK_MAGIC_BYTE:
      case MessageConstants.REGISTRY_UPLINK_MAGIC_BYTE:
        addVersionDecoder(ctx, magicByte, version);
        break;
      default:
        String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
        logger.warn(String.format(
            "Invalid magic byte %d from client %s",
            magicByte, clientInfo));
        ctx.close();
        logger.info(String.format("Closed connection to client %s", clientInfo));
        break;
    }
  }

}