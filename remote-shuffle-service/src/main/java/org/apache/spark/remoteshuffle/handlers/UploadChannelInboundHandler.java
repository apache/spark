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

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.apache.spark.remoteshuffle.exceptions.RssMaxConnectionsException;
import org.apache.spark.remoteshuffle.exceptions.RssTooMuchDataException;
import org.apache.spark.remoteshuffle.execution.ShuffleExecutor;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.util.ExceptionUtils;
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.spark.remoteshuffle.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class UploadChannelInboundHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(UploadChannelInboundHandler.class);

  private static Counter numChannelActive =
      M3Stats.getDefaultScope().counter("numUploadChannelActive");
  private static Counter numChannelInactive =
      M3Stats.getDefaultScope().counter("numUploadChannelInactive");

  private static AtomicInteger concurrentChannelsAtomicInteger = new AtomicInteger();
  private static Gauge numConcurrentChannels =
      M3Stats.getDefaultScope().gauge("numConcurrentUploadChannels");
  private static Gauge finishUploadRequestLag =
      M3Stats.getDefaultScope().gauge("finishUploadRequestLag");

  private static Counter closedIdleUploadChannels =
      M3Stats.getDefaultScope().counter("closedIdleUploadChannels");

  private final String serverId;

  private final long idleTimeoutMillis;

  private final UploadServerHandler uploadServerHandler;

  private String connectionInfo = "";
  private String appId = null;
  private String appAttempt = null;

  private StartUploadMessage startUploadMessage = null;

  private ChannelIdleCheck idleCheck;

  final int CONCURRENT_CONNS = 1;

  public UploadChannelInboundHandler(String serverId,
                                     long idleTimeoutMillis,
                                     ShuffleExecutor executor,
                                     UploadChannelManager channelManager) {
    this.serverId = serverId;
    this.uploadServerHandler = new UploadServerHandler(executor, channelManager);
    this.idleTimeoutMillis = idleTimeoutMillis;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);

    processChannelActive(ctx);
  }

  public void processChannelActive(final ChannelHandlerContext ctx) {
    logger.debug("Channel active: {}", connectionInfo);
    numChannelActive.inc(1);
    numConcurrentChannels.update(concurrentChannelsAtomicInteger.incrementAndGet());
    connectionInfo = NettyUtils.getServerConnectionInfo(ctx);

    idleCheck = new ChannelIdleCheck(ctx, idleTimeoutMillis, closedIdleUploadChannels);
    idleCheck.schedule();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);

    logger.debug("Channel inactive: {}", connectionInfo);
    numChannelInactive.inc(1);
    numConcurrentChannels.update(concurrentChannelsAtomicInteger.decrementAndGet());
    uploadServerHandler.onChannelInactive();

    if (idleCheck != null) {
      idleCheck.cancel();
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    try {
      if (logger.isDebugEnabled() && !(msg instanceof ShuffleDataWrapper)) {
        logger.debug("Got incoming message: {}, {}", msg, connectionInfo);
      }

      if (idleCheck != null) {
        idleCheck.updateLastReadTime();
      }

      // Process other messages. We assume the header messages are already processed, thus some fields of this
      // class are already populated with proper values, e.g. user field.

      if (msg instanceof ConnectUploadRequest) {
        try {
          uploadServerHandler.checkMaxConnections();
        } catch (RssMaxConnectionsException e) {
          logger.info(
              "Cannot handle new connection due to server capacity. Closing current connection: {}. {}",
              connectionInfo, ExceptionUtils.getSimpleMessage(e));
          M3Stats.addException(e, this.getClass().getSimpleName());
          ByteBuf buf = ctx.alloc().buffer(1);
          buf.writeByte(MessageConstants.RESPONSE_STATUS_SERVER_BUSY);
          ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
          return;
        }

        ConnectUploadRequest connectUploadRequest = (ConnectUploadRequest) msg;
        appId = connectUploadRequest.getAppId();
        appAttempt = connectUploadRequest.getAppAttempt();

        try {
          uploadServerHandler.checkAppMaxWriteBytes(appId);
        } catch (RssTooMuchDataException e) {
          logger.info(
              "Cannot handle new connection due to writing too much data from app (%s). " +
                  "Closing current connection: {}. {}", appId, connectionInfo,
              ExceptionUtils.getSimpleMessage(e));
          M3Stats.addException(e, this.getClass().getSimpleName());
          ByteBuf buf = ctx.alloc().buffer(1);
          buf.writeByte(MessageConstants.RESPONSE_STATUS_APP_TOO_MUCH_DATA);
          ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
        }

        uploadServerHandler.updateLiveness(appId);

        ConnectUploadResponse connectUploadResponse = new ConnectUploadResponse(
            serverId);
        HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK,
            connectUploadResponse, true);
      } else if (msg instanceof StartUploadMessage) {
        startUploadMessage = (StartUploadMessage) msg;

        AppShuffleId appShuffleId = new AppShuffleId(appId,
            appAttempt,
            startUploadMessage.getShuffleId());
        long taskAttemptId = startUploadMessage.getAttemptId();

        ShuffleWriteConfig writeConfig = new ShuffleWriteConfig(startUploadMessage.getNumSplits());
        uploadServerHandler.initializeAppTaskAttempt(appShuffleId, taskAttemptId,
            startUploadMessage.getNumPartitions(),
            writeConfig, ctx);
      } else if (msg instanceof FinishUploadMessage) {
        logger.debug("FinishUploadMessage, {}, {}", msg, connectionInfo);
        FinishUploadMessage finishUploadMessage = (FinishUploadMessage) msg;
        finishUploadRequestLag
            .update(System.currentTimeMillis() - finishUploadMessage.getTimestamp());
        byte ackFlag = finishUploadMessage.getAckFlag();
        uploadServerHandler.finishUpload(finishUploadMessage.getTaskAttemptId());
        if (ackFlag != FinishUploadMessage.ACK_FLAG_NO_ACK) {
          ByteBuf buf = ctx.alloc().buffer(1);
          buf.writeByte(MessageConstants.RESPONSE_STATUS_OK);
          ctx.writeAndFlush(buf);
        }
      } else if (msg instanceof ShuffleDataWrapper) {
        ShuffleDataWrapper shuffleDataWrapper = (ShuffleDataWrapper) msg;
        uploadServerHandler.writeRecord(shuffleDataWrapper);
      } else if (msg instanceof HeartbeatMessage) {
        HeartbeatMessage heartbeatMessage = (HeartbeatMessage) msg;
        String heartbeatAppId = heartbeatMessage.getAppId();
        boolean heartbeatKeepLive = heartbeatMessage.isKeepLive();
        uploadServerHandler.updateLiveness(heartbeatAppId);
        if (!heartbeatKeepLive) {
          ctx.close();
        }
      } else if (msg instanceof GetBusyStatusRequest) {
        GetBusyStatusRequest getBusyStatusRequest = (GetBusyStatusRequest) msg;
        // TODO ideally clients should send some information to tell server what status they are interested
        Map<Long, Long> metricsMap = new HashMap<>();
        GetBusyStatusResponse getBusyStatusResponse =
            new GetBusyStatusResponse(metricsMap, new HashMap<>());
        getBusyStatusResponse.getMetrics().put(new Long(CONCURRENT_CONNS),
            new Long(concurrentChannelsAtomicInteger.get()));
        ChannelFuture channelFuture =
            HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK,
                getBusyStatusResponse, true);
        channelFuture.addListener(ChannelFutureListener.CLOSE);
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
