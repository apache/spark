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

package org.apache.spark.remoteshuffle.handlers;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.FilePathAndLength;
import org.apache.spark.remoteshuffle.common.MapTaskCommitStatus;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.apache.spark.remoteshuffle.execution.ShuffleExecutor;
import org.apache.spark.remoteshuffle.messages.*;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DownloadChannelInboundHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger =
      LoggerFactory.getLogger(DownloadChannelInboundHandler.class);

  private static Counter numChannelActive =
      M3Stats.getDefaultScope().counter("numDownloadChannelActive");
  private static Counter numChannelInactive =
      M3Stats.getDefaultScope().counter("numDownloadChannelInactive");

  private static AtomicInteger concurrentChannelsAtomicInteger = new AtomicInteger();
  private static Gauge numConcurrentChannels =
      M3Stats.getDefaultScope().gauge("numConcurrentDownloadChannels");

  private static Counter closedIdleDownloadChannels =
      M3Stats.getDefaultScope().counter("closedIdleDownloadChannels");

  private final String serverId;

  private final long idleTimeoutMillis;

  private final DownloadServerHandler downloadServerHandler;

  private String connectionInfo = "";
  private AppShufflePartitionId appShufflePartitionId = null;
  private List<Long> fetchTaskAttemptIds = new ArrayList<>();

  private ChannelIdleCheck idleCheck;

  public DownloadChannelInboundHandler(String serverId,
                                       long idleTimeoutMillis,
                                       ShuffleExecutor executor) {
    this.serverId = serverId;
    this.idleTimeoutMillis = idleTimeoutMillis;
    this.downloadServerHandler = new DownloadServerHandler(executor);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);

    processChannelActive(ctx);
  }

  public void processChannelActive(final ChannelHandlerContext ctx) {
    numChannelActive.inc(1);
    numConcurrentChannels.update(concurrentChannelsAtomicInteger.incrementAndGet());
    connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
    logger.debug("Channel active: {}", connectionInfo);

    idleCheck = new ChannelIdleCheck(ctx, idleTimeoutMillis, closedIdleDownloadChannels);
    idleCheck.schedule();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);

    numChannelInactive.inc(1);
    numConcurrentChannels.update(concurrentChannelsAtomicInteger.decrementAndGet());
    logger.debug("Channel inactive: {}", connectionInfo);

    if (idleCheck != null) {
      idleCheck.cancel();
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    try {
      logger.debug("Got incoming message: {}, {}", msg, connectionInfo);

      if (idleCheck != null) {
        idleCheck.updateLastReadTime();
      }

      // Process other messages. We assume the header messages are already processed, thus some fields of this
      // class are already populated with proper values, e.g. user field.

      if (msg instanceof ConnectDownloadRequest) {
        logger.info("ConnectDownloadRequest: {}, {}", msg, connectionInfo);

        ConnectDownloadRequest connectRequest = (ConnectDownloadRequest) msg;
        appShufflePartitionId = new AppShufflePartitionId(
            connectRequest.getAppId(),
            connectRequest.getAppAttempt(),
            connectRequest.getShuffleId(),
            connectRequest.getPartitionId()
        );
        fetchTaskAttemptIds = connectRequest.getTaskAttemptIds();

        ShuffleStageStatus shuffleStageStatus =
            downloadServerHandler.getShuffleStageStatus(appShufflePartitionId.getAppShuffleId());
        if (shuffleStageStatus.getFileStatus() ==
            ShuffleStageStatus.FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED) {
          logger.warn(String.format("Shuffle stage not started for %s, %s",
              appShufflePartitionId.getAppShuffleId(), connectionInfo));
          HandlerUtil.writeResponseStatus(ctx,
              MessageConstants.RESPONSE_STATUS_SHUFFLE_STAGE_NOT_STARTED);
          return;
        }

        downloadServerHandler.initialize(connectRequest);

        MapTaskCommitStatus mapTaskCommitStatus = shuffleStageStatus.getMapTaskCommitStatus();
        boolean dataAvailable = mapTaskCommitStatus != null &&
            mapTaskCommitStatus.isPartitionDataAvailable(fetchTaskAttemptIds);
        String fileCompressionCodec = ""; // TODO delete this
        ConnectDownloadResponse connectResponse =
            new ConnectDownloadResponse(serverId, fileCompressionCodec, mapTaskCommitStatus,
                dataAvailable);
        logger.info("ConnectDownloadResponse: {}, {}", connectResponse, connectionInfo);
        sendResponseAndFiles(ctx, dataAvailable, shuffleStageStatus, connectResponse, idleCheck);
      } else if (msg instanceof GetDataAvailabilityRequest) {
        logger.info("GetDataAvailabilityRequest: {}, {}", msg, connectionInfo);
        ShuffleStageStatus shuffleStageStatus =
            downloadServerHandler.getShuffleStageStatus(appShufflePartitionId.getAppShuffleId());
        MapTaskCommitStatus mapTaskCommitStatus = shuffleStageStatus.getMapTaskCommitStatus();
        boolean dataAvailable;
        dataAvailable = mapTaskCommitStatus != null &&
            mapTaskCommitStatus.isPartitionDataAvailable(fetchTaskAttemptIds);
        GetDataAvailabilityResponse getDataAvailabilityResponse =
            new GetDataAvailabilityResponse(mapTaskCommitStatus, dataAvailable);
        sendResponseAndFiles(ctx, dataAvailable, shuffleStageStatus, getDataAvailabilityResponse,
            idleCheck);
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
    M3Stats.addException(cause, "serverHandler");
    String msg = "Got exception " + connectionInfo;
    logger.warn(msg, cause);
    ctx.close();
  }

  // send response to client, also send files if data is available
  private void sendResponseAndFiles(ChannelHandlerContext ctx, boolean dataAvailable,
                                    ShuffleStageStatus shuffleStageStatus,
                                    BaseMessage responseMessage, ChannelIdleCheck idleCheck) {
    byte responseStatus = shuffleStageStatus.transformToMessageResponseStatus();
    if (dataAvailable) {
      // TODO optimize following and run them asynchronously and only run once for each stage
      downloadServerHandler.finishShuffleStage(appShufflePartitionId.getAppShuffleId());

      List<FilePathAndLength> files =
          downloadServerHandler.getNonEmptyPartitionFiles(connectionInfo);

      ChannelFuture responseMessageChannelFuture =
          HandlerUtil.writeResponseMsg(ctx, responseStatus, responseMessage, true);

      if (shuffleStageStatus.getFileStatus() == ShuffleStageStatus.FILE_STATUS_CORRUPTED) {
        logger.warn("Partition file corrupted, partition {}, {}", appShufflePartitionId,
            connectionInfo);
        responseMessageChannelFuture.addListener(new ChannelFutureCloseListener(connectionInfo));
        return;
      }

      long dataLength = files.stream().mapToLong(t -> t.getLength()).sum();
      ByteBuf dataLengthBuf = ctx.alloc().buffer(Long.BYTES);
      dataLengthBuf.writeLong(dataLength);
      ChannelFuture dataLengthChannelFuture = ctx.writeAndFlush(dataLengthBuf);

      if (files.isEmpty()) {
        logger.warn("No partition file, partition {}, {}", appShufflePartitionId, connectionInfo);
        dataLengthChannelFuture.addListener(new ChannelFutureCloseListener(connectionInfo));
      } else {
        ChannelFuture sendFileChannelFuture =
            downloadServerHandler.sendFiles(ctx, files, idleCheck);
        if (sendFileChannelFuture == null) {
          logger.warn("No file sent out, closing the connection, partition {}, {}",
              appShufflePartitionId, connectionInfo);
          dataLengthChannelFuture.addListener(new ChannelFutureCloseListener(connectionInfo));
        }
      }
    } else {
      ChannelFuture channelFuture =
          HandlerUtil.writeResponseMsg(ctx, responseStatus, responseMessage, true);
      if (shuffleStageStatus.getFileStatus() == ShuffleStageStatus.FILE_STATUS_CORRUPTED) {
        logger.warn("Partition file corrupted, partition {}, {}", appShufflePartitionId,
            connectionInfo);
        channelFuture.addListener(new ChannelFutureCloseListener(connectionInfo));
      }
    }
  }
}
