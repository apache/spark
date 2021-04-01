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

import org.apache.spark.remoteshuffle.metrics.ApplicationJobStatusMetrics;
import org.apache.spark.remoteshuffle.metrics.ApplicationMetrics;
import org.apache.spark.remoteshuffle.metrics.NotifyServerMetricsContainer;
import org.apache.spark.remoteshuffle.util.MonitorUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.spark.remoteshuffle.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * This class handles messages as a notify server, which accepts some notification from shuffle client.
 */
public class NotifyServerHandler {
  private static final Logger logger = LoggerFactory.getLogger(NotifyServerHandler.class);

  private static final NotifyServerMetricsContainer metricsContainer =
      new NotifyServerMetricsContainer();

  private final String serverId;

  private String user;

  public NotifyServerHandler(String serverId) {
    this.serverId = serverId;
  }

  public void handleMessage(ChannelHandlerContext ctx, ConnectNotifyRequest msg) {
    logger.debug("Handle message: " + msg);
    user = msg.getUser();

    ConnectNotifyResponse response = new ConnectNotifyResponse(serverId);
    HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true);
  }

  public void handleMessage(ChannelHandlerContext ctx, FinishApplicationJobRequestMessage msg) {
    writeAndFlushByte(ctx, MessageConstants.RESPONSE_STATUS_OK);

    ApplicationJobStatusMetrics metrics =
        metricsContainer.getApplicationJobStatusMetrics(user, msg.getJobStatus());
    metrics.getNumApplicationJobs().inc(1);

    String exceptionDetail = msg.getExceptionDetail();
    if (MonitorUtils.hasRssException(exceptionDetail)) {
      metrics.getNumRssExceptionJobs().inc(1);
    }
  }

  public void handleMessage(ChannelHandlerContext ctx,
                            FinishApplicationAttemptRequestMessage msg) {
    logger.info("finishApplicationAttempt, appId: {}, appAttempt: {}", msg.getAppId(),
        msg.getAppAttempt());

    writeAndFlushByte(ctx, MessageConstants.RESPONSE_STATUS_OK);

    ApplicationMetrics metrics = metricsContainer.getApplicationMetrics(user, msg.getAppAttempt());
    metrics.getNumApplications().inc(1);
  }

  private void writeAndFlushByte(ChannelHandlerContext ctx, byte value) {
    ByteBuf buf = ctx.alloc().buffer(1);
    buf.writeByte(value);
    ctx.writeAndFlush(buf);
  }
}
