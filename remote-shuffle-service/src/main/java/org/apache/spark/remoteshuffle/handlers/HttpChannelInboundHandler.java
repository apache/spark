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

import com.uber.m3.tally.Gauge;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.util.ExceptionUtils;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

import static io.netty.buffer.Unpooled.copiedBuffer;

// Based upon https://github.com/zoomulus/servers (MIT license)

public class HttpChannelInboundHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(HttpChannelInboundHandler.class);

  private final Gauge healthServerLatency = M3Stats.getDefaultScope().gauge("healthServerLatency");

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof FullHttpRequest) {
      final FullHttpRequest request = (FullHttpRequest) msg;

      HttpResponseStatus status;
      String responseMessage;
      // TODO More detailed HTTP server, for now it's just the health check
      if (request.uri().startsWith("/health")) {
        long startTime = System.currentTimeMillis();
        try {
          logger.info(
              "Hit /health endpoint sysenv: " + System.getenv("UBER_HEALTH_CHECK_TIMEOUT_RSS"));
          // disable checkDiskFreeSpace for long time spending to minute level
          // FileUtils.checkDiskFreeSpace(MIN_TOTAL_DISK_SPACE, MIN_FREE_DISK_SPACE);
        } catch (Throwable ex) {
          logger.error("Failed at checkDiskFreeSpace", ex);
          M3Stats.addException(ex, this.getClass().getSimpleName() + "CheckDiskFreeSpace");
          throw ex;
        } finally {
          responseMessage = "OK";
          status = HttpResponseStatus.OK;
          healthServerLatency.update(System.currentTimeMillis() - startTime);
        }
      } else if (request.uri().startsWith("/threadDump")) {
        responseMessage = Arrays.stream(org.apache.spark.util.Utils.getThreadDump())
            .map(t -> String.valueOf(t))
            .collect(Collectors
                .joining(System.lineSeparator() + "----------" + System.lineSeparator()));
        status = HttpResponseStatus.OK;
      } else {
        responseMessage = String.format("%s not found", request.uri());
        status = HttpResponseStatus.NOT_FOUND;
      }
      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
          copiedBuffer(responseMessage.getBytes()));

      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, responseMessage.length());

      if (HttpUtil.isKeepAlive(request)) {
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.writeAndFlush(response, ctx.voidPromise());
      } else {
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
      }
    } else {
      super.channelRead(ctx, msg);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    M3Stats.addException(cause, this.getClass().getSimpleName());
    logger.warn("HTTPHandler got exception", cause);
    ctx.writeAndFlush(new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR,
        copiedBuffer(ExceptionUtils.getSimpleMessage(cause).getBytes(StandardCharsets.UTF_8))
    )).addListener(ChannelFutureListener.CLOSE);
  }
}
