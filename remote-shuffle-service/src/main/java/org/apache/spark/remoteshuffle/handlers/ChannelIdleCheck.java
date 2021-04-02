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
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ChannelIdleCheck implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(ChannelIdleCheck.class);

  private final ChannelHandlerContext ctx;
  private final long idleTimeoutMillis;
  private final Counter closedIdleChannelCounterMetric;

  private volatile long lastReadTime = System.currentTimeMillis();
  private volatile boolean canceled = false;

  public ChannelIdleCheck(ChannelHandlerContext ctx, long idleTimeoutMillis,
                          Counter closedIdleChannelCounterMetric) {
    this.ctx = ctx;
    this.idleTimeoutMillis = idleTimeoutMillis;
    this.closedIdleChannelCounterMetric = closedIdleChannelCounterMetric;
  }

  @Override
  public void run() {
    try {
      if (canceled) {
        return;
      }

      if (!ctx.channel().isOpen()) {
        return;
      }

      checkIdle(ctx);
    } catch (Throwable ex) {
      logger.warn(
          String.format("Failed to run idle check, %s", NettyUtils.getServerConnectionInfo(ctx)),
          ex);
    }
  }

  public void updateLastReadTime() {
    lastReadTime = System.currentTimeMillis();
  }

  public void cancel() {
    canceled = true;
  }

  public void schedule() {
    ctx.executor().schedule(this, idleTimeoutMillis, TimeUnit.MILLISECONDS);
  }

  private void checkIdle(ChannelHandlerContext ctx) {
    if (System.currentTimeMillis() - lastReadTime >= idleTimeoutMillis) {
      closedIdleChannelCounterMetric.inc(1);
      logger.info("Closing idle connection {}", NettyUtils.getServerConnectionInfo(ctx));
      ctx.close();
      return;
    }

    schedule();
  }
}
