/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.io.IOException;
import java.io.InputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.logging.LogLevel;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

public class NettyLogger {
  private static final SparkLogger logger = SparkLoggerFactory.getLogger(NettyLogger.class);

  /** A Netty LoggingHandler which does not dump the message contents. */
  private static class NoContentLoggingHandler extends LoggingHandler {

    NoContentLoggingHandler(Class<?> clazz, LogLevel level) {
      super(clazz, level);
    }

    @Override
    protected String format(ChannelHandlerContext ctx, String eventName, Object arg) {
      if (arg instanceof ByteBuf byteBuf) {
        return format(ctx, eventName) + " " + byteBuf.readableBytes() + "B";
      } else if (arg instanceof ByteBufHolder byteBufHolder) {
        return format(ctx, eventName) + " " + byteBufHolder.content().readableBytes() + "B";
      } else if (arg instanceof InputStream inputStream) {
        int available = -1;
        try {
          available = inputStream.available();
        } catch (IOException ex) {
          // Swallow, but return -1 to indicate an error happened
        }
        return format(ctx, eventName, arg) + " " + available + "B";
      } else {
        return super.format(ctx, eventName, arg);
      }
    }
  }

  private final LoggingHandler loggingHandler;

  public NettyLogger() {
    if (logger.isTraceEnabled()) {
      loggingHandler = new LoggingHandler(NettyLogger.class, LogLevel.TRACE);
    } else if (logger.isDebugEnabled()) {
      loggingHandler = new NoContentLoggingHandler(NettyLogger.class, LogLevel.DEBUG);
    } else {
      loggingHandler = null;
    }
  }

  public LoggingHandler getLoggingHandler() {
    return loggingHandler;
  }
}
