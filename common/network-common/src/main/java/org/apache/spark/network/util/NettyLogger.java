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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyLogger {
  private static final Logger logger = LoggerFactory.getLogger(NettyLogger.class);

  /** A Netty LoggingHandler which does not dump the message contents. */
  private static class NoContentLoggingHandler extends LoggingHandler {

    NoContentLoggingHandler(Class<?> clazz, LogLevel level) {
      super(clazz, level);
    }

    protected String format(ChannelHandlerContext ctx, String eventName, Object arg) {
      if (arg instanceof ByteBuf) {
        return format(ctx, eventName) + " " + ((ByteBuf) arg).readableBytes() + "B";
      } else if (arg instanceof ByteBufHolder) {
        return format(ctx, eventName) + " " +
          ((ByteBufHolder) arg).content().readableBytes() + "B";
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
