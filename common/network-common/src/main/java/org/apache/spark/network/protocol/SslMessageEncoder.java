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
package org.apache.spark.network.protocol;

import java.io.InputStream;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.stream.ChunkedStream;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

/**
 * Encoder used by the server side to encode secure (SSL) server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class SslMessageEncoder extends MessageToMessageEncoder<Message> {

  private static final Logger logger = LoggerFactory.getLogger(SslMessageEncoder.class);

  private SslMessageEncoder() {}

  public static final SslMessageEncoder INSTANCE = new SslMessageEncoder();

  /**
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out'.
   */
  @Override
  public void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {
    Object body = null;
    long bodyLength = 0;
    boolean isBodyInFrame = false;

    // If the message has a body, take it out...
    // For SSL, zero-copy transfer will not work, so we will check if
    // the body is an InputStream, and if so, use an EncryptedMessageWithHeader
    // to wrap the header+body appropriately (for thread safety).
    if (in.body() != null) {
      try {
        bodyLength = in.body().size();
        body = in.body().convertToNettyForSsl();
        isBodyInFrame = in.isBodyInFrame();
      } catch (Exception e) {
        in.body().release();
        if (in instanceof AbstractResponseMessage resp) {
          // Re-encode this message as a failure response.
          String error = e.getMessage() != null ? e.getMessage() : "null";
          logger.error("Error processing {} for client {}", e,
            MDC.of(LogKeys.MESSAGE$.MODULE$, in),
            MDC.of(LogKeys.HOST_PORT$.MODULE$, ctx.channel().remoteAddress()));
          encode(ctx, resp.createFailureResponse(error), out);
        } else {
          throw e;
        }
        return;
      }
    }

    Message.Type msgType = in.type();
    // All messages have the frame length, message type, and message itself. The frame length
    // may optionally include the length of the body data, depending on what message is being
    // sent.
    int headerLength = 8 + msgType.encodedLength() + in.encodedLength();
    long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
    ByteBuf header = ctx.alloc().buffer(headerLength);
    header.writeLong(frameLength);
    msgType.encode(header);
    in.encode(header);
    assert header.writableBytes() == 0;

    if (body != null && bodyLength > 0) {
      if (body instanceof ByteBuf byteBuf) {
        out.add(Unpooled.wrappedBuffer(header, byteBuf));
      } else if (body instanceof InputStream || body instanceof ChunkedStream) {
        // For now, assume the InputStream is doing proper chunking.
        out.add(new EncryptedMessageWithHeader(in.body(), header, body, bodyLength));
      } else {
        throw new IllegalArgumentException(
          "Body must be a ByteBuf, ChunkedStream or an InputStream");
      }
    } else {
      out.add(header);
    }
  }
}
