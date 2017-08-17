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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 * <p></p>
 * The following is the wire format that RPC message used, it is a typical
 * header+payload structure, while the header contains body length which will
 * be an indication for {@link org.apache.spark.network.util.TransportFrameDecoder}
 * to build a complete message out of byte array for downstream {@link ChannelHandler}
 * to use.
 * <p></p>
 * The underlying network I/O handles {@link MessageWithHeader} to transport message
 * between peers. Below shows how RPC message looks like. The header is
 * {@link MessageWithHeader#header}
 * and the body is {@link MessageWithHeader#body}.
 * <pre>
 * Byte/      0       |       1       |       2       |       3       |
 *     /              |               |               |               |
 *     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
 *     +---------------+---------------+---------------+---------------+
 *     0/ Header                                                       /
 *     /                                                               /
 *     /                                                               /
 *     /                                                               /
 *     +---------------+---------------+---------------+---------------+
 *     /  Body (a.k.a payload)                                         /
 *     +---------------+---------------+---------------+---------------+
 * </pre>
 * The detailed header wire format is shown as below. Header consists of
 * <ul>
 *     <li>1. frame length: the total byte size of header+payload,
 *     the length is an Integer value, which means maximum frame size would
 *     be 16MB</li>
 *     <li>2. message type: which subclass of {@link Message} are wrapped,
 *     the length is {@link Message.Type#encodedLength()}</li>
 *     <li>3. encoded message: some {@link Message} may provide additional
 *     information and being included in the header</li>
 * </ul>
 * <pre>
 * Byte/      0        |       1       |       2       |       3       |
 *     /               |               |               |               |
 *     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
 *     +---------------+---------------+---------------+---------------+
 *    0| frame length                                                  |
 *     +---------------+---------------+---------------+---------------+
 *    4| message type  |                                               |
 *     +---------------+       encoded message                         |
 *     |                                                               |
 *     +---------------+---------------+---------------+---------------+
 * </pre>
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {

  private static final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  public static final MessageEncoder INSTANCE = new MessageEncoder();

  private MessageEncoder() {}

  /***
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   */
  @Override
  public void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {
    Object body = null;
    long bodyLength = 0;
    boolean isBodyInFrame = false;

    // If the message has a body, take it out to enable zero-copy transfer for the payload.
    if (in.body() != null) {
      try {
        bodyLength = in.body().size();
        body = in.body().convertToNetty();
        isBodyInFrame = in.isBodyInFrame();
      } catch (Exception e) {
        in.body().release();
        if (in instanceof AbstractResponseMessage) {
          AbstractResponseMessage resp = (AbstractResponseMessage) in;
          // Re-encode this message as a failure response.
          String error = e.getMessage() != null ? e.getMessage() : "null";
          logger.error(String.format("Error processing %s for client %s",
            in, ctx.channel().remoteAddress()), e);
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
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    header.writeLong(frameLength);
    msgType.encode(header);
    in.encode(header);
    assert header.writableBytes() == 0;

    if (body != null) {
      // We transfer ownership of the reference on in.body() to MessageWithHeader.
      // This reference will be freed when MessageWithHeader.deallocate() is called.
      out.add(new MessageWithHeader(in.body(), header, body, bodyLength));
    } else {
      out.add(header);
    }
  }

}
