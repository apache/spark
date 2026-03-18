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

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * Verifies reference counting correctness in SslMessageEncoder.encode() for messages with a
 * NettyManagedBuffer body.
 *
 * <p>When convertToNettyForSsl() returns a ByteBuf, the encoder wraps it in an
 * EncryptedMessageWithHeader whose close() releases the ManagedBuffer. This mirrors the non-SSL
 * MessageEncoder which uses MessageWithHeader.deallocate().
 */
public class SslMessageEncoderSuite {

  /**
   * Core regression test: encoding an RpcRequest with a NettyManagedBuffer body must leave the
   * underlying ByteBuf at refCnt=0 after Netty reads and closes the EncryptedMessageWithHeader.
   */
  @Test
  public void testNettyManagedBufferBodyIsReleasedAfterEncoding() throws Exception {
    ByteBuf bodyBuf = Unpooled.copyLong(1L);
    assertEquals(1, bodyBuf.refCnt());

    RpcRequest rpcRequest = new RpcRequest(1L, new NettyManagedBuffer(bodyBuf));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    List<Object> out = new ArrayList<>();
    SslMessageEncoder.INSTANCE.encode(ctx, rpcRequest, out);

    assertEquals(1, out.size());
    assertTrue(out.get(0) instanceof EncryptedMessageWithHeader);

    EncryptedMessageWithHeader msg = (EncryptedMessageWithHeader) out.get(0);

    // convertToNettyForSsl() called retain on a duplicate, so refCnt is 2
    // (original + duplicate). The ManagedBuffer has not been released yet — that
    // happens when close() is called.
    assertEquals(2, bodyBuf.refCnt());

    // Simulate Netty's ChunkedWriteHandler: read the chunk, then release it.
    ByteBuf chunk = msg.readChunk(UnpooledByteBufAllocator.DEFAULT);
    assertNotNull(chunk);
    assertTrue(msg.isEndOfInput());
    ReferenceCountUtil.release(chunk);

    // Simulate Netty closing the ChunkedInput after transfer completes.
    msg.close();

    // After close(), the ManagedBuffer is released, bringing refCnt to 0.
    assertEquals(0, bodyBuf.refCnt());
  }
}
