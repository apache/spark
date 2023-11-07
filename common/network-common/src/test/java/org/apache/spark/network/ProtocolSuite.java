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

package org.apache.spark.network;

import java.util.List;

import com.google.common.primitives.Ints;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.protocol.OneWayMessage;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamRequest;
import org.apache.spark.network.protocol.StreamResponse;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.NettyUtils;

public class ProtocolSuite {
  private void testServerToClient(Message msg) {
    EmbeddedChannel serverChannel = new EmbeddedChannel(new FileRegionEncoder(),
      MessageEncoder.INSTANCE);
    serverChannel.writeOutbound(msg);

    EmbeddedChannel clientChannel = new EmbeddedChannel(
        NettyUtils.createFrameDecoder(), MessageDecoder.INSTANCE);

    while (!serverChannel.outboundMessages().isEmpty()) {
      clientChannel.writeOneInbound(serverChannel.readOutbound());
    }

    assertEquals(1, clientChannel.inboundMessages().size());
    assertEquals(msg, clientChannel.readInbound());
  }

  private void testClientToServer(Message msg) {
    EmbeddedChannel clientChannel = new EmbeddedChannel(new FileRegionEncoder(),
      MessageEncoder.INSTANCE);
    clientChannel.writeOutbound(msg);

    EmbeddedChannel serverChannel = new EmbeddedChannel(
        NettyUtils.createFrameDecoder(), MessageDecoder.INSTANCE);

    while (!clientChannel.outboundMessages().isEmpty()) {
      serverChannel.writeOneInbound(clientChannel.readOutbound());
    }

    assertEquals(1, serverChannel.inboundMessages().size());
    assertEquals(msg, serverChannel.readInbound());
  }

  @Test
  public void requests() {
    testClientToServer(new ChunkFetchRequest(new StreamChunkId(1, 2)));
    testClientToServer(new RpcRequest(12345, new TestManagedBuffer(0)));
    testClientToServer(new RpcRequest(12345, new TestManagedBuffer(10)));
    testClientToServer(new StreamRequest("abcde"));
    testClientToServer(new OneWayMessage(new TestManagedBuffer(10)));
  }

  @Test
  public void responses() {
    testServerToClient(new ChunkFetchSuccess(new StreamChunkId(1, 2), new TestManagedBuffer(10)));
    testServerToClient(new ChunkFetchSuccess(new StreamChunkId(1, 2), new TestManagedBuffer(0)));
    testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2), "this is an error"));
    testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2), ""));
    testServerToClient(new RpcResponse(12345, new TestManagedBuffer(0)));
    testServerToClient(new RpcResponse(12345, new TestManagedBuffer(100)));
    testServerToClient(new RpcFailure(0, "this is an error"));
    testServerToClient(new RpcFailure(0, ""));
    // Note: buffer size must be "0" since StreamResponse's buffer is written differently to the
    // channel and cannot be tested like this.
    testServerToClient(new StreamResponse("anId", 12345L, new TestManagedBuffer(0)));
    testServerToClient(new StreamFailure("anId", "this is an error"));
  }

  /**
   * Handler to transform a FileRegion into a byte buffer. EmbeddedChannel doesn't actually transfer
   * bytes, but messages, so this is needed so that the frame decoder on the receiving side can
   * understand what MessageWithHeader actually contains.
   */
  private static class FileRegionEncoder extends MessageToMessageEncoder<FileRegion> {

    @Override
    public void encode(ChannelHandlerContext ctx, FileRegion in, List<Object> out)
      throws Exception {

      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(Ints.checkedCast(in.count()));
      while (in.transferred() < in.count()) {
        in.transferTo(channel, in.transferred());
      }
      out.add(Unpooled.wrappedBuffer(channel.getData()));
    }

  }

}
