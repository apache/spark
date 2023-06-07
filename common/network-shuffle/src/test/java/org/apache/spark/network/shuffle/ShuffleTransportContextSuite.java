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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.SocketChannel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.protocol.MessageWithHeader;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ShuffleTransportContextSuite {

  private ExternalBlockHandler blockHandler;

  @Before
  public void before() throws IOException {
    blockHandler = mock(ExternalBlockHandler.class);
  }

  ShuffleTransportContext createShuffleTransportContext(boolean separateFinalizeThread)
      throws IOException {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.shuffle.server.finalizeShuffleMergeThreads",
        separateFinalizeThread ? "1" : "0");
    TransportConf transportConf = new TransportConf("shuffle",
        new MapConfigProvider(configs));
    return new ShuffleTransportContext(transportConf, blockHandler, true);
  }

  private ByteBuf getDecodableMessageBuf(Message req) throws Exception {
    List<Object> out = Lists.newArrayList();
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
    MessageEncoder.INSTANCE.encode(context, req, out);
    MessageWithHeader msgWithHeader = (MessageWithHeader) out.remove(0);
    ByteArrayWritableChannel writableChannel =
      new ByteArrayWritableChannel((int) msgWithHeader.count());
    while (msgWithHeader.transfered() < msgWithHeader.count()) {
      msgWithHeader.transferTo(writableChannel, msgWithHeader.transfered());
    }
    ByteBuf messageBuf = Unpooled.wrappedBuffer(writableChannel.getData());
    messageBuf.readLong(); // frame length
    return messageBuf;
  }

  @Test
  public void testInitializePipeline() throws IOException {
    // SPARK-43987: test that the FinalizedHandler is added to the pipeline only when configured
    for (boolean enabled : new boolean[]{true, false}) {
      ShuffleTransportContext ctx = createShuffleTransportContext(enabled);
      SocketChannel channel = new NioSocketChannel();
      RpcHandler rpcHandler = mock(RpcHandler.class);
      ctx.initializePipeline(channel, rpcHandler);
      String handlerName = ShuffleTransportContext.FinalizedHandler.HANDLER_NAME;
      if (enabled) {
        Assert.assertNotNull(channel.pipeline().get(handlerName));
      } else {
        Assert.assertNull(channel.pipeline().get(handlerName));
      }
    }
  }

  @Test
  public void testDecodeOfFinalizeShuffleMessage() throws Exception {
    // SPARK-43987: test FinalizeShuffleMerge message is decoded correctly
    FinalizeShuffleMerge finalizeRequest = new FinalizeShuffleMerge("app0", 1, 2, 3);
    RpcRequest rpcRequest = new RpcRequest(1, new NioManagedBuffer(finalizeRequest.toByteBuffer()));
    ByteBuf messageBuf = getDecodableMessageBuf(rpcRequest);
    ShuffleTransportContext shuffleTransportContext = createShuffleTransportContext(true);
    ShuffleTransportContext.ShuffleMessageDecoder decoder =
        (ShuffleTransportContext.ShuffleMessageDecoder) shuffleTransportContext.getDecoder();
    List<Object> out = Lists.newArrayList();
    decoder.decode(mock(ChannelHandlerContext.class), messageBuf, out);

    Assert.assertEquals(1, out.size());
    Assert.assertTrue(out.get(0) instanceof ShuffleTransportContext.RpcRequestInternal);
    Assert.assertEquals(BlockTransferMessage.Type.FINALIZE_SHUFFLE_MERGE,
        ((ShuffleTransportContext.RpcRequestInternal) out.get(0)).messageType);
  }

  @Test
  public void testDecodeOfAnyOtherRpcMessage() throws Exception {
    // SPARK-43987: test any other RPC message is decoded correctly
    OpenBlocks openBlocks = new OpenBlocks("app0", "1", new String[]{"block1", "block2"});
    RpcRequest rpcRequest = new RpcRequest(1, new NioManagedBuffer(openBlocks.toByteBuffer()));
    ByteBuf messageBuf = getDecodableMessageBuf(rpcRequest);
    ShuffleTransportContext shuffleTransportContext = createShuffleTransportContext(true);
    ShuffleTransportContext.ShuffleMessageDecoder decoder =
        (ShuffleTransportContext.ShuffleMessageDecoder) shuffleTransportContext.getDecoder();
    List<Object> out = Lists.newArrayList();
    decoder.decode(mock(ChannelHandlerContext.class), messageBuf, out);

    Assert.assertEquals(1, out.size());
    Assert.assertTrue(out.get(0) instanceof RpcRequest);
    Assert.assertEquals(rpcRequest.requestId, ((RpcRequest) out.get(0)).requestId);
  }
}
