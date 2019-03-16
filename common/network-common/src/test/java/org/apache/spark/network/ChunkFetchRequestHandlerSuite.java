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

import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import org.apache.spark.network.server.ChunkFetchRequestHandler;
import org.junit.Test;

import static org.mockito.Mockito.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;

public class ChunkFetchRequestHandlerSuite {

  @Test
  public void handleChunkFetchRequest() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel())
      .thenAnswer(invocationOnMock0 -> {
        return channel;
      });
    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs =
      new ArrayList<>();
    when(channel.writeAndFlush(any()))
      .thenAnswer(invocationOnMock0 -> {
        Object response = invocationOnMock0.getArguments()[0];
        ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
        responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
        return channelFuture;
      });

    // Prepare the stream.
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    managedBuffers.add(new TestManagedBuffer(20));
    managedBuffers.add(new TestManagedBuffer(30));
    managedBuffers.add(new TestManagedBuffer(40));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);
    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(reverseClient,
      rpcHandler.getStreamManager(), 2L);

    RequestMessage request0 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request0);
    assert responseAndPromisePairs.size() == 1;
    assert responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess;
    assert ((ChunkFetchSuccess) (responseAndPromisePairs.get(0).getLeft())).body() ==
      managedBuffers.get(0);

    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    requestHandler.channelRead(context, request1);
    assert responseAndPromisePairs.size() == 2;
    assert responseAndPromisePairs.get(1).getLeft() instanceof ChunkFetchSuccess;
    assert ((ChunkFetchSuccess) (responseAndPromisePairs.get(1).getLeft())).body() ==
      managedBuffers.get(1);

    // Finish flushing the response for request0.
    responseAndPromisePairs.get(0).getRight().finish(true);

    RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId, 2));
    requestHandler.channelRead(context, request2);
    assert responseAndPromisePairs.size() == 3;
    assert responseAndPromisePairs.get(2).getLeft() instanceof ChunkFetchSuccess;
    assert ((ChunkFetchSuccess) (responseAndPromisePairs.get(2).getLeft())).body() ==
      managedBuffers.get(2);

    RequestMessage request3 = new ChunkFetchRequest(new StreamChunkId(streamId, 3));
    requestHandler.channelRead(context, request3);
    verify(channel, times(1)).close();
    assert responseAndPromisePairs.size() == 3;
  }
}
