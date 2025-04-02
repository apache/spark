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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportRequestHandler;

public class TransportRequestHandlerSuite {

  @Test
  public void handleStreamRequest() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
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
    managedBuffers.add(null);
    managedBuffers.add(new TestManagedBuffer(30));
    managedBuffers.add(new TestManagedBuffer(40));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    Assertions.assertEquals(1, streamManager.numStreamStates());

    TransportClient reverseClient = mock(TransportClient.class);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, reverseClient,
      rpcHandler, 2L, null);

    RequestMessage request0 = new StreamRequest(String.format("%d_%d", streamId, 0));
    requestHandler.handle(request0);
    Assertions.assertEquals(1, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof StreamResponse);
    Assertions.assertEquals(managedBuffers.get(0),
      ((StreamResponse) (responseAndPromisePairs.get(0).getLeft())).body());

    RequestMessage request1 = new StreamRequest(String.format("%d_%d", streamId, 1));
    requestHandler.handle(request1);
    Assertions.assertEquals(2, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(1).getLeft() instanceof StreamResponse);
    Assertions.assertEquals(managedBuffers.get(1),
      ((StreamResponse) (responseAndPromisePairs.get(1).getLeft())).body());

    // Finish flushing the response for request0.
    responseAndPromisePairs.get(0).getRight().finish(true);

    StreamRequest request2 = new StreamRequest(String.format("%d_%d", streamId, 2));
    requestHandler.handle(request2);
    Assertions.assertEquals(3, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(2).getLeft() instanceof StreamFailure);
    Assertions.assertEquals(String.format("Stream '%s' was not found.", request2.streamId),
        ((StreamFailure) (responseAndPromisePairs.get(2).getLeft())).error);

    RequestMessage request3 = new StreamRequest(String.format("%d_%d", streamId, 3));
    requestHandler.handle(request3);
    Assertions.assertEquals(4, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(3).getLeft() instanceof StreamResponse);
    Assertions.assertEquals(managedBuffers.get(3),
      ((StreamResponse) (responseAndPromisePairs.get(3).getLeft())).body());

    // Request4 will trigger the close of channel, because the number of max chunks being
    // transferred is 2;
    RequestMessage request4 = new StreamRequest(String.format("%d_%d", streamId, 4));
    requestHandler.handle(request4);
    verify(channel, times(1)).close();
    Assertions.assertEquals(4, responseAndPromisePairs.size());

    streamManager.connectionTerminated(channel);
    Assertions.assertEquals(0, streamManager.numStreamStates());
  }

  @Test
  public void handleMergedBlockMetaRequest() throws Exception {
    RpcHandler.MergedBlockMetaReqHandler metaHandler = (client, request, callback) -> {
      if (request.shuffleId != -1 && request.reduceId != -1) {
        callback.onSuccess(2, mock(ManagedBuffer.class));
      } else {
        callback.onFailure(new RuntimeException("empty block"));
      }
    };
    RpcHandler rpcHandler = new RpcHandler() {
      @Override
      public void receive(
          TransportClient client,
          ByteBuffer message,
          RpcResponseCallback callback) {}

      @Override
      public StreamManager getStreamManager() {
        return null;
      }

      @Override
      public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
        return metaHandler;
      }
    };
    Channel channel = mock(Channel.class);
    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
      return channelFuture;
    });

    TransportClient reverseClient = mock(TransportClient.class);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, reverseClient,
      rpcHandler, 2L, null);
    MergedBlockMetaRequest validMetaReq = new MergedBlockMetaRequest(19, "app1", 0, 0, 0);
    requestHandler.handle(validMetaReq);
    assertEquals(1, responseAndPromisePairs.size());
    assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof MergedBlockMetaSuccess);
    assertEquals(2,
      ((MergedBlockMetaSuccess) (responseAndPromisePairs.get(0).getLeft())).getNumChunks());

    MergedBlockMetaRequest invalidMetaReq = new MergedBlockMetaRequest(21, "app1", -1, 0, 1);
    requestHandler.handle(invalidMetaReq);
    assertEquals(2, responseAndPromisePairs.size());
    assertTrue(responseAndPromisePairs.get(1).getLeft() instanceof RpcFailure);
  }
}
