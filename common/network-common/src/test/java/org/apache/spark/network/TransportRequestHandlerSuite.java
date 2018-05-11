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

import java.io.InvalidClassException;
import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportRequestHandler;

public class TransportRequestHandlerSuite {

  @Test
  public void handleFetchRequestAndStreamRequest() throws Exception {
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
    managedBuffers.add(new TestManagedBuffer(30));
    managedBuffers.add(new TestManagedBuffer(40));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator());
    streamManager.registerChannel(channel, streamId);
    TransportClient reverseClient = mock(TransportClient.class);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, reverseClient,
      rpcHandler, 2L);

    RequestMessage request0 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.handle(request0);
    assert responseAndPromisePairs.size() == 1;
    assert responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess;
    assert ((ChunkFetchSuccess) (responseAndPromisePairs.get(0).getLeft())).body() ==
      managedBuffers.get(0);

    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    requestHandler.handle(request1);
    assert responseAndPromisePairs.size() == 2;
    assert responseAndPromisePairs.get(1).getLeft() instanceof ChunkFetchSuccess;
    assert ((ChunkFetchSuccess) (responseAndPromisePairs.get(1).getLeft())).body() ==
      managedBuffers.get(1);

    // Finish flushing the response for request0.
    responseAndPromisePairs.get(0).getRight().finish(true);

    RequestMessage request2 = new StreamRequest(String.format("%d_%d", streamId, 2));
    requestHandler.handle(request2);
    assert responseAndPromisePairs.size() == 3;
    assert responseAndPromisePairs.get(2).getLeft() instanceof StreamResponse;
    assert ((StreamResponse) (responseAndPromisePairs.get(2).getLeft())).body() ==
      managedBuffers.get(2);

    // Request3 will trigger the close of channel, because the number of max chunks being
    // transferred is 2;
    RequestMessage request3 = new StreamRequest(String.format("%d_%d", streamId, 3));
    requestHandler.handle(request3);
    verify(channel, times(1)).close();
    assert responseAndPromisePairs.size() == 3;
  }

  @Test
  public void handleOneWayMessageWithWrongSerialVersionUID() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    Channel channel = mock(Channel.class);
    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs =
      new ArrayList<>();

    when(channel.writeAndFlush(any()))
      .thenAnswer(invocationOnMock -> {
        Object response = invocationOnMock.getArguments()[0];
        ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
        responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
        return channelFuture;
      });

    TransportClient reverseClient = mock(TransportClient.class);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, reverseClient,
      rpcHandler, 2L);

    // req.body().nioByteBuffer() is the method that throws the InvalidClassException
    // with wrong svUID, so let's mock it
    ManagedBuffer body = mock(ManagedBuffer.class);
    when(body.nioByteBuffer()).thenThrow(new InvalidClassException("test - wrong version"));
    RequestMessage msg = new OneWayMessage(body);

    requestHandler.handle(msg);

    assertEquals(responseAndPromisePairs.size(), 1);
    assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof RpcFailure);
    assertEquals(((RpcFailure) responseAndPromisePairs.get(0).getLeft()).requestId,
      RpcFailure.EMPTY_REQUEST_ID);

    responseAndPromisePairs.get(0).getRight().finish(true);
  }

  private class ExtendedChannelPromise extends DefaultChannelPromise {

    private List<GenericFutureListener<Future<Void>>> listeners = new ArrayList<>();
    private boolean success;

    ExtendedChannelPromise(Channel channel) {
      super(channel);
      success = false;
    }

    @Override
    public ChannelPromise addListener(
        GenericFutureListener<? extends Future<? super Void>> listener) {
      @SuppressWarnings("unchecked")
      GenericFutureListener<Future<Void>> gfListener =
          (GenericFutureListener<Future<Void>>) listener;
      listeners.add(gfListener);
      return super.addListener(listener);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    public void finish(boolean success) {
      this.success = success;
      listeners.forEach(listener -> {
        try {
          listener.operationComplete(this);
        } catch (Exception e) {
          // do nothing
        }
      });
    }
  }
}
