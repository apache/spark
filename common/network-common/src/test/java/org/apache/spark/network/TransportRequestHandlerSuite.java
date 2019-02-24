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

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
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
    managedBuffers.add(new TestManagedBuffer(30));
    managedBuffers.add(new TestManagedBuffer(40));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    assert streamManager.numStreamStates() == 1;

    TransportClient reverseClient = mock(TransportClient.class);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, reverseClient,
      rpcHandler, 2L);

    RequestMessage request0 = new StreamRequest(String.format("%d_%d", streamId, 0));
    requestHandler.handle(request0);
    assert responseAndPromisePairs.size() == 1;
    assert responseAndPromisePairs.get(0).getLeft() instanceof StreamResponse;
    assert ((StreamResponse) (responseAndPromisePairs.get(0).getLeft())).body() ==
      managedBuffers.get(0);

    RequestMessage request1 = new StreamRequest(String.format("%d_%d", streamId, 1));
    requestHandler.handle(request1);
    assert responseAndPromisePairs.size() == 2;
    assert responseAndPromisePairs.get(1).getLeft() instanceof StreamResponse;
    assert ((StreamResponse) (responseAndPromisePairs.get(1).getLeft())).body() ==
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

    streamManager.connectionTerminated(channel);
    assert streamManager.numStreamStates() == 0;
  }
}
