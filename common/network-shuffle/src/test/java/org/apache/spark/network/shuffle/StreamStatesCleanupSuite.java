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

import java.nio.ByteBuffer;

import io.netty.channel.Channel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;

public class StreamStatesCleanupSuite {

  @Test
  public void testStreamsAreRemovedCorrectly() {
    OneForOneStreamManager streamManager = new OneForOneStreamManager();
    ExternalShuffleBlockResolver blockResolver = mock(ExternalShuffleBlockResolver.class);
    TransportClient reverseClient
      = new TransportClient(mock(Channel.class), mock(TransportResponseHandler.class));
    RpcHandler handler = new ExternalShuffleBlockHandler(streamManager, blockResolver);

    ManagedBuffer block0Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[3]));
    ManagedBuffer block1Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
    when(blockResolver.getBlockData("app0", "exec1", 0, 0, 0))
      .thenReturn(block0Marker);
    when(blockResolver.getBlockData("app0", "exec1", 0, 0, 1))
      .thenReturn(block1Marker);
    ByteBuffer openBlocks = new OpenBlocks("app0", "exec1",
      new String[]{"shuffle_0_0_0", "shuffle_0_0_1"})
      .toByteBuffer();

    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    // Open blocks
    handler.receive(reverseClient, openBlocks, callback);
    assertEquals(1, streamManager.getStreamCount());

    // Connection closed before any FetchChunk request received
    streamManager.connectionTerminated(reverseClient.getChannel());
    assertEquals(0, streamManager.getStreamCount());
  }

}
