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

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.SluiceClientHandler;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.response.ChunkFetchFailure;
import org.apache.spark.network.protocol.response.ChunkFetchSuccess;

public class SluiceClientHandlerSuite {
  @Test
  public void handleSuccessfulFetch() {
    StreamChunkId streamChunkId = new StreamChunkId(1, 0);

    SluiceClientHandler handler = new SluiceClientHandler();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    EmbeddedChannel channel = new EmbeddedChannel(handler);

    channel.writeInbound(new ChunkFetchSuccess(streamChunkId, new TestManagedBuffer(123)));
    verify(callback, times(1)).onSuccess(eq(0), (ManagedBuffer) any());
    assertEquals(0, handler.numOutstandingRequests());
    assertFalse(channel.finish());
  }

  @Test
  public void handleFailedFetch() {
    StreamChunkId streamChunkId = new StreamChunkId(1, 0);
    SluiceClientHandler handler = new SluiceClientHandler();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.writeInbound(new ChunkFetchFailure(streamChunkId, "some error msg"));
    verify(callback, times(1)).onFailure(eq(0), (Throwable) any());
    assertEquals(0, handler.numOutstandingRequests());
    assertFalse(channel.finish());
  }

  @Test
  public void clearAllOutstandingRequests() {
    SluiceClientHandler handler = new SluiceClientHandler();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(new StreamChunkId(1, 0), callback);
    handler.addFetchRequest(new StreamChunkId(1, 1), callback);
    handler.addFetchRequest(new StreamChunkId(1, 2), callback);
    assertEquals(3, handler.numOutstandingRequests());

    EmbeddedChannel channel = new EmbeddedChannel(handler);

    channel.writeInbound(new ChunkFetchSuccess(new StreamChunkId(1, 0), new TestManagedBuffer(12)));
    channel.pipeline().fireExceptionCaught(new Exception("duh duh duhhhh"));

    // should fail both b2 and b3
    verify(callback, times(1)).onSuccess(eq(0), (ManagedBuffer) any());
    verify(callback, times(1)).onFailure(eq(1), (Throwable) any());
    verify(callback, times(1)).onFailure(eq(2), (Throwable) any());
    assertEquals(0, handler.numOutstandingRequests());
    assertFalse(channel.finish());
  }
}
