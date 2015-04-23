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

import io.netty.channel.local.LocalChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;

public class TransportResponseHandlerSuite {
  @Test
  public void handleSuccessfulFetch() {
    StreamChunkId streamChunkId = new StreamChunkId(1, 0);

    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchSuccess(streamChunkId, new TestManagedBuffer(123)));
    verify(callback, times(1)).onSuccess(eq(0), (ManagedBuffer) any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedFetch() {
    StreamChunkId streamChunkId = new StreamChunkId(1, 0);
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchFailure(streamChunkId, "some error msg"));
    verify(callback, times(1)).onFailure(eq(0), (Throwable) any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void clearAllOutstandingRequests() {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(new StreamChunkId(1, 0), callback);
    handler.addFetchRequest(new StreamChunkId(1, 1), callback);
    handler.addFetchRequest(new StreamChunkId(1, 2), callback);
    assertEquals(3, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchSuccess(new StreamChunkId(1, 0), new TestManagedBuffer(12)));
    handler.exceptionCaught(new Exception("duh duh duhhhh"));

    // should fail both b2 and b3
    verify(callback, times(1)).onSuccess(eq(0), (ManagedBuffer) any());
    verify(callback, times(1)).onFailure(eq(1), (Throwable) any());
    verify(callback, times(1)).onFailure(eq(2), (Throwable) any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleSuccessfulRPC() {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcResponse(54321, new byte[7])); // should be ignored
    assertEquals(1, handler.numOutstandingRequests());

    byte[] arr = new byte[10];
    handler.handle(new RpcResponse(12345, arr));
    verify(callback, times(1)).onSuccess(eq(arr));
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedRPC() {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcFailure(54321, "uh-oh!")); // should be ignored
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcFailure(12345, "oh no"));
    verify(callback, times(1)).onFailure((Throwable) any());
    assertEquals(0, handler.numOutstandingRequests());
  }
}
