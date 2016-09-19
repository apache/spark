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
import java.util.Iterator;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.network.buffer.ChunkedByteBufferUtil;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.shuffle.protocol.UploadBlock;

public class ExternalShuffleBlockHandlerSuite {
  TransportClient client = mock(TransportClient.class);

  OneForOneStreamManager streamManager;
  ExternalShuffleBlockResolver blockResolver;
  RpcHandler handler;

  @Before
  public void beforeEach() {
    streamManager = mock(OneForOneStreamManager.class);
    blockResolver = mock(ExternalShuffleBlockResolver.class);
    handler = new ExternalShuffleBlockHandler(streamManager, blockResolver);
  }

  @Test
  public void testRegisterExecutor() throws Exception {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ExecutorShuffleInfo config = new ExecutorShuffleInfo(new String[] {"/a", "/b"}, 16, "sort");
    ChunkedByteBuffer registerMessage = new RegisterExecutor("app0", "exec1", config).
        toByteBuffer();
    handler.receive(client, registerMessage.toInputStream(), callback);
    verify(blockResolver, times(1)).registerExecutor("app0", "exec1", config);

    verify(callback, times(1)).onSuccess(any(ChunkedByteBuffer.class));
    verify(callback, never()).onFailure(any(Throwable.class));
    // Verify register executor request latency metrics
    Timer registerExecutorRequestLatencyMillis = (Timer) ((ExternalShuffleBlockHandler) handler)
        .getAllMetrics()
        .getMetrics()
        .get("registerExecutorRequestLatencyMillis");
    assertEquals(1, registerExecutorRequestLatencyMillis.getCount());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOpenShuffleBlocks() throws Exception {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ManagedBuffer block0Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[3]));
    ManagedBuffer block1Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
    when(blockResolver.getBlockData("app0", "exec1", "b0")).thenReturn(block0Marker);
    when(blockResolver.getBlockData("app0", "exec1", "b1")).thenReturn(block1Marker);
    ChunkedByteBuffer openBlocks = new OpenBlocks("app0", "exec1", new String[] { "b0", "b1" })
      .toByteBuffer();
    handler.receive(client, openBlocks.toInputStream(), callback);
    verify(blockResolver, times(1)).getBlockData("app0", "exec1", "b0");
    verify(blockResolver, times(1)).getBlockData("app0", "exec1", "b1");

    ArgumentCaptor<ChunkedByteBuffer> response = ArgumentCaptor.forClass(ChunkedByteBuffer.class);
    verify(callback, times(1)).onSuccess(response.capture());
    verify(callback, never()).onFailure((Throwable) any());

    StreamHandle handle =
      (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response.getValue());
    assertEquals(2, handle.numChunks);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterator<ManagedBuffer>> stream = (ArgumentCaptor<Iterator<ManagedBuffer>>)
        (ArgumentCaptor<?>) ArgumentCaptor.forClass(Iterator.class);
    verify(streamManager, times(1)).registerStream(anyString(), stream.capture());
    Iterator<ManagedBuffer> buffers = stream.getValue();
    assertEquals(block0Marker, buffers.next());
    assertEquals(block1Marker, buffers.next());
    assertFalse(buffers.hasNext());

    // Verify open block request latency metrics
    Timer openBlockRequestLatencyMillis = (Timer) ((ExternalShuffleBlockHandler) handler)
        .getAllMetrics()
        .getMetrics()
        .get("openBlockRequestLatencyMillis");
    assertEquals(1, openBlockRequestLatencyMillis.getCount());
    // Verify block transfer metrics
    Meter blockTransferRateBytes = (Meter) ((ExternalShuffleBlockHandler) handler)
        .getAllMetrics()
        .getMetrics()
        .get("blockTransferRateBytes");
    assertEquals(10, blockTransferRateBytes.getCount());
  }

  @Test
  public void testBadMessages() throws Exception {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ChunkedByteBuffer unserializableMsg = ChunkedByteBufferUtil.wrap(
        new byte[]{0x12, 0x34, 0x56});
    try {
      handler.receive(client, unserializableMsg.toInputStream(), callback);
      fail("Should have thrown");
    } catch (Exception e) {
      // pass
    }

    ChunkedByteBuffer unexpectedMsg = new UploadBlock("a", "e", "b", new byte[1],
        new NioManagedBuffer(ChunkedByteBufferUtil.wrap(new byte[2]))).toByteBuffer();
    try {
      handler.receive(client, unexpectedMsg.toInputStream(), callback);
      fail("Should have thrown");
    } catch (UnsupportedOperationException e) {
      // pass
    }

    verify(callback, never()).onSuccess(any(ChunkedByteBuffer.class));
    verify(callback, never()).onFailure(any(Throwable.class));
  }
}
