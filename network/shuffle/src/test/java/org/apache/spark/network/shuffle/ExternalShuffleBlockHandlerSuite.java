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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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
  public void testRegisterExecutor() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ExecutorShuffleInfo config = new ExecutorShuffleInfo(new String[] {"/a", "/b"}, 16, "sort");
    byte[] registerMessage = new RegisterExecutor("app0", "exec1", config).toByteArray();
    handler.receive(client, registerMessage, callback);
    verify(blockResolver, times(1)).registerExecutor("app0", "exec1", config);

    verify(callback, times(1)).onSuccess((byte[]) any());
    verify(callback, never()).onFailure((Throwable) any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOpenShuffleBlocks() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ManagedBuffer block0Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[3]));
    ManagedBuffer block1Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
    when(blockResolver.getBlockData("app0", "exec1", "b0")).thenReturn(block0Marker);
    when(blockResolver.getBlockData("app0", "exec1", "b1")).thenReturn(block1Marker);
    byte[] openBlocks = new OpenBlocks("app0", "exec1", new String[] { "b0", "b1" }).toByteArray();
    handler.receive(client, openBlocks, callback);
    verify(blockResolver, times(1)).getBlockData("app0", "exec1", "b0");
    verify(blockResolver, times(1)).getBlockData("app0", "exec1", "b1");

    ArgumentCaptor<byte[]> response = ArgumentCaptor.forClass(byte[].class);
    verify(callback, times(1)).onSuccess(response.capture());
    verify(callback, never()).onFailure((Throwable) any());

    StreamHandle handle =
      (StreamHandle) BlockTransferMessage.Decoder.fromByteArray(response.getValue());
    assertEquals(2, handle.numChunks);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterator<ManagedBuffer>> stream = (ArgumentCaptor<Iterator<ManagedBuffer>>)
        (ArgumentCaptor<?>) ArgumentCaptor.forClass(Iterator.class);
    verify(streamManager, times(1)).registerStream(stream.capture());
    Iterator<ManagedBuffer> buffers = stream.getValue();
    assertEquals(block0Marker, buffers.next());
    assertEquals(block1Marker, buffers.next());
    assertFalse(buffers.hasNext());
  }

  @Test
  public void testBadMessages() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    byte[] unserializableMsg = new byte[] { 0x12, 0x34, 0x56 };
    try {
      handler.receive(client, unserializableMsg, callback);
      fail("Should have thrown");
    } catch (Exception e) {
      // pass
    }

    byte[] unexpectedMsg = new UploadBlock("a", "e", "b", new byte[1], new byte[2]).toByteArray();
    try {
      handler.receive(client, unexpectedMsg, callback);
      fail("Should have thrown");
    } catch (UnsupportedOperationException e) {
      // pass
    }

    verify(callback, never()).onSuccess((byte[]) any());
    verify(callback, never()).onFailure((Throwable) any());
  }
}
