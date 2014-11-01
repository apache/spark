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

import static org.apache.spark.network.shuffle.ExternalShuffleMessages.OpenShuffleBlocks;
import static org.apache.spark.network.shuffle.ExternalShuffleMessages.RegisterExecutor;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.util.JavaUtils;

public class ExternalShuffleBlockHandlerSuite {
  TransportClient client = mock(TransportClient.class);

  OneForOneStreamManager streamManager;
  ExternalShuffleBlockManager blockManager;
  RpcHandler handler;

  @Before
  public void beforeEach() {
    streamManager = mock(OneForOneStreamManager.class);
    blockManager = mock(ExternalShuffleBlockManager.class);
    handler = new ExternalShuffleBlockHandler(streamManager, blockManager);
  }

  @Test
  public void testRegisterExecutor() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ExecutorShuffleInfo config = new ExecutorShuffleInfo(new String[] {"/a", "/b"}, 16, "sort");
    byte[] registerMessage = JavaUtils.serialize(
      new RegisterExecutor("app0", "exec1", config));
    handler.receive(client, registerMessage, callback);
    verify(blockManager, times(1)).registerExecutor("app0", "exec1", config);

    verify(callback, times(1)).onSuccess((byte[]) any());
    verify(callback, never()).onFailure((Throwable) any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOpenShuffleBlocks() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ManagedBuffer block0Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[3]));
    ManagedBuffer block1Marker = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
    when(blockManager.getBlockData("app0", "exec1", "b0")).thenReturn(block0Marker);
    when(blockManager.getBlockData("app0", "exec1", "b1")).thenReturn(block1Marker);
    byte[] openBlocksMessage = JavaUtils.serialize(
      new OpenShuffleBlocks("app0", "exec1", new String[] { "b0", "b1" }));
    handler.receive(client, openBlocksMessage, callback);
    verify(blockManager, times(1)).getBlockData("app0", "exec1", "b0");
    verify(blockManager, times(1)).getBlockData("app0", "exec1", "b1");

    ArgumentCaptor<byte[]> response = ArgumentCaptor.forClass(byte[].class);
    verify(callback, times(1)).onSuccess(response.capture());
    verify(callback, never()).onFailure((Throwable) any());

    ShuffleStreamHandle handle = JavaUtils.deserialize(response.getValue());
    assertEquals(2, handle.numChunks);

    ArgumentCaptor<Iterator> stream = ArgumentCaptor.forClass(Iterator.class);
    verify(streamManager, times(1)).registerStream(stream.capture());
    Iterator<ManagedBuffer> buffers = (Iterator<ManagedBuffer>) stream.getValue();
    assertEquals(block0Marker, buffers.next());
    assertEquals(block1Marker, buffers.next());
    assertFalse(buffers.hasNext());
  }

  @Test
  public void testBadMessages() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    byte[] unserializableMessage = new byte[] { 0x12, 0x34, 0x56 };
    try {
      handler.receive(client, unserializableMessage, callback);
      fail("Should have thrown");
    } catch (Exception e) {
      // pass
    }

    byte[] unexpectedMessage = JavaUtils.serialize(
      new ExecutorShuffleInfo(new String[] {"/a", "/b"}, 16, "sort"));
    try {
      handler.receive(client, unexpectedMessage, callback);
      fail("Should have thrown");
    } catch (UnsupportedOperationException e) {
      // pass
    }

    verify(callback, never()).onSuccess((byte[]) any());
    verify(callback, never()).onFailure((Throwable) any());
  }
}
