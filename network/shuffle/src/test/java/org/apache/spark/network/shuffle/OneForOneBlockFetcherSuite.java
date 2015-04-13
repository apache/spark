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
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;

public class OneForOneBlockFetcherSuite {
  @Test
  public void testFetchOne() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffle_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[0])));

    BlockFetchingListener listener = fetchBlocks(blocks);

    verify(listener).onBlockFetchSuccess("shuffle_0_0_0", blocks.get("shuffle_0_0_0"));
  }

  @Test
  public void testFetchThree() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("b1", new NioManagedBuffer(ByteBuffer.wrap(new byte[23])));
    blocks.put("b2", new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[23])));

    BlockFetchingListener listener = fetchBlocks(blocks);

    for (int i = 0; i < 3; i ++) {
      verify(listener, times(1)).onBlockFetchSuccess("b" + i, blocks.get("b" + i));
    }
  }

  @Test
  public void testFailure() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("b1", null);
    blocks.put("b2", null);

    BlockFetchingListener listener = fetchBlocks(blocks);

    // Each failure will cause a failure to be invoked in all remaining block fetches.
    verify(listener, times(1)).onBlockFetchSuccess("b0", blocks.get("b0"));
    verify(listener, times(1)).onBlockFetchFailure(eq("b1"), (Throwable) any());
    verify(listener, times(2)).onBlockFetchFailure(eq("b2"), (Throwable) any());
  }

  @Test
  public void testFailureAndSuccess() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("b1", null);
    blocks.put("b2", new NioManagedBuffer(ByteBuffer.wrap(new byte[21])));

    BlockFetchingListener listener = fetchBlocks(blocks);

    // We may call both success and failure for the same block.
    verify(listener, times(1)).onBlockFetchSuccess("b0", blocks.get("b0"));
    verify(listener, times(1)).onBlockFetchFailure(eq("b1"), (Throwable) any());
    verify(listener, times(1)).onBlockFetchSuccess("b2", blocks.get("b2"));
    verify(listener, times(1)).onBlockFetchFailure(eq("b2"), (Throwable) any());
  }

  @Test
  public void testEmptyBlockFetch() {
    try {
      fetchBlocks(Maps.<String, ManagedBuffer>newLinkedHashMap());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Zero-sized blockIds array", e.getMessage());
    }
  }

  /**
   * Begins a fetch on the given set of blocks by mocking out the server side of the RPC which
   * simply returns the given (BlockId, Block) pairs.
   * As "blocks" is a LinkedHashMap, the blocks are guaranteed to be returned in the same order
   * that they were inserted in.
   *
   * If a block's buffer is "null", an exception will be thrown instead.
   */
  private BlockFetchingListener fetchBlocks(final LinkedHashMap<String, ManagedBuffer> blocks) {
    TransportClient client = mock(TransportClient.class);
    BlockFetchingListener listener = mock(BlockFetchingListener.class);
    final String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);
    OneForOneBlockFetcher fetcher =
      new OneForOneBlockFetcher(client, "app-id", "exec-id", blockIds, listener);

    // Respond to the "OpenBlocks" message with an appropirate ShuffleStreamHandle with streamId 123
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        BlockTransferMessage message = BlockTransferMessage.Decoder.fromByteArray(
          (byte[]) invocationOnMock.getArguments()[0]);
        RpcResponseCallback callback = (RpcResponseCallback) invocationOnMock.getArguments()[1];
        callback.onSuccess(new StreamHandle(123, blocks.size()).toByteArray());
        assertEquals(new OpenBlocks("app-id", "exec-id", blockIds), message);
        return null;
      }
    }).when(client).sendRpc((byte[]) any(), (RpcResponseCallback) any());

    // Respond to each chunk request with a single buffer from our blocks array.
    final AtomicInteger expectedChunkIndex = new AtomicInteger(0);
    final Iterator<ManagedBuffer> blockIterator = blocks.values().iterator();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        try {
          long streamId = (Long) invocation.getArguments()[0];
          int myChunkIndex = (Integer) invocation.getArguments()[1];
          assertEquals(123, streamId);
          assertEquals(expectedChunkIndex.getAndIncrement(), myChunkIndex);

          ChunkReceivedCallback callback = (ChunkReceivedCallback) invocation.getArguments()[2];
          ManagedBuffer result = blockIterator.next();
          if (result != null) {
            callback.onSuccess(myChunkIndex, result);
          } else {
            callback.onFailure(myChunkIndex, new RuntimeException("Failed " + myChunkIndex));
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("Unexpected failure");
        }
        return null;
      }
    }).when(client).fetchChunk(anyLong(), anyInt(), (ChunkReceivedCallback) any());

    fetcher.start();
    return listener;
  }
}
