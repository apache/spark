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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
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
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlockChunks;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class OneForOneBlockFetcherSuite {

  private static final TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);

  @Test
  public void testFetchOne() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffle_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[0])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new FetchShuffleBlocks("app-id", "exec-id", 0, new long[] { 0 }, new int[][] {{ 0 }}, false),
      conf);

    verify(listener).onBlockFetchSuccess("shuffle_0_0_0", blocks.get("shuffle_0_0_0"));
  }

  @Test
  public void testUseOldProtocol() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffle_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[0])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new OpenBlocks("app-id", "exec-id", blockIds),
      new TransportConf("shuffle", new MapConfigProvider(
        new HashMap<String, String>() {{
          put("spark.shuffle.useOldFetchProtocol", "true");
        }}
      )));

    verify(listener).onBlockFetchSuccess("shuffle_0_0_0", blocks.get("shuffle_0_0_0"));
  }

  @Test
  public void testFetchThreeShuffleBlocks() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffle_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("shuffle_0_0_1", new NioManagedBuffer(ByteBuffer.wrap(new byte[23])));
    blocks.put("shuffle_0_0_2", new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[23])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new FetchShuffleBlocks(
        "app-id", "exec-id", 0, new long[] { 0 }, new int[][] {{ 0, 1, 2 }}, false),
      conf);

    for (int i = 0; i < 3; i ++) {
      verify(listener, times(1)).onBlockFetchSuccess(
        "shuffle_0_0_" + i, blocks.get("shuffle_0_0_" + i));
    }
  }

  @Test
  public void testBatchFetchThreeShuffleBlocks() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffle_0_0_0_3", new NioManagedBuffer(ByteBuffer.wrap(new byte[58])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new FetchShuffleBlocks(
        "app-id", "exec-id", 0, new long[] { 0 }, new int[][] {{ 0, 3 }}, true),
      conf);

    verify(listener, times(1)).onBlockFetchSuccess(
      "shuffle_0_0_0_3", blocks.get("shuffle_0_0_0_3"));
  }

  @Test
  public void testFetchThree() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("b1", new NioManagedBuffer(ByteBuffer.wrap(new byte[23])));
    blocks.put("b2", new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[23])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new OpenBlocks("app-id", "exec-id", blockIds),
      conf);

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
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new OpenBlocks("app-id", "exec-id", blockIds),
      conf);

    // Each failure will cause a failure to be invoked in all remaining block fetches.
    verify(listener, times(1)).onBlockFetchSuccess("b0", blocks.get("b0"));
    verify(listener, times(1)).onBlockFetchFailure(eq("b1"), any());
    verify(listener, times(2)).onBlockFetchFailure(eq("b2"), any());
  }

  @Test
  public void testFailureAndSuccess() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("b1", null);
    blocks.put("b2", new NioManagedBuffer(ByteBuffer.wrap(new byte[21])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new OpenBlocks("app-id", "exec-id", blockIds),
      conf);

    // We may call both success and failure for the same block.
    verify(listener, times(1)).onBlockFetchSuccess("b0", blocks.get("b0"));
    verify(listener, times(1)).onBlockFetchFailure(eq("b1"), any());
    verify(listener, times(1)).onBlockFetchSuccess("b2", blocks.get("b2"));
    verify(listener, times(1)).onBlockFetchFailure(eq("b2"), any());
  }

  @Test
  public void testEmptyBlockFetch() {
    try {
      fetchBlocks(
        Maps.newLinkedHashMap(),
        new String[] {},
        new OpenBlocks("app-id", "exec-id", new String[] {}),
        conf);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Zero-sized blockIds array", e.getMessage());
    }
  }

  @Test
  public void testFetchShuffleBlocksOrder() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffle_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[1])));
    blocks.put("shuffle_0_2_1", new NioManagedBuffer(ByteBuffer.wrap(new byte[2])));
    blocks.put("shuffle_0_10_2", new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[3])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new FetchShuffleBlocks("app-id", "exec-id", 0,
              new long[]{0, 2, 10}, new int[][]{{0}, {1}, {2}}, false),
      conf);

    for (String blockId : blockIds) {
      verify(listener).onBlockFetchSuccess(blockId, blocks.get(blockId));
    }
  }

  @Test
  public void testBatchFetchShuffleBlocksOrder() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffle_0_0_1_2", new NioManagedBuffer(ByteBuffer.wrap(new byte[1])));
    blocks.put("shuffle_0_2_2_3", new NioManagedBuffer(ByteBuffer.wrap(new byte[2])));
    blocks.put("shuffle_0_10_3_4", new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[3])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(
      blocks,
      blockIds,
      new FetchShuffleBlocks("app-id", "exec-id", 0,
              new long[]{0, 2, 10}, new int[][]{{1, 2}, {2, 3}, {3, 4}}, true),
      conf);

    for (String blockId : blockIds) {
      verify(listener).onBlockFetchSuccess(blockId, blocks.get(blockId));
    }
  }

  @Test
  public void testShuffleBlockChunksFetch() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffleChunk_0_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("shuffleChunk_0_0_0_1", new NioManagedBuffer(ByteBuffer.wrap(new byte[23])));
    blocks.put("shuffleChunk_0_0_0_2",
      new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[23])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(blocks, blockIds,
      new FetchShuffleBlockChunks("app-id", "exec-id", 0, 0, new int[] { 0 },
        new int[][] {{ 0, 1, 2 }}), conf);
    for (int i = 0; i < 3; i ++) {
      verify(listener, times(1)).onBlockFetchSuccess("shuffleChunk_0_0_0_" + i,
        blocks.get("shuffleChunk_0_0_0_" + i));
    }
  }

  @Test
  public void testShuffleBlockChunkFetchFailure() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shuffleChunk_0_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("shuffleChunk_0_0_0_1", null);
    blocks.put("shuffleChunk_0_0_0_2",
      new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[23])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockFetchingListener listener = fetchBlocks(blocks, blockIds,
      new FetchShuffleBlockChunks("app-id", "exec-id", 0, 0, new int[]{0}, new int[][]{{0, 1, 2}}),
        conf);
    verify(listener, times(1)).onBlockFetchSuccess("shuffleChunk_0_0_0_0",
      blocks.get("shuffleChunk_0_0_0_0"));
    verify(listener, times(1)).onBlockFetchFailure(eq("shuffleChunk_0_0_0_1"), any());
    verify(listener, times(1)).onBlockFetchSuccess("shuffleChunk_0_0_0_2",
      blocks.get("shuffleChunk_0_0_0_2"));
  }

  @Test
  public void testInvalidShuffleBlockIds() {
    assertThrows(IllegalArgumentException.class, () -> fetchBlocks(new LinkedHashMap<>(),
      new String[]{"shuffle_0_0"},
      new FetchShuffleBlocks("app-id", "exec-id", 0, new long[] { 0 },
        new int[][] {{ 0 }}, false), conf));
    assertThrows(IllegalArgumentException.class, () -> fetchBlocks(new LinkedHashMap<>(),
      new String[]{"shuffleChunk_0_0_0_0_0"},
      new FetchShuffleBlockChunks("app-id", "exec-id", 0, 0, new int[] { 0 },
        new int[][] {{ 0 }}), conf));
  }

  /**
   * Begins a fetch on the given set of blocks by mocking out the server side of the RPC which
   * simply returns the given (BlockId, Block) pairs.
   * As "blocks" is a LinkedHashMap, the blocks are guaranteed to be returned in the same order
   * that they were inserted in.
   *
   * If a block's buffer is "null", an exception will be thrown instead.
   */
  private static BlockFetchingListener fetchBlocks(
      LinkedHashMap<String, ManagedBuffer> blocks,
      String[] blockIds,
      BlockTransferMessage expectMessage,
      TransportConf transportConf) {
    TransportClient client = mock(TransportClient.class);
    BlockFetchingListener listener = mock(BlockFetchingListener.class);
    OneForOneBlockFetcher fetcher =
      new OneForOneBlockFetcher(client, "app-id", "exec-id", blockIds, listener, transportConf);

    // Respond to the "OpenBlocks" message with an appropriate ShuffleStreamHandle with streamId 123
    doAnswer(invocationOnMock -> {
      BlockTransferMessage message = BlockTransferMessage.Decoder.fromByteBuffer(
        (ByteBuffer) invocationOnMock.getArguments()[0]);
      RpcResponseCallback callback = (RpcResponseCallback) invocationOnMock.getArguments()[1];
      callback.onSuccess(new StreamHandle(123, blocks.size()).toByteBuffer());
      assertEquals(expectMessage, message);
      return null;
    }).when(client).sendRpc(any(ByteBuffer.class), any(RpcResponseCallback.class));

    // Respond to each chunk request with a single buffer from our blocks array.
    AtomicInteger expectedChunkIndex = new AtomicInteger(0);
    Iterator<ManagedBuffer> blockIterator = blocks.values().iterator();
    doAnswer(invocation -> {
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
    }).when(client).fetchChunk(anyLong(), anyInt(), any());

    fetcher.start();
    return listener;
  }
}
