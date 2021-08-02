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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.AdditionalMatchers.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;


public class OneForOneBlockPusherSuite {

  @Test
  public void testPushOne() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shufflePush_0_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[1])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockPushingListener listener = pushBlocks(
      blocks,
      blockIds,
      Arrays.asList(new PushBlockStream("app-id", 0, 0, 0, 0, 0, 0)));

    verify(listener).onBlockPushSuccess(eq("shufflePush_0_0_0_0"), any());
  }

  @Test
  public void testPushThree() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shufflePush_0_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("shufflePush_0_0_1_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[23])));
    blocks.put("shufflePush_0_0_2_0",
      new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[23])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockPushingListener listener = pushBlocks(
      blocks,
      blockIds,
      Arrays.asList(new PushBlockStream("app-id",0,  0, 0, 0, 0, 0),
        new PushBlockStream("app-id", 0, 0, 0, 1, 0, 1),
        new PushBlockStream("app-id", 0, 0, 0, 2, 0, 2)));

    verify(listener, times(1)).onBlockPushSuccess(eq("shufflePush_0_0_0_0"), any());
    verify(listener, times(1)).onBlockPushSuccess(eq("shufflePush_0_0_1_0"), any());
    verify(listener, times(1)).onBlockPushSuccess(eq("shufflePush_0_0_2_0"), any());
  }

  @Test
  public void testServerFailures() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shufflePush_0_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("shufflePush_0_0_1_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[0])));
    blocks.put("shufflePush_0_0_2_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[0])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockPushingListener listener = pushBlocks(
      blocks,
      blockIds,
      Arrays.asList(new PushBlockStream("app-id", 0, 0, 0, 0, 0, 0),
        new PushBlockStream("app-id", 0, 0, 0, 1, 0, 1),
        new PushBlockStream("app-id", 0, 0, 0, 2, 0, 2)));

    verify(listener, times(1)).onBlockPushSuccess(eq("shufflePush_0_0_0_0"), any());
    verify(listener, times(1)).onBlockPushFailure(eq("shufflePush_0_0_1_0"), any());
    verify(listener, times(1)).onBlockPushFailure(eq("shufflePush_0_0_2_0"), any());
  }

  @Test
  public void testHandlingRetriableFailures() {
    LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
    blocks.put("shufflePush_0_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
    blocks.put("shufflePush_0_0_1_0", null);
    blocks.put("shufflePush_0_0_2_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[0])));
    String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);

    BlockPushingListener listener = pushBlocks(
      blocks,
      blockIds,
      Arrays.asList(new PushBlockStream("app-id", 0, 0, 0, 0, 0, 0),
        new PushBlockStream("app-id", 0, 0, 0, 1, 0, 1),
        new PushBlockStream("app-id", 0, 0, 0, 2, 0, 2)));

    verify(listener, times(1)).onBlockPushSuccess(eq("shufflePush_0_0_0_0"), any());
    verify(listener, times(0)).onBlockPushSuccess(not(eq("shufflePush_0_0_0_0")), any());
    verify(listener, times(0)).onBlockPushFailure(eq("shufflePush_0_0_0_0"), any());
    verify(listener, times(1)).onBlockPushFailure(eq("shufflePush_0_0_1_0"), any());
    verify(listener, times(2)).onBlockPushFailure(eq("shufflePush_0_0_2_0"), any());
  }

  /**
   * Begins a push on the given set of blocks by mocking the response from server side.
   * If a block is an empty byte, a server side retriable exception will be thrown.
   * If a block is null, a non-retriable exception will be thrown.
   */
  private static BlockPushingListener pushBlocks(
      LinkedHashMap<String, ManagedBuffer> blocks,
      String[] blockIds,
      Iterable<BlockTransferMessage> expectMessages) {
    TransportClient client = mock(TransportClient.class);
    BlockPushingListener listener = mock(BlockPushingListener.class);
    OneForOneBlockPusher pusher =
      new OneForOneBlockPusher(client, "app-id", 0, blockIds, listener, blocks);

    Iterator<Map.Entry<String, ManagedBuffer>> blockIterator = blocks.entrySet().iterator();
    Iterator<BlockTransferMessage> msgIterator = expectMessages.iterator();
    doAnswer(invocation -> {
      ByteBuffer header = ((ManagedBuffer) invocation.getArguments()[0]).nioByteBuffer();
      BlockTransferMessage message = BlockTransferMessage.Decoder.fromByteBuffer(header);
      RpcResponseCallback callback = (RpcResponseCallback) invocation.getArguments()[2];
      Map.Entry<String, ManagedBuffer> entry = blockIterator.next();
      ManagedBuffer block = entry.getValue();
      if (block != null && block.nioByteBuffer().capacity() > 0) {
        callback.onSuccess(header);
      } else if (block != null) {
        callback.onFailure(new RuntimeException("Failed " + entry.getKey()
          + ErrorHandler.BlockPushErrorHandler.BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX));
      } else {
        callback.onFailure(new RuntimeException("Quick fail " + entry.getKey()
          + ErrorHandler.BlockPushErrorHandler.TOO_LATE_OR_STALE_BLOCK_PUSH_MESSAGE_SUFFIX));
      }
      assertEquals(msgIterator.next(), message);
      return null;
    }).when(client).uploadStream(any(ManagedBuffer.class), any(), any(RpcResponseCallback.class));

    pusher.start();
    return listener;
  }
}
