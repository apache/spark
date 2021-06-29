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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;

/**
 * Similar to {@link OneForOneBlockFetcher}, but for pushing blocks to remote shuffle service to
 * be merged instead of for fetching them from remote shuffle services. This is used by
 * ShuffleWriter when the block push process is initiated. The supplied BlockFetchingListener
 * is used to handle the success or failure in pushing each blocks.
 *
 * @since 3.1.0
 */
public class OneForOneBlockPusher {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockPusher.class);
  private static final ErrorHandler PUSH_ERROR_HANDLER = new ErrorHandler.BlockPushErrorHandler();
  public static final String SHUFFLE_PUSH_BLOCK_PREFIX = "shufflePush";

  private final TransportClient client;
  private final String appId;
  private final String[] blockIds;
  private final BlockFetchingListener listener;
  private final Map<String, ManagedBuffer> buffers;

  public OneForOneBlockPusher(
      TransportClient client,
      String appId,
      String[] blockIds,
      BlockFetchingListener listener,
      Map<String, ManagedBuffer> buffers) {
    this.client = client;
    this.appId = appId;
    this.blockIds = blockIds;
    this.listener = listener;
    this.buffers = buffers;
  }

  private class BlockPushCallback implements RpcResponseCallback {

    private int index;
    private String blockId;

    BlockPushCallback(int index, String blockId) {
      this.index = index;
      this.blockId = blockId;
    }

    @Override
    public void onSuccess(ByteBuffer response) {
      // On receipt of a successful block push
      listener.onBlockFetchSuccess(blockId, new NioManagedBuffer(ByteBuffer.allocate(0)));
    }

    @Override
    public void onFailure(Throwable e) {
      // Since block push is best effort, i.e., if we encountered a block push failure that's not
      // retriable or exceeding the max retires, we should not fail all remaining block pushes.
      // The best effort nature makes block push tolerable of a partial completion. Thus, we only
      // fail the block that's actually failed. Not that, on the RetryingBlockFetcher side, once
      // retry is initiated, it would still invalidate the previous active retry listener, and
      // retry all outstanding blocks. We are preventing forwarding unnecessary block push failures
      // to the parent listener of the retry listener. The only exceptions would be if the block
      // push failure is due to block arriving on the server side after merge finalization, or the
      // client fails to establish connection to the server side. In both cases, we would fail all
      // remaining blocks.
      if (PUSH_ERROR_HANDLER.shouldRetryError(e)) {
        String[] targetBlockId = Arrays.copyOfRange(blockIds, index, index + 1);
        failRemainingBlocks(targetBlockId, e);
      } else {
        String[] targetBlockId = Arrays.copyOfRange(blockIds, index, blockIds.length);
        failRemainingBlocks(targetBlockId, e);
      }
    }
  }

  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block push failure callback", e2);
      }
    }
  }

  /**
   * Begins the block pushing process, calling the listener with every block pushed.
   */
  public void start() {
    logger.debug("Start pushing {} blocks", blockIds.length);
    for (int i = 0; i < blockIds.length; i++) {
      assert buffers.containsKey(blockIds[i]) : "Could not find the block buffer for block "
        + blockIds[i];
      String[] blockIdParts = blockIds[i].split("_");
      if (blockIdParts.length != 4 || !blockIdParts[0].equals(SHUFFLE_PUSH_BLOCK_PREFIX)) {
        throw new IllegalArgumentException(
          "Unexpected shuffle push block id format: " + blockIds[i]);
      }
      ByteBuffer header = new PushBlockStream(appId, Integer.parseInt(blockIdParts[1]),
        Integer.parseInt(blockIdParts[2]), Integer.parseInt(blockIdParts[3]) , i).toByteBuffer();
      client.uploadStream(new NioManagedBuffer(header), buffers.get(blockIds[i]),
        new BlockPushCallback(i, blockIds[i]));
    }
  }
}
