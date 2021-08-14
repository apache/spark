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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.BlockPushNonFatalFailure;
import org.apache.spark.network.server.BlockPushNonFatalFailure.ReturnCode;
import org.apache.spark.network.shuffle.protocol.BlockPushReturnCode;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
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
  private final int appAttemptId;
  private final String[] blockIds;
  private final BlockPushingListener listener;
  private final Map<String, ManagedBuffer> buffers;

  public OneForOneBlockPusher(
      TransportClient client,
      String appId,
      int appAttemptId,
      String[] blockIds,
      BlockPushingListener listener,
      Map<String, ManagedBuffer> buffers) {
    this.client = client;
    this.appId = appId;
    this.appAttemptId = appAttemptId;
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
      BlockPushReturnCode pushResponse =
        (BlockPushReturnCode) BlockTransferMessage.Decoder.fromByteBuffer(response);
      // If the return code is not SUCCESS, the server has responded some error code. Handle
      // the error accordingly.
      ReturnCode returnCode = BlockPushNonFatalFailure.getReturnCode(pushResponse.returnCode);
      if (returnCode != ReturnCode.SUCCESS) {
        String blockId = pushResponse.failureBlockId;
        Preconditions.checkArgument(!blockId.isEmpty());
        checkAndFailRemainingBlocks(index, new BlockPushNonFatalFailure(returnCode,
          BlockPushNonFatalFailure.getErrorMsg(blockId, returnCode)));
      } else {
        // On receipt of a successful block push
        listener.onBlockPushSuccess(blockId, new NioManagedBuffer(ByteBuffer.allocate(0)));
      }
    }

    @Override
    public void onFailure(Throwable e) {
      checkAndFailRemainingBlocks(index, e);
    }
  }

  private void checkAndFailRemainingBlocks(int index, Throwable e) {
    // Since block push is best effort, i.e., if we encounter a block push failure that's still
    // retriable according to ErrorHandler (not a connection exception and the block is not too
    // late), we should not fail all remaining block pushes even though
    // RetryingBlockTransferor might consider this failure not retriable (exceeding max retry
    // count etc). The best effort nature makes block push tolerable of a partial completion.
    // Thus, we only fail the block that's actually failed in this case. Note that, on the
    // RetryingBlockTransferor side, if retry is initiated, it would still invalidate the
    // previous active retry listener, and retry pushing all outstanding blocks. However, since
    // the blocks to be pushed are preloaded into memory and the first attempt of pushing these
    // blocks might have already succeeded, retry pushing all the outstanding blocks should be
    // very cheap (on the client side, the block data is in memory; on the server side, the block
    // will be recognized as a duplicate which triggers noop handling). Here, by failing only the
    // one block that's actually failed, we are essentially preventing forwarding unnecessary
    // block push failures to the parent listener of the retry listener.
    //
    // Take the following as an example. For the common exception during block push handling,
    // i.e. block collision, it is considered as retriable by ErrorHandler but not retriable
    // by RetryingBlockTransferor. When we encounter a failure of this type, we only fail the
    // one block encountering this issue not the remaining blocks in the same batch. On the
    // RetryingBlockTransferor side, since this exception is considered as not retriable, it
    // would immediately invoke parent listener's onBlockTransferFailure. However, the remaining
    // blocks in the same batch would remain current and active and they won't be impacted by
    // this exception.
    if (PUSH_ERROR_HANDLER.shouldRetryError(e)) {
      String[] targetBlockId = Arrays.copyOfRange(blockIds, index, index + 1);
      failRemainingBlocks(targetBlockId, e);
    } else {
      String[] targetBlockId = Arrays.copyOfRange(blockIds, index, blockIds.length);
      failRemainingBlocks(targetBlockId, e);
    }
  }

  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockPushFailure(blockId, e);
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
      if (blockIdParts.length != 5 || !blockIdParts[0].equals(SHUFFLE_PUSH_BLOCK_PREFIX)) {
        throw new IllegalArgumentException(
          "Unexpected shuffle push block id format: " + blockIds[i]);
      }
      ByteBuffer header =
        new PushBlockStream(appId, appAttemptId, Integer.parseInt(blockIdParts[1]),
          Integer.parseInt(blockIdParts[2]), Integer.parseInt(blockIdParts[3]),
            Integer.parseInt(blockIdParts[4]), i).toByteBuffer();
      client.uploadStream(new NioManagedBuffer(header), buffers.get(blockIds[i]),
        new BlockPushCallback(i, blockIds[i]));
    }
  }
}
