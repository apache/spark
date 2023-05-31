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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.shuffle.protocol.AbstractFetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlockChunks;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockId$;
import org.apache.spark.storage.ShuffleBlockBatchId;
import org.apache.spark.storage.ShuffleBlockChunkId;
import org.apache.spark.storage.ShuffleIdCommon;

/**
 * Simple wrapper on top of a TransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 *
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class OneForOneBlockFetcher {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);

  private final TransportClient client;
  private final BlockTransferMessage message;
  private final String[] inputBlockIds;
  private final String[] blockIds;
  private final BlockFetchingListener listener;
  private final ChunkReceivedCallback chunkCallback;
  private final TransportConf transportConf;
  private final DownloadFileManager downloadFileManager;

  private StreamHandle streamHandle = null;

  public OneForOneBlockFetcher(
    TransportClient client,
    String appId,
    String execId,
    String[] inputBlockIds,
    BlockFetchingListener listener,
    TransportConf transportConf) {
    this(client, appId, execId, inputBlockIds, listener, transportConf, null);
  }

  public OneForOneBlockFetcher(
      TransportClient client,
      String appId,
      String execId,
      String[] inputBlockIds,
      BlockFetchingListener listener,
      TransportConf transportConf,
      DownloadFileManager downloadFileManager) {
    this.client = client;
    this.listener = listener;
    this.chunkCallback = new ChunkCallback();
    this.transportConf = transportConf;
    this.downloadFileManager = downloadFileManager;
    if (inputBlockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }
    this.inputBlockIds = inputBlockIds;

    // SPARK-40398 optimized just the initial looping logic that checked if the input was
    // all shuffles or all shuffle chunks.
    // With typed BlockIds, we can further optimize and simplify by parsing the all of
    // the input block ids only once.
    boolean allShuffle = true;
    boolean allShuffleChunk = true;

    // typedInputBlockIds has the same index ordering as inputBlockIds
    BlockId[] typedInputBlockIds = new BlockId[inputBlockIds.length];
    for (int i = 0; (allShuffle || allShuffleChunk) && i < inputBlockIds.length; i++) {
      BlockId typedBlockId = BlockId$.MODULE$.apply(inputBlockIds[i]);
      typedInputBlockIds[i] = typedBlockId;
      allShuffle = allShuffle && typedBlockId.isShuffle();
      allShuffleChunk = allShuffleChunk && typedBlockId.isShuffleChunk();
    }

    if (!transportConf.useOldFetchProtocol() && (allShuffle || allShuffleChunk)) {
      this.blockIds = new String[inputBlockIds.length];
      this.message = createFetchShuffleBlocksOrChunksMsg(appId, execId, typedInputBlockIds);
    } else {
      this.blockIds = inputBlockIds;
      this.message = new OpenBlocks(appId, execId, blockIds);
    }
  }

  /** Creates either a {@link FetchShuffleBlocks} or {@link FetchShuffleBlockChunks} message. */
  private AbstractFetchShuffleBlocks createFetchShuffleBlocksOrChunksMsg(
      String appId,
      String execId,
      BlockId[] typedInputBlockIds) {
    BlockId firstBlockId = typedInputBlockIds[0];
    if (firstBlockId.isShuffleChunk()) {
      return createFetchShuffleChunksMsg(appId, execId, typedInputBlockIds);
    } else {
      return createFetchShuffleBlocksMsg(appId, execId, typedInputBlockIds);
    }
  }

  private AbstractFetchShuffleBlocks createFetchShuffleBlocksMsg(
      String appId,
      String execId,
      BlockId[] typedInputBlockIds) {
    BlockId firstBlockId = typedInputBlockIds[0];
    boolean batchFetchEnabled = firstBlockId instanceof ShuffleBlockBatchId;
    int shuffleId = ((ShuffleIdCommon) firstBlockId).shuffleId();
    Map<Long, BlocksInfo> mapIdToBlocksInfo = new LinkedHashMap<>();
    for (int i = 0; i < typedInputBlockIds.length; i++) {
      ShuffleIdCommon commonShuffleId = (ShuffleIdCommon) typedInputBlockIds[i];
      if (commonShuffleId.shuffleId() != shuffleId) {
        throw new IllegalArgumentException(
          "Expected shuffleId=" + shuffleId + ", got:" + this.inputBlockIds[i]);
      }

      BlocksInfo blocksInfoByMapId = mapIdToBlocksInfo.computeIfAbsent(
        commonShuffleId.mapId(),
        id -> new BlocksInfo());
      blocksInfoByMapId.blockIds.add(this.inputBlockIds[i]);
      blocksInfoByMapId.ids.add(commonShuffleId.reduceId());

      if (batchFetchEnabled) {
        // When we read continuous shuffle blocks in batch, we will reuse reduceIds in
        // FetchShuffleBlocks to store the start and end reduce id for range
        // [startReduceId, endReduceId).
        int endReduceId = ((ShuffleBlockBatchId) typedInputBlockIds[i]).endReduceId();
        blocksInfoByMapId.ids.add(endReduceId);
      }
    }

    int[][] reduceIdsArray = getSecondaryIds(mapIdToBlocksInfo);
    long[] mapIds = Longs.toArray(mapIdToBlocksInfo.keySet());
    return new FetchShuffleBlocks(
      appId, execId, shuffleId, mapIds, reduceIdsArray, batchFetchEnabled);
  }

  private AbstractFetchShuffleBlocks createFetchShuffleChunksMsg(
      String appId,
      String execId,
      BlockId[] typedInputBlockIds) {
    ShuffleBlockChunkId firstShuffleBlockChunkId =
      (ShuffleBlockChunkId) typedInputBlockIds[0];
    int shuffleId = firstShuffleBlockChunkId.shuffleId();
    int shuffleMergeId = firstShuffleBlockChunkId.shuffleMergeId();
    Map<Integer, BlocksInfo> reduceIdToBlocksInfo = new LinkedHashMap<>();
    for (int i = 0; i < typedInputBlockIds.length; i++) {
      ShuffleBlockChunkId shuffleBlockChunkId = (ShuffleBlockChunkId) typedInputBlockIds[i];
      if (shuffleBlockChunkId.shuffleId() != shuffleId ||
          shuffleBlockChunkId.shuffleMergeId() != shuffleMergeId) {
        throw new IllegalArgumentException(String.format("Expected shuffleId = %s and"
          + " shuffleMergeId = %s but got %s", shuffleId, shuffleMergeId, this.inputBlockIds[i]));
      }
      BlocksInfo blocksInfoByReduceId = reduceIdToBlocksInfo.computeIfAbsent(
        shuffleBlockChunkId.reduceId(),
        id -> new BlocksInfo());
      blocksInfoByReduceId.blockIds.add(this.inputBlockIds[i]);
      blocksInfoByReduceId.ids.add(shuffleBlockChunkId.chunkId());
    }

    int[][] chunkIdsArray = getSecondaryIds(reduceIdToBlocksInfo);
    int[] reduceIds = Ints.toArray(reduceIdToBlocksInfo.keySet());

    return new FetchShuffleBlockChunks(appId, execId, shuffleId, shuffleMergeId, reduceIds,
      chunkIdsArray);
  }

  private int[][] getSecondaryIds(Map<? extends Number, BlocksInfo> primaryIdsToBlockInfo) {
    // In case of FetchShuffleBlocks, secondaryIds are reduceIds. For FetchShuffleBlockChunks,
    // secondaryIds are chunkIds.
    int[][] secondaryIds = new int[primaryIdsToBlockInfo.size()][];
    int blockIdIndex = 0;
    int secIndex = 0;
    for (BlocksInfo blocksInfo: primaryIdsToBlockInfo.values()) {
      secondaryIds[secIndex++] = Ints.toArray(blocksInfo.ids);

      // The `blockIds`'s order must be same with the read order specified in FetchShuffleBlocks/
      // FetchShuffleBlockChunks because the shuffle data's return order should match the
      // `blockIds`'s order to ensure blockId and data match.
      for (String blockId : blocksInfo.blockIds) {
        this.blockIds[blockIdIndex++] = blockId;
      }
    }
    assert(blockIdIndex == this.blockIds.length);
    return secondaryIds;
  }

  /** The reduceIds and blocks in a single mapId */
  private static class BlocksInfo {

    /**
     * For {@link FetchShuffleBlocks} message, the ids are reduceIds.
     * For {@link FetchShuffleBlockChunks} message, the ids are chunkIds.
     */
    final ArrayList<Integer> ids;
    final ArrayList<String> blockIds;

    BlocksInfo() {
      this.ids = new ArrayList<>();
      this.blockIds = new ArrayList<>();
    }
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
  private class ChunkCallback implements ChunkReceivedCallback {
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, e);
    }
  }

  /**
   * Begins the fetching process, calling the listener with every block fetched.
   * The given message will be serialized with the Java serializer, and the RPC must return a
   * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
   */
  public void start() {
    client.sendRpc(message.toByteBuffer(), new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          for (int i = 0; i < streamHandle.numChunks; i++) {
            if (downloadFileManager != null) {
              client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
                new DownloadCallback(i));
            } else {
              client.fetchChunk(streamHandle.streamId, i, chunkCallback);
            }
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          failRemainingBlocks(blockIds, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(blockIds, e);
      }
    });
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }

  private class DownloadCallback implements StreamCallback {

    private DownloadFileWritableChannel channel = null;
    private DownloadFile targetFile = null;
    private int chunkIndex;

    DownloadCallback(int chunkIndex) throws IOException {
      this.targetFile = downloadFileManager.createTempFile(transportConf);
      this.channel = targetFile.openForWriting();
      this.chunkIndex = chunkIndex;
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      listener.onBlockFetchSuccess(blockIds[chunkIndex], channel.closeAndRead());
      if (!downloadFileManager.registerTempFileToClean(targetFile)) {
        targetFile.delete();
      }
    }

    @Override
    public void onFailure(String streamId, Throwable cause) throws IOException {
      channel.close();
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, cause);
      targetFile.delete();
    }
  }
}
