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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.MergedBlockMetaRequest;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.shuffle.checksum.Cause;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TimerWithCustomTimeUnit;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockId$;
import org.apache.spark.storage.RDDBlockId;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.storage.ShuffleBlockChunkId;

/**
 * RPC Handler for a server which can serve both RDD blocks and shuffle blocks from outside
 * of an Executor process.
 *
 * Handles registering executors and opening shuffle or disk persisted RDD blocks from them.
 * Blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one block.
 */
public class ExternalBlockHandler extends RpcHandler
    implements RpcHandler.MergedBlockMetaReqHandler {
  private static final Logger logger = LoggerFactory.getLogger(ExternalBlockHandler.class);
  private static final String SHUFFLE_MERGER_IDENTIFIER = "shuffle-push-merger";
  private static final String SHUFFLE_BLOCK_ID = "shuffle";
  private static final String SHUFFLE_CHUNK_ID = "shuffleChunk";

  @VisibleForTesting
  final ExternalShuffleBlockResolver blockManager;
  private final OneForOneStreamManager streamManager;
  private final ShuffleMetrics metrics;
  private final MergedShuffleFileManager mergeManager;

  public ExternalBlockHandler(TransportConf conf, File registeredExecutorFile)
    throws IOException {
    this(new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, registeredExecutorFile),
      new NoOpMergedShuffleFileManager(conf, null));
  }

  public ExternalBlockHandler(
      TransportConf conf,
      File registeredExecutorFile,
      MergedShuffleFileManager mergeManager) throws IOException {
    this(new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, registeredExecutorFile), mergeManager);
  }

  @VisibleForTesting
  public ExternalShuffleBlockResolver getBlockResolver() {
    return blockManager;
  }

  /** Enables mocking out the StreamManager and BlockManager. */
  @VisibleForTesting
  public ExternalBlockHandler(
      OneForOneStreamManager streamManager,
      ExternalShuffleBlockResolver blockManager) {
    this(streamManager, blockManager, new NoOpMergedShuffleFileManager(null, null));
  }

  /** Enables mocking out the StreamManager, BlockManager, and MergeManager. */
  @VisibleForTesting
  public ExternalBlockHandler(
      OneForOneStreamManager streamManager,
      ExternalShuffleBlockResolver blockManager,
      MergedShuffleFileManager mergeManager) {
    this.metrics = new ShuffleMetrics();
    this.streamManager = streamManager;
    this.blockManager = blockManager;
    this.mergeManager = mergeManager;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
    handleMessage(msgObj, client, callback);
  }

  @Override
  public StreamCallbackWithID receiveStream(
      TransportClient client,
      ByteBuffer messageHeader,
      RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(messageHeader);
    if (msgObj instanceof PushBlockStream) {
      PushBlockStream message = (PushBlockStream) msgObj;
      checkAuth(client, message.appId);
      return mergeManager.receiveBlockDataAsStream(message);
    } else {
      throw new UnsupportedOperationException("Unexpected message with #receiveStream: " + msgObj);
    }
  }

  protected void handleMessage(
      BlockTransferMessage msgObj,
      TransportClient client,
      RpcResponseCallback callback) {
    if (msgObj instanceof AbstractFetchShuffleBlocks || msgObj instanceof OpenBlocks) {
      final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
      try {
        int numBlockIds;
        long streamId;
        if (msgObj instanceof AbstractFetchShuffleBlocks) {
          AbstractFetchShuffleBlocks msg = (AbstractFetchShuffleBlocks) msgObj;
          checkAuth(client, msg.appId);
          numBlockIds = ((AbstractFetchShuffleBlocks) msgObj).getNumBlocks();
          Iterator<ManagedBuffer> iterator;
          if (msgObj instanceof  FetchShuffleBlocks) {
            iterator = new ShuffleManagedBufferIterator((FetchShuffleBlocks)msgObj);
          } else {
            iterator = new ShuffleChunkManagedBufferIterator((FetchShuffleBlockChunks) msgObj);
          }
          streamId = streamManager.registerStream(client.getClientId(), iterator,
            client.getChannel(), true);
        } else {
          // For the compatibility with the old version, still keep the support for OpenBlocks.
          OpenBlocks msg = (OpenBlocks) msgObj;
          numBlockIds = msg.blockIds.length;
          checkAuth(client, msg.appId);
          streamId = streamManager.registerStream(client.getClientId(),
            new ManagedBufferIterator(msg), client.getChannel(), true);
        }
        if (logger.isTraceEnabled()) {
          logger.trace(
            "Registered streamId {} with {} buffers for client {} from host {}",
            streamId,
            numBlockIds,
            client.getClientId(),
            getRemoteAddress(client.getChannel()));
        }
        callback.onSuccess(new StreamHandle(streamId, numBlockIds).toByteBuffer());
      } finally {
        responseDelayContext.stop();
      }

    } else if (msgObj instanceof RegisterExecutor) {
      final Timer.Context responseDelayContext =
        metrics.registerExecutorRequestLatencyMillis.time();
      try {
        RegisterExecutor msg = (RegisterExecutor) msgObj;
        checkAuth(client, msg.appId);
        blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
        mergeManager.registerExecutor(msg.appId, msg.executorInfo);
        callback.onSuccess(ByteBuffer.wrap(new byte[0]));
      } finally {
        responseDelayContext.stop();
      }

    } else if (msgObj instanceof RemoveBlocks) {
      RemoveBlocks msg = (RemoveBlocks) msgObj;
      checkAuth(client, msg.appId);
      int numRemovedBlocks = blockManager.removeBlocks(msg.appId, msg.execId, msg.blockIds);
      callback.onSuccess(new BlocksRemoved(numRemovedBlocks).toByteBuffer());

    } else if (msgObj instanceof GetLocalDirsForExecutors) {
      GetLocalDirsForExecutors msg = (GetLocalDirsForExecutors) msgObj;
      checkAuth(client, msg.appId);
      Set<String> execIdsForBlockResolver = Sets.newHashSet(msg.execIds);
      boolean fetchMergedBlockDirs = execIdsForBlockResolver.remove(SHUFFLE_MERGER_IDENTIFIER);
      Map<String, String[]> localDirs = blockManager.getLocalDirs(msg.appId,
        execIdsForBlockResolver);
      if (fetchMergedBlockDirs) {
        localDirs.put(SHUFFLE_MERGER_IDENTIFIER, mergeManager.getMergedBlockDirs(msg.appId));
      }
      callback.onSuccess(new LocalDirsForExecutors(localDirs).toByteBuffer());
    } else if (msgObj instanceof FinalizeShuffleMerge) {
      final Timer.Context responseDelayContext =
          metrics.finalizeShuffleMergeLatencyMillis.time();
      FinalizeShuffleMerge msg = (FinalizeShuffleMerge) msgObj;
      try {
        checkAuth(client, msg.appId);
        MergeStatuses statuses = mergeManager.finalizeShuffleMerge(msg);
        callback.onSuccess(statuses.toByteBuffer());
      } catch(IOException e) {
        throw new RuntimeException(String.format("Error while finalizing shuffle merge "
          + "for application %s shuffle %d with shuffleMergeId %d", msg.appId, msg.shuffleId,
            msg.shuffleMergeId), e);
      } finally {
        responseDelayContext.stop();
      }
    } else if (msgObj instanceof RemoveShuffleMerge) {
      RemoveShuffleMerge msg = (RemoveShuffleMerge) msgObj;
      checkAuth(client, msg.appId);
      logger.info("Removing shuffle merge data for application {} shuffle {} shuffleMerge {}",
          msg.appId, msg.shuffleId, msg.shuffleMergeId);
      mergeManager.removeShuffleMerge(msg);
    } else if (msgObj instanceof DiagnoseCorruption) {
      DiagnoseCorruption msg = (DiagnoseCorruption) msgObj;
      checkAuth(client, msg.appId);
      Cause cause = blockManager.diagnoseShuffleBlockCorruption(
        msg.appId, msg.execId, msg.shuffleId, msg.mapId, msg.reduceId, msg.checksum, msg.algorithm);
      // In any cases of the error, diagnoseShuffleBlockCorruption should return UNKNOWN_ISSUE,
      // so it should always reply as success.
      callback.onSuccess(new CorruptionCause(cause).toByteBuffer());
    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
  }

  @Override
  public void receiveMergeBlockMetaReq(
      TransportClient client,
      MergedBlockMetaRequest metaRequest,
      MergedBlockMetaResponseCallback callback) {
    final Timer.Context responseDelayContext = metrics.fetchMergedBlocksMetaLatencyMillis.time();
    try {
      checkAuth(client, metaRequest.appId);
      MergedBlockMeta mergedMeta =
        mergeManager.getMergedBlockMeta(metaRequest.appId, metaRequest.shuffleId,
          metaRequest.shuffleMergeId, metaRequest.reduceId);
      logger.debug(
        "Merged block chunks appId {} shuffleId {} reduceId {} num-chunks : {} ",
          metaRequest.appId, metaRequest.shuffleId, metaRequest.reduceId,
          mergedMeta.getNumChunks());
      callback.onSuccess(mergedMeta.getNumChunks(), mergedMeta.getChunksBitmapBuffer());
    } finally {
      responseDelayContext.stop();
    }
  }

  @Override
  public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
    return this;
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    metrics.caughtExceptions.inc();
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  /**
   * Removes an application (once it has been terminated), and optionally will clean up any
   * local directories associated with the executors of that application in a separate thread.
   */
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    blockManager.applicationRemoved(appId, cleanupLocalDirs);
    mergeManager.applicationRemoved(appId, cleanupLocalDirs);
  }

  /**
   * Clean up any non-shuffle files in any local directories associated with an finished executor.
   */
  public void executorRemoved(String executorId, String appId) {
    blockManager.executorRemoved(executorId, appId);
  }

  public void close() {
    blockManager.close();
    mergeManager.close();
  }

  private void checkAuth(TransportClient client, String appId) {
    if (client.getClientId() != null && !client.getClientId().equals(appId)) {
      throw new SecurityException(String.format(
        "Client for %s not authorized for application %s.", client.getClientId(), appId));
    }
  }

  /**
   * A simple class to wrap all shuffle service wrapper metrics
   */
  @VisibleForTesting
  public class ShuffleMetrics implements MetricSet {
    private final Map<String, Metric> allMetrics;
    // Time latency for open block request in ms
    private final Timer openBlockRequestLatencyMillis =
        new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
    // Time latency for executor registration latency in ms
    private final Timer registerExecutorRequestLatencyMillis =
        new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
    // Time latency for processing fetch merged blocks meta request latency in ms
    private final Timer fetchMergedBlocksMetaLatencyMillis =
        new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
    // Time latency for processing finalize shuffle merge request latency in ms
    private final Timer finalizeShuffleMergeLatencyMillis =
        new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
    // Block transfer rate in blocks per second
    private final Meter blockTransferRate = new Meter();
    // Block fetch message rate per second. When using non-batch fetches
    // (`OpenBlocks` or `FetchShuffleBlocks` with `batchFetchEnabled` as false), this will be the
    // same as the `blockTransferRate`. When batch fetches are enabled, this will represent the
    // number of batch fetches, and `blockTransferRate` will represent the number of blocks
    // returned by the fetches.
    private final Meter blockTransferMessageRate = new Meter();
    // Block transfer rate in byte per second
    private final Meter blockTransferRateBytes = new Meter();
    // Number of active connections to the shuffle service
    private Counter activeConnections = new Counter();
    // Number of exceptions caught in connections to the shuffle service
    private Counter caughtExceptions = new Counter();

    public ShuffleMetrics() {
      allMetrics = new HashMap<>();
      allMetrics.put("openBlockRequestLatencyMillis", openBlockRequestLatencyMillis);
      allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis);
      allMetrics.put("fetchMergedBlocksMetaLatencyMillis", fetchMergedBlocksMetaLatencyMillis);
      allMetrics.put("finalizeShuffleMergeLatencyMillis", finalizeShuffleMergeLatencyMillis);
      allMetrics.put("blockTransferRate", blockTransferRate);
      allMetrics.put("blockTransferMessageRate", blockTransferMessageRate);
      allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
      allMetrics.put("blockTransferAvgSize_1min", new RatioGauge() {
        @Override
        protected Ratio getRatio() {
          return Ratio.of(
              blockTransferRateBytes.getOneMinuteRate(),
              // use blockTransferMessageRate here instead of blockTransferRate to represent the
              // average size of the disk read / network message which has more operational impact
              // than the actual size of the block
              blockTransferMessageRate.getOneMinuteRate());
        }
      });
      allMetrics.put("registeredExecutorsSize",
                     (Gauge<Integer>) () -> blockManager.getRegisteredExecutorsSize());
      allMetrics.put("numActiveConnections", activeConnections);
      allMetrics.put("numCaughtExceptions", caughtExceptions);
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return allMetrics;
    }
  }

  private class ManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int index = 0;
    private final Function<Integer, ManagedBuffer> blockDataForIndexFn;
    private final int size;

    ManagedBufferIterator(OpenBlocks msg) {
      String appId = msg.appId;
      String execId = msg.execId;
      String[] blockIds = msg.blockIds;
      this.size = blockIds.length;
      BlockId firstBlockId = BlockId$.MODULE$.apply(blockIds[0]);
      if (firstBlockId instanceof ShuffleBlockId) {
        final int expectedShuffleId = ((ShuffleBlockId) firstBlockId).shuffleId();
        final ShuffleBlockId[] shuffleBlockIds = Arrays.stream(blockIds)
          .map(blockId -> {
            ShuffleBlockId shuffleBlockId = (ShuffleBlockId) (BlockId$.MODULE$.apply(blockId));
            if (expectedShuffleId != shuffleBlockId.shuffleId()) {
              throw new IllegalArgumentException("Expected shuffleId=" + expectedShuffleId +
                ", got:" + shuffleBlockId.name());
            }
            return shuffleBlockId;
          })
          .toArray(typedBlockIdsSize -> new ShuffleBlockId[typedBlockIdsSize]);
        this.blockDataForIndexFn = index -> blockManager.getBlockData(
          appId,
          execId,
          shuffleBlockIds[index].shuffleId(),
          shuffleBlockIds[index].mapId(),
          shuffleBlockIds[index].reduceId());
      } else if (firstBlockId instanceof ShuffleBlockChunkId) {
        final int expectedShuffleId = ((ShuffleBlockChunkId) firstBlockId).shuffleId();
        final int expectedShuffleMergeId = ((ShuffleBlockChunkId) firstBlockId).shuffleMergeId();
        final ShuffleBlockChunkId[] shuffleBlockChunkIds = Arrays.stream(blockIds)
          .map(blockId -> {
              ShuffleBlockChunkId shuffleBlockChunkId =
                (ShuffleBlockChunkId) (BlockId$.MODULE$.apply(blockId));
              if ((expectedShuffleId != shuffleBlockChunkId.shuffleId()) ||
                  (expectedShuffleMergeId != shuffleBlockChunkId.shuffleMergeId())) {
                throw new IllegalArgumentException(String.format(
                  "Expected shuffleId = %s and shuffleMergeId = %s but got %s",
                  expectedShuffleId, expectedShuffleMergeId, shuffleBlockChunkId.name()));
              }
              return shuffleBlockChunkId;
          })
          .toArray(typedBlockIdsSize -> new ShuffleBlockChunkId[typedBlockIdsSize]);
        this.blockDataForIndexFn = index -> mergeManager.getMergedBlockData(
          msg.appId,
          shuffleBlockChunkIds[index].shuffleId(),
          shuffleBlockChunkIds[index].shuffleMergeId(),
          shuffleBlockChunkIds[index].reduceId(),
          shuffleBlockChunkIds[index].chunkId());
      } else if (firstBlockId instanceof RDDBlockId) {
        final RDDBlockId[] rddBlockIds = Arrays.stream(blockIds)
          .map(blockId -> (RDDBlockId) (BlockId$.MODULE$.apply(blockId)))
          .toArray(typedBlockIdsSize -> new RDDBlockId[typedBlockIdsSize]);
        this.blockDataForIndexFn = index -> blockManager.getRddBlockData(
          appId,
          execId,
          rddBlockIds[index].rddId(),
          rddBlockIds[index].splitIndex());
      } else {
        throw new IllegalArgumentException("Unexpected block id format: " + blockIds[0]);
      }
    }

    @Override
    public boolean hasNext() {
      return index < size;
    }

    @Override
    public ManagedBuffer next() {
      final ManagedBuffer block = blockDataForIndexFn.apply(index);
      index++;
      metrics.blockTransferRate.mark();
      metrics.blockTransferMessageRate.mark();
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }

  private class ShuffleManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int mapIdx = 0;
    private int reduceIdx = 0;

    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final long[] mapIds;
    private final int[][] reduceIds;
    private final boolean batchFetchEnabled;

    ShuffleManagedBufferIterator(FetchShuffleBlocks msg) {
      appId = msg.appId;
      execId = msg.execId;
      shuffleId = msg.shuffleId;
      mapIds = msg.mapIds;
      reduceIds = msg.reduceIds;
      batchFetchEnabled = msg.batchFetchEnabled;
      // mapIds.length must equal to reduceIds.length, and the passed in FetchShuffleBlocks
      // must have non-empty mapIds and reduceIds, see the checking logic in
      // OneForOneBlockFetcher.
      assert(mapIds.length != 0 && mapIds.length == reduceIds.length);
    }

    @Override
    public boolean hasNext() {
      return mapIdx < mapIds.length && reduceIdx < reduceIds[mapIdx].length;
    }

    @Override
    public ManagedBuffer next() {
      ManagedBuffer block;
      if (!batchFetchEnabled) {
        block = blockManager.getBlockData(
          appId, execId, shuffleId, mapIds[mapIdx], reduceIds[mapIdx][reduceIdx]);
        if (reduceIdx < reduceIds[mapIdx].length - 1) {
          reduceIdx += 1;
        } else {
          reduceIdx = 0;
          mapIdx += 1;
        }
        metrics.blockTransferRate.mark();
      } else {
        assert(reduceIds[mapIdx].length == 2);
        int startReduceId = reduceIds[mapIdx][0];
        int endReduceId = reduceIds[mapIdx][1];
        block = blockManager.getContinuousBlocksData(appId, execId, shuffleId, mapIds[mapIdx],
          startReduceId, endReduceId);
        mapIdx += 1;
        metrics.blockTransferRate.mark(endReduceId - startReduceId);
      }
      metrics.blockTransferMessageRate.mark();
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }

  private class ShuffleChunkManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int reduceIdx = 0;
    private int chunkIdx = 0;

    private final String appId;
    private final int shuffleId;
    private final int shuffleMergeId;
    private final int[] reduceIds;
    private final int[][] chunkIds;

    ShuffleChunkManagedBufferIterator(FetchShuffleBlockChunks msg) {
      appId = msg.appId;
      shuffleId = msg.shuffleId;
      shuffleMergeId = msg.shuffleMergeId;
      reduceIds = msg.reduceIds;
      chunkIds = msg.chunkIds;
      // reduceIds.length must equal to chunkIds.length, and the passed in FetchShuffleBlockChunks
      // must have non-empty reduceIds and chunkIds, see the checking logic in
      // OneForOneBlockFetcher.
      assert(reduceIds.length != 0 && reduceIds.length == chunkIds.length);
    }

    @Override
    public boolean hasNext() {
      return reduceIdx < reduceIds.length && chunkIdx < chunkIds[reduceIdx].length;
    }

    @Override
    public ManagedBuffer next() {
      ManagedBuffer block = Preconditions.checkNotNull(mergeManager.getMergedBlockData(
        appId, shuffleId, shuffleMergeId, reduceIds[reduceIdx], chunkIds[reduceIdx][chunkIdx]));
      if (chunkIdx < chunkIds[reduceIdx].length - 1) {
        chunkIdx += 1;
      } else {
        chunkIdx = 0;
        reduceIdx += 1;
      }
      metrics.blockTransferRateBytes.mark(block.size());
      return block;
    }
  }

  @Override
  public void channelActive(TransportClient client) {
    metrics.activeConnections.inc();
    super.channelActive(client);
  }

  @Override
  public void channelInactive(TransportClient client) {
    metrics.activeConnections.dec();
    super.channelInactive(client);
  }

}
