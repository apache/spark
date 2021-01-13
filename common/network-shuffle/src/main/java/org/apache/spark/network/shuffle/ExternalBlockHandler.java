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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId;
import org.apache.spark.network.shuffle.protocol.*;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportConf;

/**
 * RPC Handler for a server which can serve both RDD blocks and shuffle blocks from outside
 * of an Executor process.
 *
 * Handles registering executors and opening shuffle or disk persisted RDD blocks from them.
 * Blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one block.
 */
public class ExternalBlockHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(ExternalBlockHandler.class);

  @VisibleForTesting
  final ExternalShuffleBlockResolver blockManager;
  private final OneForOneStreamManager streamManager;
  private final ShuffleMetrics metrics;
  private final MergedShuffleFileManager mergeManager;

  public ExternalBlockHandler(TransportConf conf, File registeredExecutorFile)
    throws IOException {
    this(new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, registeredExecutorFile),
      new NoOpMergedShuffleFileManager(conf));
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
    this(streamManager, blockManager, new NoOpMergedShuffleFileManager(null));
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
    if (msgObj instanceof FetchShuffleBlocks || msgObj instanceof OpenBlocks) {
      final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
      try {
        int numBlockIds;
        long streamId;
        if (msgObj instanceof FetchShuffleBlocks) {
          FetchShuffleBlocks msg = (FetchShuffleBlocks) msgObj;
          checkAuth(client, msg.appId);
          numBlockIds = 0;
          if (msg.batchFetchEnabled) {
            numBlockIds = msg.mapIds.length;
          } else {
            for (int[] ids: msg.reduceIds) {
              numBlockIds += ids.length;
            }
          }
          streamId = streamManager.registerStream(client.getClientId(),
            new ShuffleManagedBufferIterator(msg), client.getChannel());
        } else {
          // For the compatibility with the old version, still keep the support for OpenBlocks.
          OpenBlocks msg = (OpenBlocks) msgObj;
          numBlockIds = msg.blockIds.length;
          checkAuth(client, msg.appId);
          streamId = streamManager.registerStream(client.getClientId(),
            new ManagedBufferIterator(msg), client.getChannel());
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
      Map<String, String[]> localDirs = blockManager.getLocalDirs(msg.appId, msg.execIds);
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
          + "for application %s shuffle %d", msg.appId, msg.shuffleId), e);
      } finally {
        responseDelayContext.stop();
      }
    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
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

  /**
   * Register an (application, executor) with the given shuffle info.
   *
   * The "re-" is meant to highlight the intended use of this method -- when this service is
   * restarted, this is used to restore the state of executors from before the restart.  Normal
   * registration will happen via a message handled in receive()
   *
   * @param appExecId
   * @param executorInfo
   */
  public void reregisterExecutor(AppExecId appExecId, ExecutorShuffleInfo executorInfo) {
    blockManager.registerExecutor(appExecId.appId, appExecId.execId, executorInfo);
  }

  public void close() {
    blockManager.close();
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
    private final Timer openBlockRequestLatencyMillis = new Timer();
    // Time latency for executor registration latency in ms
    private final Timer registerExecutorRequestLatencyMillis = new Timer();
    // Time latency for processing finalize shuffle merge request latency in ms
    private final Timer finalizeShuffleMergeLatencyMillis = new Timer();
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
      allMetrics.put("finalizeShuffleMergeLatencyMillis", finalizeShuffleMergeLatencyMillis);
      allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
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
      String[] blockId0Parts = blockIds[0].split("_");
      if (blockId0Parts.length == 4 && blockId0Parts[0].equals("shuffle")) {
        final int shuffleId = Integer.parseInt(blockId0Parts[1]);
        final int[] mapIdAndReduceIds = shuffleMapIdAndReduceIds(blockIds, shuffleId);
        size = mapIdAndReduceIds.length;
        blockDataForIndexFn = index -> blockManager.getBlockData(appId, execId, shuffleId,
          mapIdAndReduceIds[index], mapIdAndReduceIds[index + 1]);
      } else if (blockId0Parts.length == 3 && blockId0Parts[0].equals("rdd")) {
        final int[] rddAndSplitIds = rddAndSplitIds(blockIds);
        size = rddAndSplitIds.length;
        blockDataForIndexFn = index -> blockManager.getRddBlockData(appId, execId,
          rddAndSplitIds[index], rddAndSplitIds[index + 1]);
      } else {
        throw new IllegalArgumentException("Unexpected block id format: " + blockIds[0]);
      }
    }

    private int[] rddAndSplitIds(String[] blockIds) {
      final int[] rddAndSplitIds = new int[2 * blockIds.length];
      for (int i = 0; i < blockIds.length; i++) {
        String[] blockIdParts = blockIds[i].split("_");
        if (blockIdParts.length != 3 || !blockIdParts[0].equals("rdd")) {
          throw new IllegalArgumentException("Unexpected RDD block id format: " + blockIds[i]);
        }
        rddAndSplitIds[2 * i] = Integer.parseInt(blockIdParts[1]);
        rddAndSplitIds[2 * i + 1] = Integer.parseInt(blockIdParts[2]);
      }
      return rddAndSplitIds;
    }

    private int[] shuffleMapIdAndReduceIds(String[] blockIds, int shuffleId) {
      final int[] mapIdAndReduceIds = new int[2 * blockIds.length];
      for (int i = 0; i < blockIds.length; i++) {
        String[] blockIdParts = blockIds[i].split("_");
        if (blockIdParts.length != 4 || !blockIdParts[0].equals("shuffle")) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[i]);
        }
        if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
          throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
            ", got:" + blockIds[i]);
        }
        mapIdAndReduceIds[2 * i] = Integer.parseInt(blockIdParts[2]);
        mapIdAndReduceIds[2 * i + 1] = Integer.parseInt(blockIdParts[3]);
      }
      return mapIdAndReduceIds;
    }

    @Override
    public boolean hasNext() {
      return index < size;
    }

    @Override
    public ManagedBuffer next() {
      final ManagedBuffer block = blockDataForIndexFn.apply(index);
      index += 2;
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
    }

    @Override
    public boolean hasNext() {
      // mapIds.length must equal to reduceIds.length, and the passed in FetchShuffleBlocks
      // must have non-empty mapIds and reduceIds, see the checking logic in
      // OneForOneBlockFetcher.
      assert(mapIds.length != 0 && mapIds.length == reduceIds.length);
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
      } else {
        assert(reduceIds[mapIdx].length == 2);
        block = blockManager.getContinuousBlocksData(appId, execId, shuffleId, mapIds[mapIdx],
          reduceIds[mapIdx][0], reduceIds[mapIdx][1]);
        mapIdx += 1;
      }
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }

  /**
   * Dummy implementation of merged shuffle file manager. Suitable for when push-based shuffle
   * is not enabled.
   *
   * @since 3.1.0
   */
  public static class NoOpMergedShuffleFileManager implements MergedShuffleFileManager {

    // This constructor is needed because we use this constructor to instantiate an implementation
    // of MergedShuffleFileManager using reflection.
    // See YarnShuffleService#newMergedShuffleFileManagerInstance.
    public NoOpMergedShuffleFileManager(TransportConf transportConf) {}

    @Override
    public StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public void registerExecutor(String appId, ExecutorShuffleInfo executorInfo) {
      // No-Op. Do nothing.
    }

    @Override
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
      // No-Op. Do nothing.
    }

    @Override
    public ManagedBuffer getMergedBlockData(
        String appId, int shuffleId, int reduceId, int chunkId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public MergedBlockMeta getMergedBlockMeta(String appId, int shuffleId, int reduceId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public String[] getMergedBlockDirs(String appId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
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
