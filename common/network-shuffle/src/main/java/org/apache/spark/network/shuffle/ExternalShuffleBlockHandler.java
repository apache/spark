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
import java.util.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
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
 * RPC Handler for a server which can serve shuffle blocks from outside of an Executor process.
 *
 * Handles registering executors and opening shuffle blocks from them. Shuffle blocks are registered
 * with the "one-for-one" strategy, meaning each Transport-layer Chunk is equivalent to one Spark-
 * level shuffle block.
 */
public class ExternalShuffleBlockHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleBlockHandler.class);

  @VisibleForTesting
  final ExternalShuffleBlockResolver blockManager;
  private final OneForOneStreamManager streamManager;
  private final ShuffleMetrics metrics;

  public ExternalShuffleBlockHandler(TransportConf conf, File registeredExecutorFile)
    throws IOException {
    this(new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, registeredExecutorFile));
  }

  /** Enables mocking out the StreamManager and BlockManager. */
  @VisibleForTesting
  public ExternalShuffleBlockHandler(
      OneForOneStreamManager streamManager,
      ExternalShuffleBlockResolver blockManager) {
    this.metrics = new ShuffleMetrics();
    this.streamManager = streamManager;
    this.blockManager = blockManager;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
    handleMessage(msgObj, client, callback);
  }

  protected void handleMessage(
      BlockTransferMessage msgObj,
      TransportClient client,
      RpcResponseCallback callback) {
    if (msgObj instanceof OpenBlocks) {
      final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
      try {
        OpenBlocks msg = (OpenBlocks) msgObj;
        checkAuth(client, msg.appId);
        ManagedBufferIterator blocksIter = new ManagedBufferIterator(
          msg.appId, msg.execId, msg.blockIds, msg.fetchContinuousShuffleBlocksInBatch);
        long streamId = streamManager.registerStream(client.getClientId(), blocksIter);
        if (logger.isTraceEnabled()) {
          logger.trace("Registered streamId {} with {} buffers for client {} from host {}",
                       streamId,
                       blocksIter.getNumChunks(),
                       client.getClientId(),
                       getRemoteAddress(client.getChannel()));
        }
        callback.onSuccess(new StreamHandle(streamId, blocksIter.getNumChunks()).toByteBuffer());
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
        callback.onSuccess(ByteBuffer.wrap(new byte[0]));
      } finally {
        responseDelayContext.stop();
      }

    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
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
    // Block transfer rate in byte per second
    private final Meter blockTransferRateBytes = new Meter();
    // Number of active connections to the shuffle service
    private Counter activeConnections = new Counter();
    // Number of registered connections to the shuffle service
    private Counter registeredConnections = new Counter();

    public ShuffleMetrics() {
      allMetrics = new HashMap<>();
      allMetrics.put("openBlockRequestLatencyMillis", openBlockRequestLatencyMillis);
      allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis);
      allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
      allMetrics.put("registeredExecutorsSize",
                     (Gauge<Integer>) () -> blockManager.getRegisteredExecutorsSize());
      allMetrics.put("numActiveConnections", activeConnections);
      allMetrics.put("numRegisteredConnections", registeredConnections);
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return allMetrics;
    }
  }

  private boolean isShuffleBlock(String[] blockIdParts) {
    // length == 4: ShuffleBlockId
    // length == 5: ShuffleBlockBatchId
    return (blockIdParts.length == 4 || blockIdParts.length == 5) &&
      blockIdParts[0].equals("shuffle");
  }

  private class ManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int index = 0;
    private final String appId;
    private final String execId;
    private final int shuffleId;
    // An array containing mapId, reduceId and numBlocks tuple
    private int[] shuffleBlockIds;

    private String[] getBlockIdParts(String blockId) {
      String[] blockIdParts = blockId.split("_");
      if (!isShuffleBlock(blockIdParts)) {
        throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockId);
      }
      if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
        throw new IllegalArgumentException("Expected shuffleId=" + shuffleId + ", got:" + blockId);
      }
      return blockIdParts;
    }

    private void mergeShuffleBlockIds(String[] blockIds) {
      ArrayList<int[]> originBlocks = new ArrayList<>(blockIds.length);
      for (String blockId : blockIds) {
        String[] blockIdParts = getBlockIdParts(blockId);
        int[] parsedBlockId =
          { Integer.parseInt(blockIdParts[2]), Integer.parseInt(blockIdParts[3]) };
        originBlocks.add(parsedBlockId);
      }

      int[] shuffleBlockBatchIds = new int[3 * blockIds.length];

      int capacity = 0;
      int prevIdx = 0;
      int[] prevBlock = originBlocks.get(0);
      for (int i = 1; i <= blockIds.length; i++) {
        if (i == blockIds.length || originBlocks.get(i)[0] != prevBlock[0]) {
          prevBlock = originBlocks.get(prevIdx);
          shuffleBlockBatchIds[3 * capacity] = prevBlock[0];
          shuffleBlockBatchIds[3 * capacity + 1] = prevBlock[1];
          shuffleBlockBatchIds[3 * capacity + 2] = originBlocks.get(i - 1)[1] - prevBlock[1] + 1;
          prevIdx = i;
          capacity++;
        }
      }

      this.shuffleBlockIds = Arrays.copyOfRange(shuffleBlockBatchIds, 0, capacity * 3);
    }

    ManagedBufferIterator(
        String appId,
        String execId,
        String[] blockIds,
        boolean fetchContinuousShuffleBlocksInBatch) {
      this.appId = appId;
      this.execId = execId;
      String[] blockId0Parts = blockIds[0].split("_");
      if (!isShuffleBlock(blockId0Parts)) {
        throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[0]);
      }
      this.shuffleId = Integer.parseInt(blockId0Parts[1]);
      // When fetch again happens (corrupted), openBlocks could contain ShuffleBlockBatchId
      if (fetchContinuousShuffleBlocksInBatch && blockId0Parts.length == 4) {
        mergeShuffleBlockIds(blockIds);
      } else {
        shuffleBlockIds = new int[3 * blockIds.length];
        for (int i = 0; i < blockIds.length; i++) {
          String[] blockIdParts = getBlockIdParts(blockIds[i]);
          shuffleBlockIds[3 * i] = Integer.parseInt(blockIdParts[2]);
          shuffleBlockIds[3 * i + 1] = Integer.parseInt(blockIdParts[3]);
          if (blockIdParts.length == 4) {
            shuffleBlockIds[3 * i + 2] = 1;
          } else {
            shuffleBlockIds[3 * i + 2] = Integer.parseInt(blockIdParts[4]);
          }
        }
      }
    }

    public int getNumChunks() {
      return shuffleBlockIds.length / 3;
    }

    @Override
    public boolean hasNext() {
      return index < shuffleBlockIds.length;
    }

    @Override
    public ManagedBuffer next() {
      final ManagedBuffer block = blockManager.getBlockData(appId, execId, shuffleId,
        shuffleBlockIds[index], shuffleBlockIds[index + 1], shuffleBlockIds[index + 2]);
      index += 3;
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
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
