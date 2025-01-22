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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.codahale.metrics.MetricSet;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.checksum.Cause;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;

/**
 * Provides an interface for reading both shuffle files and RDD blocks, either from an Executor
 * or external service.
 */
public abstract class BlockStoreClient implements Closeable {
  protected final SparkLogger logger = SparkLoggerFactory.getLogger(this.getClass());

  protected volatile TransportClientFactory clientFactory;
  protected String appId;
  // Store the application attemptId
  private String appAttemptId;
  protected TransportConf transportConf;

  /**
   * Send the diagnosis request for the corrupted shuffle block to the server.
   *
   * @param host the host of the remote node.
   * @param port the port of the remote node.
   * @param execId the executor id.
   * @param shuffleId the shuffleId of the corrupted shuffle block
   * @param mapId the mapId of the corrupted shuffle block
   * @param reduceId the reduceId of the corrupted shuffle block
   * @param checksum the shuffle checksum which calculated at client side for the corrupted
   *                 shuffle block
   * @return The cause of the shuffle block corruption
   */
  public Cause diagnoseCorruption(
      String host,
      int port,
      String execId,
      int shuffleId,
      long mapId,
      int reduceId,
      long checksum,
      String algorithm) {
    try {
      TransportClient client = clientFactory.createClient(host, port);
      ByteBuffer response = client.sendRpcSync(
        new DiagnoseCorruption(appId, execId, shuffleId, mapId, reduceId, checksum, algorithm)
          .toByteBuffer(),
        transportConf.connectionTimeoutMs()
      );
      CorruptionCause cause =
        (CorruptionCause) BlockTransferMessage.Decoder.fromByteBuffer(response);
      return cause.cause;
    } catch (Exception e) {
      logger.warn("Failed to get the corruption cause.");
      return Cause.UNKNOWN_ISSUE;
    }
  }

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   *
   * @param host the host of the remote node.
   * @param port the port of the remote node.
   * @param execId the executor id.
   * @param blockIds block ids to fetch.
   * @param listener the listener to receive block fetching status.
   * @param downloadFileManager DownloadFileManager to create and clean temp files.
   *                        If it's not <code>null</code>, the remote blocks will be streamed
   *                        into temp shuffle files to reduce the memory usage, otherwise,
   *                        they will be kept in memory.
   */
  public abstract void fetchBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      DownloadFileManager downloadFileManager);

  /**
   * Get the shuffle MetricsSet from BlockStoreClient, this will be used in MetricsSystem to
   * get the Shuffle related metrics.
   */
  public MetricSet shuffleMetrics() {
    // Return an empty MetricSet by default.
    return () -> Collections.emptyMap();
  }

  protected void checkInit() {
    assert appId != null : "Called before init()";
  }

  // Set the application attemptId
  public void setAppAttemptId(String appAttemptId) {
    this.appAttemptId = appAttemptId;
  }

  // Get the application attemptId
  public String getAppAttemptId() {
    return this.appAttemptId;
  }

  /**
   * Request the local disk directories for executors which are located at the same host with
   * the current BlockStoreClient(it can be ExternalBlockStoreClient or NettyBlockTransferService).
   *
   * @param host the host of BlockManager or ExternalShuffleService. It should be the same host
   *             with current BlockStoreClient.
   * @param port the port of BlockManager or ExternalShuffleService.
   * @param execIds a collection of executor Ids, which specifies the target executors that we
   *                want to get their local directories. There could be multiple executor Ids if
   *                BlockStoreClient is implemented by ExternalBlockStoreClient since the request
   *                handler, ExternalShuffleService, can serve multiple executors on the same node.
   *                Or, only one executor Id if BlockStoreClient is implemented by
   *                NettyBlockTransferService.
   * @param hostLocalDirsCompletable a CompletableFuture which contains a map from executor Id
   *                                 to its local directories if the request handler replies
   *                                 successfully. Otherwise, it contains a specific error.
   */
  public void getHostLocalDirs(
      String host,
      int port,
      String[] execIds,
      CompletableFuture<Map<String, String[]>> hostLocalDirsCompletable) {
    checkInit();
    GetLocalDirsForExecutors getLocalDirsMessage = new GetLocalDirsForExecutors(appId, execIds);
    try {
      TransportClient client = clientFactory.createClient(host, port);
      client.sendRpc(getLocalDirsMessage.toByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          try {
            BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(response);
            hostLocalDirsCompletable.complete(
              ((LocalDirsForExecutors) msgObj).getLocalDirsByExec());
          } catch (Throwable t) {
            logger.warn("Error while trying to get the host local dirs for {}", t.getCause(),
              MDC.of(LogKeys.EXECUTOR_IDS$.MODULE$, Arrays.toString(getLocalDirsMessage.execIds)));
            hostLocalDirsCompletable.completeExceptionally(t);
          }
        }

        @Override
        public void onFailure(Throwable t) {
          logger.warn("Error while trying to get the host local dirs for {}", t.getCause(),
            MDC.of(LogKeys.EXECUTOR_IDS$.MODULE$, Arrays.toString(getLocalDirsMessage.execIds)));
          hostLocalDirsCompletable.completeExceptionally(t);
        }
      });
    } catch (IOException | InterruptedException e) {
      hostLocalDirsCompletable.completeExceptionally(e);
    }
  }

  /**
   * Push a sequence of shuffle blocks in a best-effort manner to a remote node asynchronously.
   * These shuffle blocks, along with blocks pushed by other clients, will be merged into
   * per-shuffle partition merged shuffle files on the destination node.
   *
   * @param host the host of the remote node.
   * @param port the port of the remote node.
   * @param blockIds block ids to be pushed
   * @param buffers buffers to be pushed
   * @param listener the listener to receive block push status.
   *
   * @since 3.1.0
   */
  public void pushBlocks(
      String host,
      int port,
      String[] blockIds,
      ManagedBuffer[] buffers,
      BlockPushingListener listener) {
    throw new UnsupportedOperationException();
  }

  /**
   * Invoked by Spark driver to notify external shuffle services to finalize the shuffle merge
   * for a given shuffle. This allows the driver to start the shuffle reducer stage after properly
   * finishing the shuffle merge process associated with the shuffle mapper stage.
   *
   * @param host host of shuffle server
   * @param port port of shuffle server.
   * @param shuffleId shuffle ID of the shuffle to be finalized
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param listener the listener to receive MergeStatuses
   *
   * @since 3.1.0
   */
  public void finalizeShuffleMerge(
      String host,
      int port,
      int shuffleId,
      int shuffleMergeId,
      MergeFinalizerListener listener) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the meta information of a merged block from the remote shuffle service.
   *
   * @param host the host of the remote node.
   * @param port the port of the remote node.
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param listener the listener to receive chunk counts.
   *
   * @since 3.2.0
   */
  public void getMergedBlockMeta(
      String host,
      int port,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      MergedBlocksMetaListener listener) {
    throw new UnsupportedOperationException();
  }

  /**
   * Remove the shuffle merge data in shuffle services
   *
   * @param host the host of the remote node.
   * @param port the port of the remote node.
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffle merge id.
   *
   * @since 3.4.0
   */
  public boolean removeShuffleMerge(String host, int port, int shuffleId, int shuffleMergeId) {
    throw new UnsupportedOperationException();
  }
}
