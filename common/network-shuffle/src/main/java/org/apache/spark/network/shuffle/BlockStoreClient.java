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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.GetLocalDirsForExecutors;
import org.apache.spark.network.shuffle.protocol.LocalDirsForExecutors;

/**
 * Provides an interface for reading both shuffle files and RDD blocks, either from an Executor
 * or external service.
 */
public abstract class BlockStoreClient implements Closeable {
  protected final Logger logger = LoggerFactory.getLogger(this.getClass());

  protected volatile TransportClientFactory clientFactory;
  protected String appId;

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
            logger.warn("Error while trying to get the host local dirs for " +
              Arrays.toString(getLocalDirsMessage.execIds), t.getCause());
            hostLocalDirsCompletable.completeExceptionally(t);
          }
        }

        @Override
        public void onFailure(Throwable t) {
          logger.warn("Error while trying to get the host local dirs for " +
            Arrays.toString(getLocalDirsMessage.execIds), t.getCause());
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
      BlockFetchingListener listener) {
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
   * @param listener the listener to receive MergeStatuses
   *
   * @since 3.1.0
   */
  public void finalizeShuffleMerge(
      String host,
      int port,
      int shuffleId,
      MergeFinalizerListener listener) {
    throw new UnsupportedOperationException();
  }
}
