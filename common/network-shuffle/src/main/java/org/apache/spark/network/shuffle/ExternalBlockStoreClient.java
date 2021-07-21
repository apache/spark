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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import com.codahale.metrics.MetricSet;
import com.google.common.collect.Lists;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;

/**
 * Client for reading both RDD blocks and shuffle blocks which points to an external
 * (outside of executor) server. This is instead of reading blocks directly from other executors
 * (via BlockTransferService), which has the downside of losing the data if we lose the executors.
 */
public class ExternalBlockStoreClient extends BlockStoreClient {
  private static final ErrorHandler PUSH_ERROR_HANDLER = new ErrorHandler.BlockPushErrorHandler();

  private final TransportConf conf;
  private final boolean authEnabled;
  private final SecretKeyHolder secretKeyHolder;
  private final long registrationTimeoutMs;

  /**
   * Creates an external shuffle client, with SASL optionally enabled. If SASL is not enabled,
   * then secretKeyHolder may be null.
   */
  public ExternalBlockStoreClient(
      TransportConf conf,
      SecretKeyHolder secretKeyHolder,
      boolean authEnabled,
      long registrationTimeoutMs) {
    this.conf = conf;
    this.secretKeyHolder = secretKeyHolder;
    this.authEnabled = authEnabled;
    this.registrationTimeoutMs = registrationTimeoutMs;
  }

  /**
   * Initializes the BlockStoreClient, specifying this Executor's appId.
   * Must be called before any other method on the BlockStoreClient.
   */
  public void init(String appId) {
    this.appId = appId;
    TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true, true);
    List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
    if (authEnabled) {
      bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
    }
    clientFactory = context.createClientFactory(bootstraps);
  }

  @Override
  public void fetchBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      DownloadFileManager downloadFileManager) {
    checkInit();
    logger.debug("External shuffle fetch from {}:{} (executor id {})", host, port, execId);
    try {
      int maxRetries = conf.maxIORetries();
      RetryingBlockFetcher.BlockFetchStarter blockFetchStarter =
          (inputBlockId, inputListener) -> {
            // Unless this client is closed.
            if (clientFactory != null) {
              TransportClient client = clientFactory.createClient(host, port, maxRetries > 0);
              new OneForOneBlockFetcher(client, appId, execId,
                inputBlockId, inputListener, conf, downloadFileManager).start();
            } else {
              logger.info("This clientFactory was closed. Skipping further block fetch retries.");
            }
          };

      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(conf, blockFetchStarter, blockIds, listener).start();
      } else {
        blockFetchStarter.createAndStart(blockIds, listener);
      }
    } catch (Exception e) {
      logger.error("Exception while beginning fetchBlocks", e);
      for (String blockId : blockIds) {
        listener.onBlockFetchFailure(blockId, e);
      }
    }
  }

  @Override
  public void pushBlocks(
      String host,
      int port,
      String[] blockIds,
      ManagedBuffer[] buffers,
      BlockFetchingListener listener) {
    checkInit();
    assert blockIds.length == buffers.length : "Number of block ids and buffers do not match.";

    Map<String, ManagedBuffer> buffersWithId = new HashMap<>();
    for (int i = 0; i < blockIds.length; i++) {
      buffersWithId.put(blockIds[i], buffers[i]);
    }
    logger.debug("Push {} shuffle blocks to {}:{}", blockIds.length, host, port);
    try {
      RetryingBlockFetcher.BlockFetchStarter blockPushStarter =
          (inputBlockId, inputListener) -> {
            TransportClient client = clientFactory.createClient(host, port);
            new OneForOneBlockPusher(client, appId, conf.appAttemptId(), inputBlockId,
              inputListener, buffersWithId).start();
          };
      int maxRetries = conf.maxIORetries();
      if (maxRetries > 0) {
        new RetryingBlockFetcher(
          conf, blockPushStarter, blockIds, listener, PUSH_ERROR_HANDLER).start();
      } else {
        blockPushStarter.createAndStart(blockIds, listener);
      }
    } catch (Exception e) {
      logger.error("Exception while beginning pushBlocks", e);
      for (String blockId : blockIds) {
        listener.onBlockFetchFailure(blockId, e);
      }
    }
  }

  @Override
  public void finalizeShuffleMerge(
      String host,
      int port,
      int shuffleId,
      MergeFinalizerListener listener) {
    checkInit();
    try {
      TransportClient client = clientFactory.createClient(host, port);
      ByteBuffer finalizeShuffleMerge =
        new FinalizeShuffleMerge(appId, conf.appAttemptId(), shuffleId).toByteBuffer();
      client.sendRpc(finalizeShuffleMerge, new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          listener.onShuffleMergeSuccess(
            (MergeStatuses) BlockTransferMessage.Decoder.fromByteBuffer(response));
        }

        @Override
        public void onFailure(Throwable e) {
          listener.onShuffleMergeFailure(e);
        }
      });
    } catch (Exception e) {
      logger.error("Exception while sending finalizeShuffleMerge request to {}:{}",
        host, port, e);
      listener.onShuffleMergeFailure(e);
    }
  }

  @Override
  public void getMergedBlockMeta(
      String host,
      int port,
      int shuffleId,
      int reduceId,
      MergedBlocksMetaListener listener) {
    checkInit();
    logger.debug("Get merged blocks meta from {}:{} for shuffleId {} reduceId {}", host, port,
      shuffleId, reduceId);
    try {
      TransportClient client = clientFactory.createClient(host, port);
      client.sendMergedBlockMetaReq(appId, shuffleId, reduceId,
        new MergedBlockMetaResponseCallback() {
          @Override
          public void onSuccess(int numChunks, ManagedBuffer buffer) {
            logger.trace("Successfully got merged block meta for shuffleId {} reduceId {}",
              shuffleId, reduceId);
            listener.onSuccess(shuffleId, reduceId, new MergedBlockMeta(numChunks, buffer));
          }

          @Override
          public void onFailure(Throwable e) {
            listener.onFailure(shuffleId, reduceId, e);
          }
        });
    } catch (Exception e) {
      listener.onFailure(shuffleId, reduceId, e);
    }
  }

  @Override
  public MetricSet shuffleMetrics() {
    checkInit();
    return clientFactory.getAllMetrics();
  }

  /**
   * Registers this executor with an external shuffle server. This registration is required to
   * inform the shuffle server about where and how we store our shuffle files.
   *
   * @param host Host of shuffle server.
   * @param port Port of shuffle server.
   * @param execId This Executor's id.
   * @param executorInfo Contains all info necessary for the service to find our shuffle files.
   */
  public void registerWithShuffleServer(
      String host,
      int port,
      String execId,
      ExecutorShuffleInfo executorInfo) throws IOException, InterruptedException {
    checkInit();
    try (TransportClient client = clientFactory.createClient(host, port)) {
      ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();
      client.sendRpcSync(registerMessage, registrationTimeoutMs);
    }
  }

  public Future<Integer> removeBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds) throws IOException, InterruptedException {
    checkInit();
    CompletableFuture<Integer> numRemovedBlocksFuture = new CompletableFuture<>();
    ByteBuffer removeBlocksMessage = new RemoveBlocks(appId, execId, blockIds).toByteBuffer();
    final TransportClient client = clientFactory.createClient(host, port);
    client.sendRpc(removeBlocksMessage, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(response);
          numRemovedBlocksFuture.complete(((BlocksRemoved) msgObj).numRemovedBlocks);
        } catch (Throwable t) {
          logger.warn("Error trying to remove RDD blocks " + Arrays.toString(blockIds) +
            " via external shuffle service from executor: " + execId, t);
          numRemovedBlocksFuture.complete(0);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.warn("Error trying to remove RDD blocks " + Arrays.toString(blockIds) +
          " via external shuffle service from executor: " + execId, e);
        numRemovedBlocksFuture.complete(0);
      }
    });
    return numRemovedBlocksFuture;
  }

  @Override
  public void close() {
    checkInit();
    if (clientFactory != null) {
      clientFactory.close();
      clientFactory = null;
    }
  }
}
