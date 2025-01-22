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

import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
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

  private final boolean authEnabled;
  private final SecretKeyHolder secretKeyHolder;
  private final long registrationTimeoutMs;
  // Push based shuffle requires a comparable Id to distinguish the shuffle data among multiple
  // application attempts. This variable is derived from the String typed appAttemptId. If no
  // appAttemptId is set, the default comparableAppAttemptId is -1.
  private int comparableAppAttemptId = -1;

  /**
   * Creates an external shuffle client, with SASL optionally enabled. If SASL is not enabled,
   * then secretKeyHolder may be null.
   */
  public ExternalBlockStoreClient(
      TransportConf conf,
      SecretKeyHolder secretKeyHolder,
      boolean authEnabled,
      long registrationTimeoutMs) {
    this.transportConf = conf;
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
    TransportContext context = new TransportContext(
      transportConf, new NoOpRpcHandler(), true, true);
    List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
    if (authEnabled) {
      bootstraps.add(new AuthClientBootstrap(transportConf, appId, secretKeyHolder));
    }
    clientFactory = context.createClientFactory(bootstraps);
  }

  @Override
  public void setAppAttemptId(String appAttemptId) {
    super.setAppAttemptId(appAttemptId);
    setComparableAppAttemptId(appAttemptId);
  }

  private void setComparableAppAttemptId(String appAttemptId) {
    // For now, push based shuffle only supports running in YARN.
    // Application attemptId in YARN is integer and it can be safely parsed
    // to integer here. For the application attemptId from other cluster set up
    // which is not numeric, it needs to generate this comparableAppAttemptId
    // from the String typed appAttemptId through some other customized logic.
    try {
      this.comparableAppAttemptId = Integer.parseInt(appAttemptId);
    } catch (NumberFormatException e) {
      logger.warn("Push based shuffle requires comparable application attemptId, " +
        "but the appAttemptId {} cannot be parsed to Integer", e,
          MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, appAttemptId));
    }
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
      int maxRetries = transportConf.maxIORetries();
      RetryingBlockTransferor.BlockTransferStarter blockFetchStarter =
          (inputBlockId, inputListener) -> {
            // Unless this client is closed.
            if (clientFactory != null) {
              assert inputListener instanceof BlockFetchingListener :
                "Expecting a BlockFetchingListener, but got " + inputListener.getClass();
              TransportClient client = clientFactory.createClient(host, port, maxRetries > 0);
              new OneForOneBlockFetcher(client, appId, execId, inputBlockId,
                (BlockFetchingListener) inputListener, transportConf, downloadFileManager).start();
            } else {
              logger.info("This clientFactory was closed. Skipping further block fetch retries.");
            }
          };

      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockTransferor(transportConf, blockFetchStarter, blockIds, listener).start();
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
      BlockPushingListener listener) {
    checkInit();
    assert blockIds.length == buffers.length : "Number of block ids and buffers do not match.";

    Map<String, ManagedBuffer> buffersWithId = new HashMap<>();
    for (int i = 0; i < blockIds.length; i++) {
      buffersWithId.put(blockIds[i], buffers[i]);
    }
    logger.debug("Push {} shuffle blocks to {}:{}", blockIds.length, host, port);
    try {
      RetryingBlockTransferor.BlockTransferStarter blockPushStarter =
          (inputBlockId, inputListener) -> {
            if (clientFactory != null) {
              assert inputListener instanceof BlockPushingListener :
                "Expecting a BlockPushingListener, but got " + inputListener.getClass();
              TransportClient client = clientFactory.createClient(host, port);
              new OneForOneBlockPusher(client, appId, comparableAppAttemptId, inputBlockId,
                (BlockPushingListener) inputListener, buffersWithId).start();
            } else {
              logger.info("This clientFactory was closed. Skipping further block push retries.");
            }
          };
      int maxRetries = transportConf.maxIORetries();
      if (maxRetries > 0) {
        new RetryingBlockTransferor(
          transportConf, blockPushStarter, blockIds, listener, PUSH_ERROR_HANDLER).start();
      } else {
        blockPushStarter.createAndStart(blockIds, listener);
      }
    } catch (Exception e) {
      logger.error("Exception while beginning pushBlocks", e);
      for (String blockId : blockIds) {
        listener.onBlockPushFailure(blockId, e);
      }
    }
  }

  @Override
  public void finalizeShuffleMerge(
      String host,
      int port,
      int shuffleId,
      int shuffleMergeId,
      MergeFinalizerListener listener) {
    checkInit();
    try {
      TransportClient client = clientFactory.createClient(host, port);
      ByteBuffer finalizeShuffleMerge =
        new FinalizeShuffleMerge(
          appId, comparableAppAttemptId, shuffleId, shuffleMergeId).toByteBuffer();
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
      logger.error("Exception while sending finalizeShuffleMerge request to {}:{}", e,
        MDC.of(LogKeys.HOST$.MODULE$, host),
        MDC.of(LogKeys.PORT$.MODULE$, port));
      listener.onShuffleMergeFailure(e);
    }
  }

  @Override
  public void getMergedBlockMeta(
      String host,
      int port,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      MergedBlocksMetaListener listener) {
    checkInit();
    logger.debug("Get merged blocks meta from {}:{} for shuffleId {} shuffleMergeId {}"
      + " reduceId {}", host, port, shuffleId, shuffleMergeId, reduceId);
    try {
      TransportClient client = clientFactory.createClient(host, port);
      client.sendMergedBlockMetaReq(appId, shuffleId, shuffleMergeId, reduceId,
        new MergedBlockMetaResponseCallback() {
          @Override
          public void onSuccess(int numChunks, ManagedBuffer buffer) {
            logger.trace("Successfully got merged block meta for shuffleId {} shuffleMergeId {}"
              + " reduceId {}", shuffleId, shuffleMergeId, reduceId);
            listener.onSuccess(shuffleId, shuffleMergeId, reduceId,
              new MergedBlockMeta(numChunks, buffer));
          }

          @Override
          public void onFailure(Throwable e) {
            listener.onFailure(shuffleId, shuffleMergeId, reduceId, e);
          }
        });
    } catch (Exception e) {
      listener.onFailure(shuffleId, shuffleMergeId, reduceId, e);
    }
  }

  @Override
  public boolean removeShuffleMerge(String host, int port, int shuffleId, int shuffleMergeId) {
    checkInit();
    try {
      TransportClient client = clientFactory.createClient(host, port);
      client.send(
          new RemoveShuffleMerge(appId, comparableAppAttemptId, shuffleId, shuffleMergeId)
              .toByteBuffer());
      // TODO(SPARK-42025): Add some error logs for RemoveShuffleMerge RPC
    } catch (Exception e) {
      logger.debug("Exception while sending RemoveShuffleMerge request to {}:{}",
          host, port, e);
      return false;
    }
    return true;
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
          logger.warn("Error trying to remove blocks {} via external shuffle service from " +
            "executor: {}", t,
            MDC.of(LogKeys.BLOCK_IDS$.MODULE$, Arrays.toString(blockIds)),
            MDC.of(LogKeys.EXECUTOR_ID$.MODULE$, execId));
          numRemovedBlocksFuture.complete(0);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.warn("Error trying to remove blocks {} via external shuffle service from " +
          "executor: {}", e, MDC.of(LogKeys.BLOCK_IDS$.MODULE$, Arrays.toString(blockIds)),
          MDC.of(LogKeys.EXECUTOR_ID$.MODULE$, execId));
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
