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

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.network.shuffle.StandaloneShuffleMessages.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.JavaUtils;

/**
 * RPC Handler for the standalone shuffle server.
 * Handles registering executors and opening shuffle blocks from them. Shuffle blocks are registered
 * with the "one-for-one" strategy, meaning each Transport-layer Chunk is equivalent to one Spark-
 * level shuffle block.
 */
public class StandaloneShuffleBlockHandler implements RpcHandler {
  private final Logger logger = LoggerFactory.getLogger(StandaloneShuffleBlockHandler.class);

  private final StandaloneShuffleBlockManager blockManager;
  private final OneForOneStreamManager streamManager;

  public StandaloneShuffleBlockHandler() {
    this(new OneForOneStreamManager(), new StandaloneShuffleBlockManager());
  }

  /** Enables mocking out the StreamManager and BlockManager. */
  @VisibleForTesting
  StandaloneShuffleBlockHandler(
      OneForOneStreamManager streamManager,
      StandaloneShuffleBlockManager blockManager) {
    this.streamManager = streamManager;
    this.blockManager = blockManager;
  }

  @Override
  public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
    Object msgObj = JavaUtils.deserialize(message);

    logger.trace("Received message: " + msgObj);

    if (msgObj instanceof OpenShuffleBlocks) {
      OpenShuffleBlocks msg = (OpenShuffleBlocks) msgObj;
      List<ManagedBuffer> blocks = Lists.newArrayList();

      for (String blockId : msg.blockIds) {
        blocks.add(blockManager.getBlockData(msg.appId, msg.execId, blockId));
      }
      long streamId = streamManager.registerStream(blocks.iterator());
      logger.trace("Registered streamId {} with {} buffers", streamId, msg.blockIds.length);
      callback.onSuccess(JavaUtils.serialize(
        new ShuffleStreamHandle(streamId, msg.blockIds.length)));

    } else if (msgObj instanceof RegisterExecutor) {
      RegisterExecutor msg = (RegisterExecutor) msgObj;
      blockManager.registerExecutor(msg.appId, msg.execId, msg.executorConfig);
      callback.onSuccess(new byte[0]);

    } else {
      throw new UnsupportedOperationException(String.format(
        "Unexpected message: %s (class = %s)", msgObj, msgObj.getClass()));
    }
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  /** For testing, clears all executors registered with "RegisterExecutor". */
  @VisibleForTesting
  void clearRegisteredExecutors() {
    blockManager.clearRegisteredExecutors();
  }
}
