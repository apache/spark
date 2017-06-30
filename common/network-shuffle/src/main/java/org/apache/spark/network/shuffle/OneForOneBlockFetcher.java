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
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.TransportConf;

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
  private final OpenBlocks openMessage;
  private final String[] blockIds;
  private final BlockFetchingListener listener;
  private TransportConf transportConf = null;
  private File[] shuffleFiles = null;

  private StreamHandle streamHandle = null;

  public OneForOneBlockFetcher(
      TransportClient client,
      String appId,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      TransportConf transportConf,
      File[] shuffleFiles) {
    this.client = client;
    this.openMessage = new OpenBlocks(appId, execId, blockIds);
    this.blockIds = blockIds;
    this.listener = listener;
    this.transportConf = transportConf;
    if (shuffleFiles != null) {
      this.shuffleFiles = shuffleFiles;
      assert this.shuffleFiles.length == blockIds.length:
        "Number of shuffle files should equal to blocks";
    }
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
  private class ChunkCallback implements ChunkReceivedCallback {

    private boolean toDisk;
    private File file;

    public ChunkCallback(boolean toDisk, File file) {
      this.toDisk = toDisk;
      this.file = file;
    }

    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      if (!(buffer instanceof FileSegmentManagedBuffer) && toDisk) {
        try {
          listener.onBlockFetchSuccess(blockIds[chunkIndex],
            FileSegmentManagedBuffer.fromManagedBuffer(buffer, transportConf, file));
        } catch (Exception e) {
          listener.onBlockFetchFailure(blockIds[chunkIndex], e);
        }
      } else {
        listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
      }
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
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }

    client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          for (int i = 0; i < streamHandle.numChunks; i++) {
            if (shuffleFiles != null) {
              client.fetchChunk(streamHandle.streamId, i, new ChunkCallback(true, shuffleFiles[i]));
            } else {
              client.fetchChunk(streamHandle.streamId, i, new ChunkCallback(false, null));
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
}
