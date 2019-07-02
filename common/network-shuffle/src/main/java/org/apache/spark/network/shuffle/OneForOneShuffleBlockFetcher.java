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

import com.google.common.primitives.Ints;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.util.TransportConf;

import java.util.ArrayList;
import java.util.HashMap;

public class OneForOneShuffleBlockFetcher extends OneForOneBlockFetcher {
  private final int shuffleGenerationId;

  public OneForOneShuffleBlockFetcher(
      TransportClient client,
      String appId,
      String execId,
      int shuffleGenerationId,
      String[] blockIds,
      BlockFetchingListener listener,
      TransportConf transportConf,
      DownloadFileManager downloadFileManager) {
    super(client, appId, execId, blockIds, listener, transportConf, downloadFileManager);
    this.shuffleGenerationId = shuffleGenerationId;
  }

  /**
   * Create the corresponding message for this fetcher.
   * For shuffle blocks, we choose different message base on whether to use the old protocol.
   * If `spark.shuffle.useOldFetchProtocol` set to true, we use OpenBlocks for shuffle blocks.
   * Otherwise, we analyze the pass in blockIds and create FetchShuffleBlocks message.
   * The blockIds has been sorted by mapId and reduceId. It's produced in
   * org.apache.spark.MapOutputTracker.convertMapStatuses.
   */
  @Override
  public BlockTransferMessage createBlockTransferMessage() {
    if (transportConf.useOldFetchProtocol()) {
      return new OpenBlocks(appId, execId, blockIds);
    } else {
      int shuffleId = splitBlockId(blockIds[0])[0];
      HashMap<Integer, ArrayList<Integer>> mapIdToReduceIds = new HashMap<>();
      for (String blockId : blockIds) {
        int[] blockIdParts = splitBlockId(blockId);
        if (blockIdParts[0] != shuffleId) {
          throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
            ", got:" + blockId);
        }
        int mapId = blockIdParts[1];
        if (!mapIdToReduceIds.containsKey(mapId)) {
          mapIdToReduceIds.put(mapId, new ArrayList<>());
        }
        mapIdToReduceIds.get(mapId).add(blockIdParts[2]);
      }
      int[] mapIds = Ints.toArray(mapIdToReduceIds.keySet());
      int[][] reduceIdArr = new int[mapIds.length][];
      for (int i = 0; i < mapIds.length; i++) {
        reduceIdArr[i] = Ints.toArray(mapIdToReduceIds.get(mapIds[i]));
      }
      return new FetchShuffleBlocks(
        appId, execId, shuffleId, shuffleGenerationId, mapIds, reduceIdArr);
    }
  }

  /** Split the shuffleBlockId and return shuffleId, mapId and reduceId. */
  private int[] splitBlockId(String blockId) {
    String[] blockIdParts = blockId.split("_");
    if (blockIdParts.length != 4 || !blockIdParts[0].equals("shuffle")) {
      throw new IllegalArgumentException(
          "Unexpected shuffle block id format: " + blockId);
    }
    return new int[] {
      Integer.parseInt(blockIdParts[1]),
      Integer.parseInt(blockIdParts[2]),
      Integer.parseInt(blockIdParts[3])
    };
  }
}
