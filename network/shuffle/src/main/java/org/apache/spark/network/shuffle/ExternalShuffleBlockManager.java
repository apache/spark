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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.JavaUtils;

/**
 * Manages converting shuffle BlockIds into physical segments of local files, from a process outside
 * of Executors. Each Executor must register its own configuration about where it stores its files
 * (local dirs) and how (shuffle manager). The logic for retrieval of individual files is replicated
 * from Spark's FileShuffleBlockManager and IndexShuffleBlockManager.
 *
 * Executors with shuffle file consolidation are not currently supported, as the index is stored in
 * the Executor's memory, unlike the IndexShuffleBlockManager.
 */
public class ExternalShuffleBlockManager {
  private final Logger logger = LoggerFactory.getLogger(ExternalShuffleBlockManager.class);

  // Map from "appId-execId" to the executor's configuration.
  private final ConcurrentHashMap<String, ExecutorShuffleInfo> executors =
    new ConcurrentHashMap<String, ExecutorShuffleInfo>();

  // Returns an id suitable for a single executor within a single application.
  private String getAppExecId(String appId, String execId) {
    return appId + "-" + execId;
  }

  /** Registers a new Executor with all the configuration we need to find its shuffle files. */
  public void registerExecutor(
      String appId,
      String execId,
      ExecutorShuffleInfo executorInfo) {
    String fullId = getAppExecId(appId, execId);
    logger.info("Registered executor {} with {}", fullId, executorInfo);
    executors.put(fullId, executorInfo);
  }

  /**
   * Obtains a FileSegmentManagedBuffer from a shuffle block id. We expect the blockId has the
   * format "shuffle_ShuffleId_MapId_ReduceId" (from ShuffleBlockId), and additionally make
   * assumptions about how the hash and sort based shuffles store their data.
   */
  public ManagedBuffer getBlockData(String appId, String execId, String blockId) {
    String[] blockIdParts = blockId.split("_");
    if (blockIdParts.length < 4) {
      throw new IllegalArgumentException("Unexpected block id format: " + blockId);
    } else if (!blockIdParts[0].equals("shuffle")) {
      throw new IllegalArgumentException("Expected shuffle block id, got: " + blockId);
    }
    int shuffleId = Integer.parseInt(blockIdParts[1]);
    int mapId = Integer.parseInt(blockIdParts[2]);
    int reduceId = Integer.parseInt(blockIdParts[3]);

    ExecutorShuffleInfo executor = executors.get(getAppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }

    if ("org.apache.spark.shuffle.hash.HashShuffleManager".equals(executor.shuffleManager)) {
      return getHashBasedShuffleBlockData(executor, blockId);
    } else if ("org.apache.spark.shuffle.sort.SortShuffleManager".equals(executor.shuffleManager)) {
      return getSortBasedShuffleBlockData(executor, shuffleId, mapId, reduceId);
    } else {
      throw new UnsupportedOperationException(
        "Unsupported shuffle manager: " + executor.shuffleManager);
    }
  }

  /**
   * Hash-based shuffle data is simply stored as one file per block.
   * This logic is from FileShuffleBlockManager.
   */
  // TODO: Support consolidated hash shuffle files
  private ManagedBuffer getHashBasedShuffleBlockData(ExecutorShuffleInfo executor, String blockId) {
    File shuffleFile = getFile(executor.localDirs, executor.subDirsPerLocalDir, blockId);
    return new FileSegmentManagedBuffer(shuffleFile, 0, shuffleFile.length());
  }

  /**
   * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
   * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockManager,
   * and the block id format is from ShuffleDataBlockId and ShuffleIndexBlockId.
   */
  private ManagedBuffer getSortBasedShuffleBlockData(
    ExecutorShuffleInfo executor, int shuffleId, int mapId, int reduceId) {
    File indexFile = getFile(executor.localDirs, executor.subDirsPerLocalDir,
      "shuffle_" + shuffleId + "_" + mapId + "_0.index");

    DataInputStream in = null;
    try {
      in = new DataInputStream(new FileInputStream(indexFile));
      in.skipBytes(reduceId * 8);
      long offset = in.readLong();
      long nextOffset = in.readLong();
      return new FileSegmentManagedBuffer(
        getFile(executor.localDirs, executor.subDirsPerLocalDir,
          "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
        offset,
        nextOffset - offset);
    } catch (IOException e) {
      throw new RuntimeException("Failed to open file: " + indexFile, e);
    } finally {
      if (in != null) {
        JavaUtils.closeQuietly(in);
      }
    }
  }

  /**
   * Hashes a filename into the corresponding local directory, in a manner consistent with
   * Spark's DiskBlockManager.getFile().
   */
  @VisibleForTesting
  static File getFile(String[] localDirs, int subDirsPerLocalDir, String filename) {
    int hash = JavaUtils.nonNegativeHash(filename);
    String localDir = localDirs[hash % localDirs.length];
    int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
    return new File(new File(localDir, String.format("%02x", subDirId)), filename);
  }

  /** For testing, clears all registered executors. */
  @VisibleForTesting
  void clearRegisteredExecutors() {
    executors.clear();
  }
}
