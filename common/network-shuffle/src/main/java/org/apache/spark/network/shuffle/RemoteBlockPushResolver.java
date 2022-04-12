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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.server.BlockPushNonFatalFailure;
import org.apache.spark.network.server.BlockPushNonFatalFailure.ReturnCode;
import org.apache.spark.network.shuffle.protocol.BlockPushReturnCode;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * An implementation of {@link MergedShuffleFileManager} that provides the most essential shuffle
 * service processing logic to support push based shuffle.
 *
 * @since 3.1.0
 */
public class RemoteBlockPushResolver implements MergedShuffleFileManager {

  private static final Logger logger = LoggerFactory.getLogger(RemoteBlockPushResolver.class);

  public static final String MERGED_SHUFFLE_FILE_NAME_PREFIX = "shuffleMerged";
  public static final String SHUFFLE_META_DELIMITER = ":";
  public static final String MERGE_DIR_KEY = "mergeDir";
  public static final String ATTEMPT_ID_KEY = "attemptId";
  private static final int UNDEFINED_ATTEMPT_ID = -1;
  private static final ErrorHandler.BlockPushErrorHandler ERROR_HANDLER = createErrorHandler();
  // ByteBuffer to respond to client upon a successful merge of a pushed block
  private static final ByteBuffer SUCCESS_RESPONSE =
    new BlockPushReturnCode(ReturnCode.SUCCESS.id(), "").toByteBuffer().asReadOnlyBuffer();

  /**
   * A concurrent hashmap where the key is the applicationId, and the value includes
   * all the merged shuffle information for this application. AppShuffleInfo stores
   * the application attemptId, merged shuffle local directories and the metadata
   * for actively being merged shuffle partitions.
   */
  private final ConcurrentMap<String, AppShuffleInfo> appsShuffleInfo;

  private final Executor mergedShuffleCleaner;
  private final TransportConf conf;
  private final int minChunkSize;
  private final int ioExceptionsThresholdDuringMerge;

  @SuppressWarnings("UnstableApiUsage")
  private final LoadingCache<String, ShuffleIndexInformation> indexCache;

  @SuppressWarnings("UnstableApiUsage")
  public RemoteBlockPushResolver(TransportConf conf) {
    this.conf = conf;
    this.appsShuffleInfo = new ConcurrentHashMap<>();
    this.mergedShuffleCleaner = Executors.newSingleThreadExecutor(
      // Add `spark` prefix because it will run in NM in Yarn mode.
      NettyUtils.createThreadFactory("spark-shuffle-merged-shuffle-directory-cleaner"));
    this.minChunkSize = conf.minChunkSizeInMergedShuffleFile();
    this.ioExceptionsThresholdDuringMerge = conf.ioExceptionsThresholdDuringMerge();
    CacheLoader<String, ShuffleIndexInformation> indexCacheLoader =
      new CacheLoader<String, ShuffleIndexInformation>() {
        public ShuffleIndexInformation load(String filePath) throws IOException {
          return new ShuffleIndexInformation(filePath);
        }
    };
    indexCache = CacheBuilder.newBuilder()
      .maximumWeight(conf.mergedIndexCacheSize())
      .weigher((Weigher<String, ShuffleIndexInformation>)
        (filePath, indexInfo) -> indexInfo.getRetainedMemorySize())
      .build(indexCacheLoader);
  }

  @VisibleForTesting
  protected static ErrorHandler.BlockPushErrorHandler createErrorHandler() {
    return new ErrorHandler.BlockPushErrorHandler() {
      // Explicitly use a shuffle service side error handler for handling exceptions.
      // BlockPushNonException on the server side only has the response field set. It
      // might require different handling logic compared with a client side error handler.
      @Override
      public boolean shouldLogError(Throwable t) {
        return !(t instanceof BlockPushNonFatalFailure);
      }
    };
  }

  @VisibleForTesting
  protected AppShuffleInfo validateAndGetAppShuffleInfo(String appId) {
    // TODO: [SPARK-33236] Change the message when this service is able to handle NM restart
    AppShuffleInfo appShuffleInfo = appsShuffleInfo.get(appId);
    Preconditions.checkArgument(appShuffleInfo != null,
      "application " + appId + " is not registered or NM was restarted.");
    return appShuffleInfo;
  }

  /**
   * Given the appShuffleInfo, shuffleId, shuffleMergeId and reduceId that uniquely identifies
   * a given shuffle partition of an application, retrieves the associated metadata. If not
   * present and the corresponding merged shuffle does not exist, initializes the metadata.
   */
  private AppShufflePartitionInfo getOrCreateAppShufflePartitionInfo(
      AppShuffleInfo appShuffleInfo,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      String blockId) throws BlockPushNonFatalFailure {
    ConcurrentMap<Integer, AppShuffleMergePartitionsInfo> shuffles = appShuffleInfo.shuffles;
    AppShuffleMergePartitionsInfo shufflePartitionsWithMergeId =
      shuffles.compute(shuffleId, (id, mergePartitionsInfo) -> {
        if (mergePartitionsInfo == null) {
          logger.info("{} attempt {} shuffle {} shuffleMerge {}: creating a new shuffle " +
              "merge metadata", appShuffleInfo.appId, appShuffleInfo.attemptId, shuffleId,
              shuffleMergeId);
          return new AppShuffleMergePartitionsInfo(shuffleMergeId, false);
        } else {
          int latestShuffleMergeId = mergePartitionsInfo.shuffleMergeId;
          if (latestShuffleMergeId > shuffleMergeId) {
            // Reject the request as we have already seen a higher shuffleMergeId than the one
            // in the current request.
            throw new BlockPushNonFatalFailure(
              new BlockPushReturnCode(ReturnCode.STALE_BLOCK_PUSH.id(), blockId).toByteBuffer(),
              BlockPushNonFatalFailure.getErrorMsg(blockId, ReturnCode.STALE_BLOCK_PUSH));
          } else if (latestShuffleMergeId < shuffleMergeId){
            // Higher shuffleMergeId seen for the shuffle ID meaning new stage attempt is being
            // run for the shuffle ID. Close and clean up old shuffleMergeId files,
            // happens in the indeterminate stage retries
            logger.info("{} attempt {} shuffle {} shuffleMerge {}: creating a new shuffle " +
                "merge metadata since received shuffleMergeId is higher than latest " +
                "shuffleMergeId {}", appShuffleInfo.appId, appShuffleInfo.attemptId, shuffleId,
                shuffleMergeId, latestShuffleMergeId);
            mergedShuffleCleaner.execute(() ->
                closeAndDeletePartitionFiles(mergePartitionsInfo.shuffleMergePartitions));
            return new AppShuffleMergePartitionsInfo(shuffleMergeId, false);
          } else {
            // The request is for block with same shuffleMergeId as the latest shuffleMergeId
            if (mergePartitionsInfo.isFinalized()) {
              throw new BlockPushNonFatalFailure(
                  new BlockPushReturnCode(
                      ReturnCode.TOO_LATE_BLOCK_PUSH.id(), blockId).toByteBuffer(),
                  BlockPushNonFatalFailure.getErrorMsg(blockId, ReturnCode.TOO_LATE_BLOCK_PUSH));
            }
            return mergePartitionsInfo;
          }
        }
      });
    Map<Integer, AppShufflePartitionInfo> shuffleMergePartitions =
        shufflePartitionsWithMergeId.shuffleMergePartitions;
    return shuffleMergePartitions.computeIfAbsent(reduceId, key -> {
      // It only gets here when the key is not present in the map. The first time the merge
      // manager receives a pushed block for a given application shuffle partition.
      File dataFile =
        appShuffleInfo.getMergedShuffleDataFile(shuffleId, shuffleMergeId, reduceId);
      File indexFile = new File(
        appShuffleInfo.getMergedShuffleIndexFilePath(shuffleId, shuffleMergeId, reduceId));
      File metaFile =
        appShuffleInfo.getMergedShuffleMetaFile(shuffleId, shuffleMergeId, reduceId);
      try {
        return newAppShufflePartitionInfo(appShuffleInfo.appId, shuffleId, shuffleMergeId,
          reduceId, dataFile, indexFile, metaFile);
      } catch (IOException e) {
        logger.error("{} attempt {} shuffle {} shuffleMerge {}: cannot create merged shuffle " +
            "partition with data file {}, index file {}, and meta file {}", appShuffleInfo.appId,
            appShuffleInfo.attemptId, shuffleId, shuffleMergeId, dataFile.getAbsolutePath(),
            indexFile.getAbsolutePath(), metaFile.getAbsolutePath());
        throw new RuntimeException(
          String.format("Cannot initialize merged shuffle partition for appId %s shuffleId %s "
            + "shuffleMergeId %s reduceId %s", appShuffleInfo.appId, shuffleId, shuffleMergeId,
              reduceId), e);
      }
    });
  }

  @VisibleForTesting
  AppShufflePartitionInfo newAppShufflePartitionInfo(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      File dataFile,
      File indexFile,
      File metaFile) throws IOException {
    return new AppShufflePartitionInfo(appId, shuffleId, shuffleMergeId, reduceId, dataFile,
      new MergeShuffleFile(indexFile), new MergeShuffleFile(metaFile));
  }

  @Override
  public MergedBlockMeta getMergedBlockMeta(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId) {
    AppShuffleInfo appShuffleInfo = validateAndGetAppShuffleInfo(appId);
    AppShuffleMergePartitionsInfo partitionsInfo = appShuffleInfo.shuffles.get(shuffleId);
    if (null != partitionsInfo && partitionsInfo.shuffleMergeId > shuffleMergeId) {
      throw new RuntimeException(String.format(
        "MergedBlockMeta fetch for shuffle %s with shuffleMergeId %s reduceId %s is %s",
        shuffleId, shuffleMergeId, reduceId,
        ErrorHandler.BlockFetchErrorHandler.STALE_SHUFFLE_BLOCK_FETCH));
    }
    File indexFile = new File(
      appShuffleInfo.getMergedShuffleIndexFilePath(shuffleId, shuffleMergeId, reduceId));
    if (!indexFile.exists()) {
      throw new RuntimeException(String.format(
        "Merged shuffle index file %s not found", indexFile.getPath()));
    }
    int size = (int) indexFile.length();
    // First entry is the zero offset
    int numChunks = (size / Long.BYTES) - 1;
    File metaFile = appShuffleInfo.getMergedShuffleMetaFile(shuffleId, shuffleMergeId, reduceId);
    if (!metaFile.exists()) {
      throw new RuntimeException(String.format("Merged shuffle meta file %s not found",
        metaFile.getPath()));
    }
    FileSegmentManagedBuffer chunkBitMaps =
      new FileSegmentManagedBuffer(conf, metaFile, 0L, metaFile.length());
    logger.trace(
      "{} shuffleId {} shuffleMergeId {} reduceId {} num chunks {}",
      appId, shuffleId, shuffleMergeId, reduceId, numChunks);
    return new MergedBlockMeta(numChunks, chunkBitMaps);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public ManagedBuffer getMergedBlockData(
      String appId, int shuffleId, int shuffleMergeId, int reduceId, int chunkId) {
    AppShuffleInfo appShuffleInfo = validateAndGetAppShuffleInfo(appId);
    AppShuffleMergePartitionsInfo partitionsInfo = appShuffleInfo.shuffles.get(shuffleId);
    if (null != partitionsInfo && partitionsInfo.shuffleMergeId > shuffleMergeId) {
      throw new RuntimeException(String.format(
        "MergedBlockData fetch for shuffle %s with shuffleMergeId %s reduceId %s is %s",
        shuffleId, shuffleMergeId, reduceId,
        ErrorHandler.BlockFetchErrorHandler.STALE_SHUFFLE_BLOCK_FETCH));
    }
    File dataFile = appShuffleInfo.getMergedShuffleDataFile(shuffleId, shuffleMergeId, reduceId);
    if (!dataFile.exists()) {
      throw new RuntimeException(String.format("Merged shuffle data file %s not found",
        dataFile.getPath()));
    }
    String indexFilePath =
      appShuffleInfo.getMergedShuffleIndexFilePath(shuffleId, shuffleMergeId, reduceId);
    try {
      // If we get here, the merged shuffle file should have been properly finalized. Thus we can
      // use the file length to determine the size of the merged shuffle block.
      ShuffleIndexInformation shuffleIndexInformation = indexCache.get(indexFilePath);
      ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(chunkId);
      return new FileSegmentManagedBuffer(
        conf, dataFile, shuffleIndexRecord.getOffset(), shuffleIndexRecord.getLength());
    } catch (ExecutionException e) {
      throw new RuntimeException(String.format(
        "Failed to open merged shuffle index file %s", indexFilePath), e);
    }
  }

  @Override
  public String[] getMergedBlockDirs(String appId) {
    AppShuffleInfo appShuffleInfo = validateAndGetAppShuffleInfo(appId);
    return appShuffleInfo.appPathsInfo.activeLocalDirs;
  }

  @Override
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    logger.info("Application {} removed, cleanupLocalDirs = {}", appId, cleanupLocalDirs);
    AppShuffleInfo appShuffleInfo = appsShuffleInfo.remove(appId);
    if (null != appShuffleInfo) {
      mergedShuffleCleaner.execute(
        () -> closeAndDeletePartitionFilesIfNeeded(appShuffleInfo, cleanupLocalDirs));
    }
  }


  /**
   * Clean up the AppShufflePartitionInfo for a specific AppShuffleInfo.
   * If cleanupLocalDirs is true, the merged shuffle files will also be deleted.
   * The cleanup will be executed in a separate thread.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @VisibleForTesting
  void closeAndDeletePartitionFilesIfNeeded(
      AppShuffleInfo appShuffleInfo,
      boolean cleanupLocalDirs) {
    appShuffleInfo.shuffles.forEach((shuffleId, shuffleInfo) -> shuffleInfo.shuffleMergePartitions
      .forEach((shuffleMergeId, partitionInfo) -> {
        synchronized (partitionInfo) {
          partitionInfo.closeAllFilesAndDeleteIfNeeded(false);
        }
      }));
    if (cleanupLocalDirs) {
      deleteExecutorDirs(appShuffleInfo);
    }
  }

  /**
   * Clean up all the AppShufflePartitionInfo for a specific shuffleMergeId. This is done
   * since there is a higher shuffleMergeId request made for a shuffleId, therefore clean
   * up older shuffleMergeId partitions. The cleanup will be executed in a separate thread.
   */
  @VisibleForTesting
  void closeAndDeletePartitionFiles(Map<Integer, AppShufflePartitionInfo> partitions) {
    partitions
      .forEach((partitionId, partitionInfo) -> {
        synchronized (partitionInfo) {
          partitionInfo.closeAllFilesAndDeleteIfNeeded(true);
        }
      });
  }

  /**
   * Serially delete local dirs.
   */
  @VisibleForTesting
  void deleteExecutorDirs(AppShuffleInfo appShuffleInfo) {
    Path[] dirs = Arrays.stream(appShuffleInfo.appPathsInfo.activeLocalDirs)
      .map(dir -> Paths.get(dir)).toArray(Path[]::new);
    for (Path localDir : dirs) {
      try {
        if (Files.exists(localDir)) {
          JavaUtils.deleteRecursively(localDir.toFile());
          logger.debug("Successfully cleaned up directory: {}", localDir);
        }
      } catch (Exception e) {
        logger.error("Failed to delete directory: {}", localDir, e);
      }
    }
  }

  @Override
  public StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg) {
    AppShuffleInfo appShuffleInfo = validateAndGetAppShuffleInfo(msg.appId);
    // Use string concatenation here to avoid the overhead with String.format on every
    // pushed block.
    final String streamId = OneForOneBlockPusher.SHUFFLE_PUSH_BLOCK_PREFIX + "_"
      + msg.shuffleId + "_" + msg.shuffleMergeId + "_" + msg.mapIndex + "_" + msg.reduceId;
    if (appShuffleInfo.attemptId != msg.appAttemptId) {
      // If this Block belongs to a former application attempt, it is considered late,
      // as only the blocks from the current application attempt will be merged
      throw new BlockPushNonFatalFailure(new BlockPushReturnCode(ReturnCode
        .TOO_OLD_ATTEMPT_PUSH.id(), streamId).toByteBuffer(),
        BlockPushNonFatalFailure.getErrorMsg(streamId, ReturnCode.TOO_OLD_ATTEMPT_PUSH));
    }
    // Retrieve merged shuffle file metadata
    AppShufflePartitionInfo partitionInfoBeforeCheck;
    BlockPushNonFatalFailure failure = null;
    try {
      partitionInfoBeforeCheck = getOrCreateAppShufflePartitionInfo(appShuffleInfo, msg.shuffleId,
        msg.shuffleMergeId, msg.reduceId, streamId);
    } catch (BlockPushNonFatalFailure bpf) {
      // Set partitionInfoBeforeCheck to null so that stale block push gets handled.
      partitionInfoBeforeCheck = null;
      failure = bpf;
    }
    // Here partitionInfo will be null in 3 cases:
    // 1) The request is received for a block that has already been merged, this is possible due
    // to the retry logic.
    // 2) The request is received after the merged shuffle is finalized, thus is too late.
    // 3) The request is received for a older shuffleMergeId, therefore the block push is rejected.
    //
    // For case 1, we will drain the data in the channel and just respond success
    // to the client. This is required because the response of the previously merged
    // block will be ignored by the client, per the logic in RetryingBlockFetcher.
    // Note that the netty server should receive data for a given block id only from 1 channel
    // at any time. The block should be pushed only from successful maps, thus there should be
    // only 1 source for a given block at any time. Although the netty client might retry sending
    // this block to the server multiple times, the data of the same block always arrives from the
    // same channel thus the server should have already processed the previous request of this
    // block before seeing it again in the channel. This guarantees that we can simply just
    // check the bitmap to determine if a block is a duplicate or not.
    //
    // For case 2, we will also drain the data in the channel, but throw an exception in
    // {@link org.apache.spark.network.client.StreamCallback#onComplete(String)}. This way,
    // the client will be notified of the failure but the channel will remain active. Keeping
    // the channel alive is important because the same channel could be reused by multiple map
    // tasks in the executor JVM, which belongs to different stages. While one of the shuffles
    // in these stages is finalized, the others might still be active. Tearing down the channel
    // on the server side will disrupt these other on-going shuffle merges. It's also important
    // to notify the client of the failure, so that it can properly halt pushing the remaining
    // blocks upon receiving such failures to preserve resources on the server/client side.
    //
    // For case 3, we will also drain the data in the channel, but throw an exception in
    // {@link org.apache.spark.network.client.StreamCallback#onComplete(String)}. This way,
    // the client will be notified of the failure but the channel will remain active. It is
    // important to notify the client of the failure, so that it can properly halt pushing the
    // remaining blocks upon receiving such failures to preserve resources on the server/client
    // side.
    //
    // Speculative execution would also raise a possible scenario with duplicate blocks. Although
    // speculative execution would kill the slower task attempt, leading to only 1 task attempt
    // succeeding in the end, there is no guarantee that only one copy of the block will be
    // pushed. This is due to our handling of block push process outside of the map task, thus
    // it is possible for the speculative task attempt to initiate the block push process before
    // getting killed. When this happens, we need to distinguish the duplicate blocks as they
    // arrive. More details on this is explained in later comments.

    // Check if the given block is already merged by checking the bitmap against the given map
    // index
    final AppShufflePartitionInfo partitionInfo = failure != null ? null :
      partitionInfoBeforeCheck.mapTracker.contains(msg.mapIndex) ? null : partitionInfoBeforeCheck;
    if (partitionInfo != null) {
      return new PushBlockStreamCallback(
        this, appShuffleInfo, streamId, partitionInfo, msg.mapIndex);
    } else {
      final BlockPushNonFatalFailure finalFailure = failure;
      // For a duplicate block or a block which is late or stale block from an older
      // shuffleMergeId, respond back with a callback that handles them differently.
      return new StreamCallbackWithID() {
        @Override
        public String getID() {
          return streamId;
        }

        @Override
        public void onData(String streamId, ByteBuffer buf) {
          // Ignore the requests. It reaches here either when a request is received after the
          // shuffle file is finalized or when a request is for a duplicate block.
        }

        @Override
        public void onComplete(String streamId) {
          // Throw non-fatal failure here so the block data is drained from channel and server
          // responds the error code to the client.
          if (finalFailure != null) {
            throw finalFailure;
          }
          // For duplicate block that is received before the shuffle merge finalizes, the
          // server should respond success to the client.
        }

        @Override
        public void onFailure(String streamId, Throwable cause) {
        }

        @Override
        public ByteBuffer getCompletionResponse() {
          return SUCCESS_RESPONSE.duplicate();
        }
      };
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) {
    logger.info("{} attempt {} shuffle {} shuffleMerge {}: finalize shuffle merge",
        msg.appId, msg.appAttemptId, msg.shuffleId, msg.shuffleMergeId);
    AppShuffleInfo appShuffleInfo = validateAndGetAppShuffleInfo(msg.appId);
    if (appShuffleInfo.attemptId != msg.appAttemptId) {
      // If finalizeShuffleMerge from a former application attempt, it is considered late,
      // as only the finalizeShuffleMerge request from the current application attempt
      // will be merged. Too old app attempt only being seen by an already failed
      // app attempt, and no need use callback to return to client now, because
      // the finalizeShuffleMerge in DAGScheduler has no retry policy, and don't
      // use the BlockPushNonFatalFailure because it's the finalizeShuffleMerge
      // related case, not the block push case, just throw it in server side now.
      // TODO we may use a new exception class to include the finalizeShuffleMerge
      // related case just as the BlockPushNonFatalFailure contains the block push cases.
      throw new IllegalArgumentException(
        String.format("The attempt id %s in this FinalizeShuffleMerge message does not match "
            + "with the current attempt id %s stored in shuffle service for application %s",
          msg.appAttemptId, appShuffleInfo.attemptId, msg.appId));
    }
    AtomicReference<Map<Integer, AppShufflePartitionInfo>> shuffleMergePartitionsRef =
      new AtomicReference<>(null);
    appShuffleInfo.shuffles.compute(msg.shuffleId, (shuffleId, mergePartitionsInfo) -> {
      if (null != mergePartitionsInfo) {
        if (msg.shuffleMergeId < mergePartitionsInfo.shuffleMergeId ||
          mergePartitionsInfo.isFinalized()) {
          throw new RuntimeException(String.format(
              "Shuffle merge finalize request for shuffle %s with" + " shuffleMergeId %s is %s",
              msg.shuffleId, msg.shuffleMergeId,
              ErrorHandler.BlockPushErrorHandler.STALE_SHUFFLE_FINALIZE_SUFFIX));
        } else if (msg.shuffleMergeId > mergePartitionsInfo.shuffleMergeId) {
          // If no blocks pushed for the finalizeShuffleMerge shuffleMergeId then return
          // empty MergeStatuses but cleanup the older shuffleMergeId files.
          mergedShuffleCleaner.execute(() ->
              closeAndDeletePartitionFiles(mergePartitionsInfo.shuffleMergePartitions));
        } else {
          // This block covers:
          //  1. finalization of determinate stage
          //  2. finalization of indeterminate stage if the shuffleMergeId related to it is the one
          //  for which the message is received.
          shuffleMergePartitionsRef.set(mergePartitionsInfo.shuffleMergePartitions);
        }
      }
      // Even when the mergePartitionsInfo is null, we mark the shuffle as finalized but the results
      // sent to the driver will be empty. This cam happen when the service didn't receive any
      // blocks for the shuffle yet and the driver didn't wait for enough time to finalize the
      // shuffle.
      return new AppShuffleMergePartitionsInfo(msg.shuffleMergeId, true);
    });
    Map<Integer, AppShufflePartitionInfo> shuffleMergePartitions = shuffleMergePartitionsRef.get();
    MergeStatuses mergeStatuses;
    if (null == shuffleMergePartitions || shuffleMergePartitions.isEmpty()) {
      mergeStatuses =
        new MergeStatuses(msg.shuffleId, msg.shuffleMergeId,
          new RoaringBitmap[0], new int[0], new long[0]);
    } else {
      List<RoaringBitmap> bitmaps = new ArrayList<>(shuffleMergePartitions.size());
      List<Integer> reduceIds = new ArrayList<>(shuffleMergePartitions.size());
      List<Long> sizes = new ArrayList<>(shuffleMergePartitions.size());
      for (AppShufflePartitionInfo partition: shuffleMergePartitions.values()) {
        synchronized (partition) {
          try {
            logger.debug("{} attempt {} shuffle {} shuffleMerge {}: finalizing shuffle " +
                "partition {} ", msg.appId, msg.appAttemptId, msg.shuffleId,
                msg.shuffleMergeId, partition.reduceId);
            // This can throw IOException which will marks this shuffle partition as not merged.
            partition.finalizePartition();
            if (partition.mapTracker.getCardinality() > 0) {
              bitmaps.add(partition.mapTracker);
              reduceIds.add(partition.reduceId);
              sizes.add(partition.getLastChunkOffset());
              logger.debug("{} attempt {} shuffle {} shuffleMerge {}: finalization results " +
                  "added for partition {} data size {} index size {} meta size {}",
                  msg.appId, msg.appAttemptId, msg.shuffleId,
                  msg.shuffleMergeId, partition.reduceId, partition.getLastChunkOffset(),
                  partition.indexFile.getPos(), partition.metaFile.getPos());
            }
          } catch (IOException ioe) {
            logger.warn("{} attempt {} shuffle {} shuffleMerge {}: exception while " +
                "finalizing shuffle partition {}", msg.appId, msg.appAttemptId, msg.shuffleId,
                msg.shuffleMergeId, partition.reduceId);
          } finally {
            partition.closeAllFilesAndDeleteIfNeeded(false);
          }
        }
      }
      mergeStatuses = new MergeStatuses(msg.shuffleId, msg.shuffleMergeId,
        bitmaps.toArray(new RoaringBitmap[bitmaps.size()]), Ints.toArray(reduceIds),
        Longs.toArray(sizes));
    }
    logger.info("{} attempt {} shuffle {} shuffleMerge {}: finalization of shuffle merge completed",
        msg.appId, msg.appAttemptId, msg.shuffleId, msg.shuffleMergeId);
    return mergeStatuses;
  }

  @Override
  public void registerExecutor(String appId, ExecutorShuffleInfo executorInfo) {
    if (logger.isDebugEnabled()) {
      logger.debug("register executor with RemoteBlockPushResolver {} local-dirs {} "
        + "num sub-dirs {} shuffleManager {}", appId, Arrays.toString(executorInfo.localDirs),
        executorInfo.subDirsPerLocalDir, executorInfo.shuffleManager);
    }
    String shuffleManagerMeta = executorInfo.shuffleManager;
    if (shuffleManagerMeta.contains(SHUFFLE_META_DELIMITER)) {
      String mergeDirInfo =
        shuffleManagerMeta.substring(shuffleManagerMeta.indexOf(SHUFFLE_META_DELIMITER) + 1);
      try {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<Map<String, String>> typeRef
          = new TypeReference<Map<String, String>>(){};
        Map<String, String> metaMap = mapper.readValue(mergeDirInfo, typeRef);
        String mergeDir = metaMap.get(MERGE_DIR_KEY);
        int attemptId = Integer.valueOf(
          metaMap.getOrDefault(ATTEMPT_ID_KEY, String.valueOf(UNDEFINED_ATTEMPT_ID)));
        if (mergeDir == null) {
          throw new IllegalArgumentException(
            String.format("Failed to get the merge directory information from the " +
              "shuffleManagerMeta %s in executor registration message", shuffleManagerMeta));
        }
        if (attemptId == UNDEFINED_ATTEMPT_ID) {
          // When attemptId is -1, there is no attemptId stored in the ExecutorShuffleInfo.
          // Only the first ExecutorRegister message can register the merge dirs
          appsShuffleInfo.computeIfAbsent(appId, id ->
            new AppShuffleInfo(
              appId, UNDEFINED_ATTEMPT_ID,
              new AppPathsInfo(appId, executorInfo.localDirs,
                mergeDir, executorInfo.subDirsPerLocalDir)
            ));
        } else {
          // If attemptId is not -1, there is attemptId stored in the ExecutorShuffleInfo.
          // The first ExecutorRegister message from the same application attempt wil register
          // the merge dirs in External Shuffle Service. Any later ExecutorRegister message
          // from the same application attempt will not override the merge dirs. But it can
          // be overridden by ExecutorRegister message from newer application attempt,
          // and former attempts' shuffle partitions information will also be cleaned up.
          AtomicReference<AppShuffleInfo> originalAppShuffleInfo = new AtomicReference<>();
          appsShuffleInfo.compute(appId, (id, appShuffleInfo) -> {
            if (appShuffleInfo == null || attemptId > appShuffleInfo.attemptId) {
              originalAppShuffleInfo.set(appShuffleInfo);
              appShuffleInfo =
                new AppShuffleInfo(
                  appId, attemptId,
                  new AppPathsInfo(appId, executorInfo.localDirs,
                    mergeDir, executorInfo.subDirsPerLocalDir));
            }
            return appShuffleInfo;
          });
          if (originalAppShuffleInfo.get() != null) {
            AppShuffleInfo appShuffleInfo = originalAppShuffleInfo.get();
            logger.warn("Cleanup shuffle info and merged shuffle files for {}_{} as new " +
                "application attempt registered", appId, appShuffleInfo.attemptId);
            mergedShuffleCleaner.execute(
              () -> closeAndDeletePartitionFilesIfNeeded(appShuffleInfo, true));
          }
        }
      } catch (JsonProcessingException e) {
        logger.warn("Failed to get the merge directory information from ExecutorShuffleInfo: ", e);
      }
    } else {
      logger.warn("ExecutorShuffleInfo does not have the expected merge directory information");
    }
  }

  /**
   * Callback for push stream that handles blocks which are not already merged.
   */
  static class PushBlockStreamCallback implements StreamCallbackWithID {

    private final RemoteBlockPushResolver mergeManager;
    private final AppShuffleInfo appShuffleInfo;
    private final String streamId;
    private final int mapIndex;
    private final AppShufflePartitionInfo partitionInfo;
    private int length = 0;
    // This indicates that this stream got the opportunity to write the blocks to the merged file.
    // Once this is set to true and the stream encounters a failure then it will unset the
    // currentMapId of the partition so that another stream can start merging the blocks to the
    // partition. This is reset to false when the stream completes.
    private boolean isWriting = false;
    // Use on-heap instead of direct ByteBuffer since these buffers will be GC'ed very quickly
    private List<ByteBuffer> deferredBufs;

    private PushBlockStreamCallback(
        RemoteBlockPushResolver mergeManager,
        AppShuffleInfo appShuffleInfo,
        String streamId,
        AppShufflePartitionInfo partitionInfo,
        int mapIndex) {
      Preconditions.checkArgument(mergeManager != null);
      this.mergeManager = mergeManager;
      Preconditions.checkArgument(appShuffleInfo != null);
      this.appShuffleInfo = appShuffleInfo;
      this.streamId = streamId;
      Preconditions.checkArgument(partitionInfo != null);
      this.partitionInfo = partitionInfo;
      this.mapIndex = mapIndex;
      abortIfNecessary();
    }

    @Override
    public String getID() {
      return streamId;
    }

    @Override
    public ByteBuffer getCompletionResponse() {
      return SUCCESS_RESPONSE.duplicate();
    }

    /**
     * Write a ByteBuffer to the merged shuffle file. Here we keep track of the length of the
     * block data written to file. In case of failure during writing block to file, we use the
     * information tracked in partitionInfo to overwrite the corrupt block when writing the new
     * block.
     */
    private void writeBuf(ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        long updatedPos = partitionInfo.getDataFilePos() + length;
        logger.debug("{} current pos {} updated pos {}", partitionInfo,
          partitionInfo.getDataFilePos(), updatedPos);
        length += partitionInfo.dataChannel.write(buf, updatedPos);
      }
    }

    /**
     * There will be multiple streams of map blocks belonging to the same reduce partition. At any
     * given point of time, only a single map stream can write its data to the merged file. Until
     * this stream is completed, the other streams defer writing. This prevents corruption of
     * merged data. This returns whether this stream is the active stream that can write to the
     * merged file.
     */
    private boolean allowedToWrite() {
      return partitionInfo.getCurrentMapIndex() < 0
        || partitionInfo.getCurrentMapIndex() == mapIndex;
    }

    /**
     * Returns if this is a duplicate block generated by speculative tasks. With speculative
     * tasks, we could receive the same block from 2 different sources at the same time. One of
     * them is going to be the first to set the currentMapIndex. When that block does so, it's
     * going to see the currentMapIndex initially as -1. After it sets the currentMapIndex, it's
     * going to write some data to disk, thus increasing the length counter. The other duplicate
     * block is going to see the currentMapIndex already set to its mapIndex. However, it hasn't
     * written any data yet. If the first block gets written completely and resets the
     * currentMapIndex to -1 before the processing for the second block finishes, we can just
     * check the bitmap to identify the second as a duplicate.
     */
    private boolean isDuplicateBlock() {
      return (partitionInfo.getCurrentMapIndex() == mapIndex && length == 0)
        || partitionInfo.mapTracker.contains(mapIndex);
    }

    /**
     * This is only invoked when the stream is able to write. The stream first writes any deferred
     * block parts buffered in memory.
     */
    private void writeDeferredBufs() throws IOException {
      for (ByteBuffer deferredBuf : deferredBufs) {
        writeBuf(deferredBuf);
      }
      deferredBufs = null;
    }

    /**
     * @throws IllegalStateException if the number of IOExceptions have exceeded threshold.
     */
    private void abortIfNecessary() {
      if (partitionInfo.shouldAbort(mergeManager.ioExceptionsThresholdDuringMerge)) {
        deferredBufs = null;
        throw new IllegalStateException(String.format("%s when merging %s",
          ErrorHandler.BlockPushErrorHandler.IOEXCEPTIONS_EXCEEDED_THRESHOLD_PREFIX,
          streamId));
      }
    }

    /**
     * This increments the number of IOExceptions and throws RuntimeException if it exceeds the
     * threshold which will abort the merge of a particular shuffle partition.
     */
    private void incrementIOExceptionsAndAbortIfNecessary() {
      // Update the count of IOExceptions
      partitionInfo.incrementIOExceptions();
      abortIfNecessary();
    }

    /**
     * If appShuffleMergePartitionsInfo is null or shuffleMergeId is
     * greater than the request shuffleMergeId then it is a stale block push.
     */
    private boolean isStale(
        AppShuffleMergePartitionsInfo appShuffleMergePartitionsInfo,
        int shuffleMergeId) {
      return null == appShuffleMergePartitionsInfo ||
        appShuffleMergePartitionsInfo.shuffleMergeId > shuffleMergeId;
    }

    /**
     * If appShuffleMergePartitionsInfo is null or shuffleMergePartitions is set to
     * INDETERMINATE_SHUFFLE_FINALIZED or if the reduceId is not in the map then the
     * shuffle is already finalized. Therefore the block push is too late.
     */
    private boolean isTooLate(
        AppShuffleMergePartitionsInfo appShuffleMergePartitionsInfo,
        int reduceId) {
      return null == appShuffleMergePartitionsInfo ||
        appShuffleMergePartitionsInfo.isFinalized() ||
        !appShuffleMergePartitionsInfo.shuffleMergePartitions.containsKey(reduceId);
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      // When handling the block data using StreamInterceptor, it can help to reduce the amount
      // of data that needs to be buffered in memory since it does not wait till the completion
      // of the frame before handling the message, thus releasing the ByteBuf earlier. However,
      // this also means it would chunk a block into multiple buffers. Here, we want to preserve
      // the benefit of handling the block data using StreamInterceptor as much as possible while
      // providing the guarantee that one block would be continuously written to the merged
      // shuffle file before the next block starts. For each shuffle partition, we would track
      // the current map index to make sure only block matching the map index can be written to
      // disk. If one server thread sees the block being handled is the current block, it would
      // directly write the block to disk. Otherwise, it would buffer the block chunks in memory.
      // If the block becomes the current block before we see the end of it, we would then dump
      // all buffered block data to disk and write the remaining portions of the block directly
      // to disk as well. This way, we avoid having to buffer the entirety of every blocks in
      // memory, while still providing the necessary guarantee.
      synchronized (partitionInfo) {
        AppShuffleMergePartitionsInfo info = appShuffleInfo.shuffles.get(partitionInfo.shuffleId);
        if (isStale(info, partitionInfo.shuffleMergeId) ||
            isTooLate(info, partitionInfo.reduceId)) {
          deferredBufs = null;
          return;
        }
        // Check whether we can write to disk
        if (allowedToWrite()) {
          // Identify duplicate block generated by speculative tasks. We respond success to
          // the client in cases of duplicate even though no data is written.
          if (isDuplicateBlock()) {
            deferredBufs = null;
            return;
          }
          abortIfNecessary();
          logger.trace("{} onData writable", partitionInfo);
          if (partitionInfo.getCurrentMapIndex() < 0) {
            partitionInfo.setCurrentMapIndex(mapIndex);
          }

          // If we got here, it's safe to write the block data to the merged shuffle file. We
          // first write any deferred block.
          isWriting = true;
          try {
            if (deferredBufs != null && !deferredBufs.isEmpty()) {
              writeDeferredBufs();
            }
            writeBuf(buf);
          } catch (IOException ioe) {
            incrementIOExceptionsAndAbortIfNecessary();
            // If the above doesn't throw a RuntimeException, then we propagate the IOException
            // back to the client so the block could be retried.
            throw ioe;
          }
        } else {
          logger.trace("{} onData deferred", partitionInfo);
          // If we cannot write to disk, we buffer the current block chunk in memory so it could
          // potentially be written to disk later. We take our best effort without guarantee
          // that the block will be written to disk. If the block data is divided into multiple
          // chunks during TCP transportation, each #onData invocation is an attempt to write
          // the block to disk. If the block is still not written to disk after all #onData
          // invocations, the final #onComplete invocation is the last attempt to write the
          // block to disk. If we still couldn't write this block to disk after this, we give up
          // on this block push request and respond failure to client. We could potentially
          // buffer the block longer or wait for a few iterations inside #onData or #onComplete
          // to increase the chance of writing the block to disk, however this would incur more
          // memory footprint or decrease the server processing throughput for the shuffle
          // service. In addition, during test we observed that by randomizing the order in
          // which clients sends block push requests batches, only ~0.5% blocks failed to be
          // written to disk due to this reason. We thus decide to optimize for server
          // throughput and memory usage.
          if (deferredBufs == null) {
            deferredBufs = new ArrayList<>();
          }
          // Write the buffer to the in-memory deferred cache. Since buf is a slice of a larger
          // byte buffer, we cache only the relevant bytes not the entire large buffer to save
          // memory.
          ByteBuffer deferredBuf = ByteBuffer.allocate(buf.remaining());
          deferredBuf.put(buf);
          deferredBuf.flip();
          deferredBufs.add(deferredBuf);
        }
      }
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      synchronized (partitionInfo) {
        logger.trace("{} onComplete invoked", partitionInfo);
        // Initially when this request got to the server, the shuffle merge finalize request
        // was not received yet or this was the latest stage attempt (or latest shuffleMergeId)
        // generating shuffle output for the shuffle ID. By the time we finish reading this
        // message, the block request is either stale or too late. We should thus respond
        // the error code to the client.
        AppShuffleMergePartitionsInfo info = appShuffleInfo.shuffles.get(partitionInfo.shuffleId);
        if (isTooLate(info, partitionInfo.reduceId)) {
          deferredBufs = null;
          throw new BlockPushNonFatalFailure(
            new BlockPushReturnCode(ReturnCode.TOO_LATE_BLOCK_PUSH.id(), streamId).toByteBuffer(),
            BlockPushNonFatalFailure.getErrorMsg(streamId, ReturnCode.TOO_LATE_BLOCK_PUSH));
        }
        if (isStale(info, partitionInfo.shuffleMergeId)) {
          deferredBufs = null;
          throw new BlockPushNonFatalFailure(
            new BlockPushReturnCode(ReturnCode.STALE_BLOCK_PUSH.id(), streamId).toByteBuffer(),
            BlockPushNonFatalFailure.getErrorMsg(streamId, ReturnCode.STALE_BLOCK_PUSH));
        }

        // Check if we can commit this block
        if (allowedToWrite()) {
          // Identify duplicate block generated by speculative tasks. We respond success to
          // the client in cases of duplicate even though no data is written.
          if (isDuplicateBlock()) {
            deferredBufs = null;
            return;
          }
          if (partitionInfo.getCurrentMapIndex() < 0) {
            try {
              if (deferredBufs != null && !deferredBufs.isEmpty()) {
                abortIfNecessary();
                isWriting = true;
                writeDeferredBufs();
              }
            } catch (IOException ioe) {
              incrementIOExceptionsAndAbortIfNecessary();
              // If the above doesn't throw a RuntimeException, then we propagate the IOException
              // back to the client so the block could be retried.
              throw ioe;
            }
          }
          long updatedPos = partitionInfo.getDataFilePos() + length;
          boolean indexUpdated = false;
          if (updatedPos - partitionInfo.getLastChunkOffset() >= mergeManager.minChunkSize) {
            try {
              partitionInfo.updateChunkInfo(updatedPos, mapIndex);
              indexUpdated = true;
            } catch (IOException ioe) {
              incrementIOExceptionsAndAbortIfNecessary();
              // If the above doesn't throw a RuntimeException, then we do not propagate the
              // IOException to the client. This may increase the chunk size however the increase is
              // still limited because of the limit on the number of IOExceptions for a
              // particular shuffle partition.
            }
          }
          partitionInfo.setDataFilePos(updatedPos);
          partitionInfo.setCurrentMapIndex(-1);

          // update merged results
          partitionInfo.blockMerged(mapIndex);
          if (indexUpdated) {
            partitionInfo.resetChunkTracker();
          }
        } else {
          deferredBufs = null;
          throw new BlockPushNonFatalFailure(
            new BlockPushReturnCode(ReturnCode.BLOCK_APPEND_COLLISION_DETECTED.id(), streamId)
              .toByteBuffer(), BlockPushNonFatalFailure.getErrorMsg(
                streamId, ReturnCode.BLOCK_APPEND_COLLISION_DETECTED));
        }
      }
      isWriting = false;
    }

    @Override
    public void onFailure(String streamId, Throwable throwable) throws IOException {
      if (ERROR_HANDLER.shouldLogError(throwable)) {
        logger.error("Encountered issue when merging {}", streamId, throwable);
      } else {
        logger.debug("Encountered issue when merging {}", streamId, throwable);
      }
      // Only update partitionInfo if the failure corresponds to a valid request. If the
      // request is too late, i.e. received after shuffle merge finalize or stale block push,
      // #onFailure will also be triggered, and we can just ignore. Also, if we couldn't find
      // an opportunity to write the block data to disk, we should also ignore here.
      if (isWriting) {
        synchronized (partitionInfo) {
          AppShuffleMergePartitionsInfo info =
            appShuffleInfo.shuffles.get(partitionInfo.shuffleId);
          if (!isTooLate(info, partitionInfo.reduceId) &&
              !isStale(info, partitionInfo.shuffleMergeId)) {
            logger.debug("{} encountered failure", partitionInfo);
            partitionInfo.setCurrentMapIndex(-1);
          }
        }
      }
      isWriting = false;
    }

    @VisibleForTesting
    AppShufflePartitionInfo getPartitionInfo() {
      return partitionInfo;
    }
  }

  /**
   * Wrapper class to hold merged Shuffle related information for a specific shuffleMergeId
   * required for the shuffles of indeterminate stages.
   */
  public static class AppShuffleMergePartitionsInfo {
    // ConcurrentHashMap doesn't allow null for keys or values which is why this is required.
    // Marker to identify finalized shuffle partitions.
    private static final Map<Integer, AppShufflePartitionInfo> SHUFFLE_FINALIZED_MARKER =
        Collections.emptyMap();
    private final int shuffleMergeId;
    private final Map<Integer, AppShufflePartitionInfo> shuffleMergePartitions;

    public AppShuffleMergePartitionsInfo(int shuffleMergeId, boolean shuffleFinalized) {
      this.shuffleMergeId = shuffleMergeId;
      this.shuffleMergePartitions = shuffleFinalized ? SHUFFLE_FINALIZED_MARKER :
          new ConcurrentHashMap<>();
    }

    @VisibleForTesting
    public Map<Integer, AppShufflePartitionInfo> getShuffleMergePartitions() {
      return shuffleMergePartitions;
    }

    public boolean isFinalized() {
      return shuffleMergePartitions == SHUFFLE_FINALIZED_MARKER;
    }
  }

  /** Metadata tracked for an actively merged shuffle partition */
  public static class AppShufflePartitionInfo {

    private final String appId;
    private final int shuffleId;
    private final int shuffleMergeId;
    private final int reduceId;
    private final File dataFile;
    // The merged shuffle data file channel
    public final FileChannel dataChannel;
    // The index file for a particular merged shuffle contains the chunk offsets.
    private final MergeShuffleFile indexFile;
    // The meta file for a particular merged shuffle contains all the map indices that belong to
    // every chunk. The entry per chunk is a serialized bitmap.
    private final MergeShuffleFile metaFile;
    // Location offset of the last successfully merged block for this shuffle partition
    private long dataFilePos;
    // Track the map index whose block is being merged for this shuffle partition
    private int currentMapIndex;
    // Bitmap tracking which mapper's blocks have been merged for this shuffle partition
    private RoaringBitmap mapTracker;
    // The offset for the last chunk tracked in the index file for this shuffle partition
    private long lastChunkOffset;
    private int lastMergedMapIndex = -1;
    // Bitmap tracking which mapper's blocks are in the current shuffle chunk
    private RoaringBitmap chunkTracker;
    private int numIOExceptions = 0;
    private boolean indexMetaUpdateFailed;

    AppShufflePartitionInfo(
        String appId,
        int shuffleId,
        int shuffleMergeId,
        int reduceId,
        File dataFile,
        MergeShuffleFile indexFile,
        MergeShuffleFile metaFile) throws IOException {
      Preconditions.checkArgument(appId != null, "app id is null");
      this.appId = appId;
      this.shuffleId = shuffleId;
      this.shuffleMergeId = shuffleMergeId;
      this.reduceId = reduceId;
      this.dataChannel = new FileOutputStream(dataFile).getChannel();
      this.dataFile = dataFile;
      this.indexFile = indexFile;
      this.metaFile = metaFile;
      this.currentMapIndex = -1;
      // Writing 0 offset so that we can reuse ShuffleIndexInformation.getIndex()
      updateChunkInfo(0L, -1);
      this.dataFilePos = 0;
      this.mapTracker = new RoaringBitmap();
      this.chunkTracker = new RoaringBitmap();
    }

    public long getDataFilePos() {
      return dataFilePos;
    }

    public void setDataFilePos(long dataFilePos) {
      logger.trace("{} current pos {} update pos {}", this, this.dataFilePos, dataFilePos);
      this.dataFilePos = dataFilePos;
    }

    int getCurrentMapIndex() {
      return currentMapIndex;
    }

    void setCurrentMapIndex(int mapIndex) {
      logger.trace("{} mapIndex {} current mapIndex {}", this, currentMapIndex, mapIndex);
      this.currentMapIndex = mapIndex;
    }

    long getLastChunkOffset() {
      return lastChunkOffset;
    }

    void blockMerged(int mapIndex) {
      logger.debug("{} updated merging mapIndex {}", this, mapIndex);
      mapTracker.add(mapIndex);
      chunkTracker.add(mapIndex);
      lastMergedMapIndex = mapIndex;
    }

    void resetChunkTracker() {
      chunkTracker.clear();
    }

    /**
     * Appends the chunk offset to the index file and adds the map index to the chunk tracker.
     *
     * @param chunkOffset the offset of the chunk in the data file.
     * @param mapIndex the map index to be added to chunk tracker.
     */
    void updateChunkInfo(long chunkOffset, int mapIndex) throws IOException {
      try {
        logger.trace("{} index current {} updated {}", this, this.lastChunkOffset,
          chunkOffset);
        if (indexMetaUpdateFailed) {
          indexFile.getChannel().position(indexFile.getPos());
        }
        indexFile.getDos().writeLong(chunkOffset);
        // Chunk bitmap should be written to the meta file after the index file because if there are
        // any exceptions during writing the offset to the index file, meta file should not be
        // updated. If the update to the index file is successful but the update to meta file isn't
        // then the index file position is not updated.
        writeChunkTracker(mapIndex);
        indexFile.updatePos(8);
        this.lastChunkOffset = chunkOffset;
        indexMetaUpdateFailed = false;
      } catch (IOException ioe) {
        logger.warn("{} shuffleId {} reduceId {} update to index/meta failed", appId,
          shuffleId, reduceId);
        indexMetaUpdateFailed = true;
        // Any exception here is propagated to the caller and the caller can decide whether to
        // abort or not.
        throw ioe;
      }
    }

    private void writeChunkTracker(int mapIndex) throws IOException {
      if (mapIndex == -1) {
        return;
      }
      chunkTracker.add(mapIndex);
      logger.trace("{} mapIndex {} write chunk to meta file", this, mapIndex);
      if (indexMetaUpdateFailed) {
        metaFile.getChannel().position(metaFile.getPos());
      }
      chunkTracker.serialize(metaFile.getDos());
      metaFile.updatePos(metaFile.getChannel().position() - metaFile.getPos());
    }

    private void incrementIOExceptions() {
      numIOExceptions++;
    }

    private boolean shouldAbort(int ioExceptionsThresholdDuringMerge) {
      return numIOExceptions > ioExceptionsThresholdDuringMerge;
    }

    private void finalizePartition() throws IOException {
      if (dataFilePos != lastChunkOffset) {
        try {
          updateChunkInfo(dataFilePos, lastMergedMapIndex);
        } catch (IOException ioe) {
          // Any exceptions here while updating the meta files can be ignored. If the files
          // aren't successfully updated they will be truncated.
        }
      }
      // Get rid of any partial block data at the end of the file. This could either
      // be due to failure, or a request still being processed when the shuffle
      // merge gets finalized, or any exceptions while updating index/meta files.
      dataChannel.truncate(lastChunkOffset);
      indexFile.getChannel().truncate(indexFile.getPos());
      metaFile.getChannel().truncate(metaFile.getPos());
    }

    void closeAllFilesAndDeleteIfNeeded(boolean delete) {
      try {
        if (dataChannel.isOpen()) {
          dataChannel.close();
          if (delete) {
            dataFile.delete();
          }
        }
      } catch (IOException ioe) {
        logger.warn("Error closing data channel for {} shuffleId {} shuffleMergeId {}"
          + " reduceId {}", appId, shuffleId, shuffleMergeId, reduceId);
      }
      try {
        metaFile.close();
        if (delete) {
          metaFile.delete();
        }
      } catch (IOException ioe) {
        logger.warn("Error closing meta file for {} shuffleId {} shuffleMergeId {}"
          + " reduceId {}", appId, shuffleId, shuffleMergeId, reduceId);
        }
      try {
        indexFile.close();
        if (delete) {
          indexFile.delete();
        }
      } catch (IOException ioe) {
        logger.warn("Error closing index file for {} shuffleId {} shuffleMergeId {}"
          + " reduceId {}", appId, shuffleId, shuffleMergeId, reduceId);
      }
    }

    @Override
    public String toString() {
      return String.format("Application %s shuffleId %s shuffleMergeId %s reduceId %s",
        appId, shuffleId, shuffleMergeId, reduceId);
    }

    @Override
    protected void finalize() throws Throwable {
      closeAllFilesAndDeleteIfNeeded(false);
    }

    @VisibleForTesting
    MergeShuffleFile getIndexFile() {
      return indexFile;
    }

    @VisibleForTesting
    MergeShuffleFile getMetaFile() {
      return metaFile;
    }

    @VisibleForTesting
    FileChannel getDataChannel() {
      return dataChannel;
    }

    @VisibleForTesting
    int getNumIOExceptions() {
      return numIOExceptions;
    }
  }

  /**
   * Wraps all the information related to the merge directory of an application.
   */
  private static class AppPathsInfo {

    private final String[] activeLocalDirs;
    private final int subDirsPerLocalDir;

    private AppPathsInfo(
        String appId,
        String[] localDirs,
        String mergeDirectory,
        int subDirsPerLocalDir) {
      activeLocalDirs = Arrays.stream(localDirs)
        .map(localDir ->
          // Merge directory is created at the same level as block-manager directory. The list of
          // local directories that we get from ExecutorShuffleInfo are paths of each
          // block-manager directory. The mergeDirectory is the merge directory name that we get
          // from ExecutorShuffleInfo. To find out the merge directory location, we first find the
          // parent dir of the block-manager directory and then append merge directory name to it.
          Paths.get(localDir).getParent().resolve(mergeDirectory).toFile().getPath())
        .toArray(String[]::new);
      this.subDirsPerLocalDir = subDirsPerLocalDir;
      if (logger.isInfoEnabled()) {
        logger.info("Updated active local dirs {} and sub dirs {} for application {}",
          Arrays.toString(activeLocalDirs),subDirsPerLocalDir, appId);
      }
    }
  }

  /** Merged Shuffle related information tracked for a specific application attempt */
  public static class AppShuffleInfo {

    private final String appId;
    private final int attemptId;
    private final AppPathsInfo appPathsInfo;
    /**
     * 1. Key tracks shuffleId for an application
     * 2. Value tracks the AppShuffleMergePartitionsInfo having shuffleMergeId and
     * a Map tracking AppShufflePartitionInfo for all the shuffle partitions.
     */
    private final ConcurrentMap<Integer, AppShuffleMergePartitionsInfo> shuffles;

    AppShuffleInfo(
        String appId,
        int attemptId,
        AppPathsInfo appPathsInfo) {
      this.appId = appId;
      this.attemptId = attemptId;
      this.appPathsInfo = appPathsInfo;
      shuffles = new ConcurrentHashMap<>();
    }

    @VisibleForTesting
    public ConcurrentMap<Integer, AppShuffleMergePartitionsInfo> getShuffles() {
      return shuffles;
    }

    /**
     * The logic here is consistent with
     * @see [[org.apache.spark.storage.DiskBlockManager#getMergedShuffleFile(
     *      org.apache.spark.storage.BlockId, scala.Option)]]
     */
    private String getFilePath(String filename) {
      // TODO: [SPARK-33236] Change the message when this service is able to handle NM restart
      String targetFile =
        ExecutorDiskUtils.getFilePath(
          appPathsInfo.activeLocalDirs,
          appPathsInfo.subDirsPerLocalDir,
          filename);
      logger.debug("Get merged file {}", targetFile);
      return targetFile;
    }

    private String generateFileName(
        String appId,
        int shuffleId,
        int shuffleMergeId,
        int reduceId) {
      return String.format(
        "%s_%s_%d_%d_%d", MERGED_SHUFFLE_FILE_NAME_PREFIX, appId, shuffleId,
          shuffleMergeId, reduceId);
    }

    public File getMergedShuffleDataFile(
        int shuffleId,
        int shuffleMergeId,
        int reduceId) {
      String fileName = String.format("%s.data", generateFileName(appId, shuffleId,
        shuffleMergeId, reduceId));
      return new File(getFilePath(fileName));
    }

    public String getMergedShuffleIndexFilePath(
        int shuffleId,
        int shuffleMergeId,
        int reduceId) {
      String indexName = String.format("%s.index", generateFileName(appId, shuffleId,
        shuffleMergeId, reduceId));
      return getFilePath(indexName);
    }

    public File getMergedShuffleMetaFile(
        int shuffleId,
        int shuffleMergeId,
        int reduceId) {
      String metaName = String.format("%s.meta", generateFileName(appId, shuffleId,
        shuffleMergeId, reduceId));
      return new File(getFilePath(metaName));
    }
  }

  @VisibleForTesting
  static class MergeShuffleFile {
    private final FileChannel channel;
    private final DataOutputStream dos;
    private long pos;
    private File file;

    @VisibleForTesting
    MergeShuffleFile(File file) throws IOException {
      FileOutputStream fos = new FileOutputStream(file);
      channel = fos.getChannel();
      dos = new DataOutputStream(fos);
      this.file = file;
    }

    @VisibleForTesting
    MergeShuffleFile(FileChannel channel, DataOutputStream dos) {
      this.channel = channel;
      this.dos = dos;
      this.file = null;
    }

    private void updatePos(long numBytes) {
      pos += numBytes;
    }

    void close() throws IOException {
      if (channel.isOpen()) {
        dos.close();
      }
    }

    void delete() throws IOException {
      try {
        if (null != file) {
          file.delete();
        }
      } finally {
        file = null;
      }
    }

    @VisibleForTesting
    DataOutputStream getDos() {
      return dos;
    }

    @VisibleForTesting
    FileChannel getChannel() {
      return channel;
    }

    @VisibleForTesting
    long getPos() {
      return pos;
    }
  }
}
