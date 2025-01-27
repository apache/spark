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
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.roaringbitmap.RoaringBitmap;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
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
import org.apache.spark.network.shuffle.protocol.RemoveShuffleMerge;
import org.apache.spark.network.shuffledb.DB;
import org.apache.spark.network.shuffledb.DBBackend;
import org.apache.spark.network.shuffledb.DBIterator;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.apache.spark.network.util.DBProvider;
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

  private static final Cleaner CLEANER = Cleaner.create();
  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(RemoteBlockPushResolver.class);

  public static final String MERGED_SHUFFLE_FILE_NAME_PREFIX = "shuffleMerged";
  public static final String SHUFFLE_META_DELIMITER = ":";
  public static final String MERGE_DIR_KEY = "mergeDir";
  public static final String ATTEMPT_ID_KEY = "attemptId";
  private static final int UNDEFINED_ATTEMPT_ID = -1;

  /**
   * The flag for deleting all merged shuffle data.
   */
  public static final int DELETE_ALL_MERGED_SHUFFLE = -1;

  private static final String DB_KEY_DELIMITER = ";";
  private static final ErrorHandler.BlockPushErrorHandler ERROR_HANDLER = createErrorHandler();
  // ByteBuffer to respond to client upon a successful merge of a pushed block
  private static final ByteBuffer SUCCESS_RESPONSE =
    new BlockPushReturnCode(ReturnCode.SUCCESS.id(), "").toByteBuffer().asReadOnlyBuffer();

  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * This a common prefix to the key for each app shuffle partition we add to leveldb, so they
   * are easy to find, since leveldb lets you search based on prefix.
   */
  private static final String APP_ATTEMPT_SHUFFLE_FINALIZE_STATUS_KEY_PREFIX =
      "AppAttemptShuffleFinalized";
  private static final String APP_ATTEMPT_PATH_KEY_PREFIX = "AppAttemptPathInfo";
  private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);

  /**
   * A concurrent hashmap where the key is the applicationId, and the value includes
   * all the merged shuffle information for this application. AppShuffleInfo stores
   * the application attemptId, merged shuffle local directories and the metadata
   * for actively being merged shuffle partitions.
   */
  @VisibleForTesting
  final ConcurrentMap<String, AppShuffleInfo> appsShuffleInfo;

  private final ExecutorService mergedShuffleCleaner;

  private final TransportConf conf;

  private final long cleanerShutdownTimeout;

  private final int minChunkSize;
  private final int ioExceptionsThresholdDuringMerge;

  @SuppressWarnings("UnstableApiUsage")
  private final LoadingCache<String, ShuffleIndexInformation> indexCache;

  private final PushMergeMetrics pushMergeMetrics;

  @VisibleForTesting
  final File recoveryFile;

  @VisibleForTesting
  final DB db;

  @SuppressWarnings("UnstableApiUsage")
  public RemoteBlockPushResolver(TransportConf conf, File recoveryFile) throws IOException {
    this.conf = conf;
    this.appsShuffleInfo = new ConcurrentHashMap<>();
    this.mergedShuffleCleaner = Executors.newSingleThreadExecutor(
      // Add `spark` prefix because it will run in NM in Yarn mode.
      NettyUtils.createThreadFactory("spark-shuffle-merged-shuffle-directory-cleaner"));
    this.cleanerShutdownTimeout = conf.mergedShuffleCleanerShutdownTimeout();
    this.minChunkSize = conf.minChunkSizeInMergedShuffleFile();
    this.ioExceptionsThresholdDuringMerge = conf.ioExceptionsThresholdDuringMerge();
    CacheLoader<String, ShuffleIndexInformation> indexCacheLoader =
      new CacheLoader<String, ShuffleIndexInformation>() {
        @Override
        public ShuffleIndexInformation load(String filePath) throws IOException {
          return new ShuffleIndexInformation(filePath);
        }
    };
    indexCache = CacheBuilder.newBuilder()
      .maximumWeight(conf.mergedIndexCacheSize())
      .weigher((Weigher<String, ShuffleIndexInformation>)
        (filePath, indexInfo) -> indexInfo.getRetainedMemorySize())
      .build(indexCacheLoader);
    this.recoveryFile = recoveryFile;
    String dbBackendName =
      conf.get(Constants.SHUFFLE_SERVICE_DB_BACKEND, DBBackend.ROCKSDB.name());
    DBBackend dbBackend = DBBackend.byName(dbBackendName);
    db = DBProvider.initDB(dbBackend, this.recoveryFile, CURRENT_VERSION, mapper);
    if (db != null) {
      logger.info("Use {} as the implementation of {}",
        MDC.of(LogKeys.SHUFFLE_DB_BACKEND_NAME$.MODULE$, dbBackend),
        MDC.of(LogKeys.SHUFFLE_DB_BACKEND_KEY$.MODULE$, Constants.SHUFFLE_SERVICE_DB_BACKEND));
      reloadAndCleanUpAppShuffleInfo(db);
    }
    this.pushMergeMetrics = new PushMergeMetrics();
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
  @VisibleForTesting
  AppShufflePartitionInfo getOrCreateAppShufflePartitionInfo(
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
            "merge metadata",
            MDC.of(LogKeys.APP_ID$.MODULE$, appShuffleInfo.appId),
            MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, appShuffleInfo.attemptId),
            MDC.of(LogKeys.SHUFFLE_ID$.MODULE$, shuffleId),
            MDC.of(LogKeys.SHUFFLE_MERGE_ID$.MODULE$, shuffleMergeId));
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
            AppAttemptShuffleMergeId currentAppAttemptShuffleMergeId =
                new AppAttemptShuffleMergeId(appShuffleInfo.appId, appShuffleInfo.attemptId,
                    shuffleId, latestShuffleMergeId);
            logger.info("{}: creating a new shuffle merge metadata since received " +
              "shuffleMergeId {} is higher than latest shuffleMergeId {}",
              MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$,
                currentAppAttemptShuffleMergeId),
              MDC.of(LogKeys.SHUFFLE_MERGE_ID$.MODULE$, shuffleMergeId),
              MDC.of(LogKeys.LATEST_SHUFFLE_MERGE_ID$.MODULE$, latestShuffleMergeId));
            submitCleanupTask(() ->
                closeAndDeleteOutdatedPartitions(currentAppAttemptShuffleMergeId,
                    mergePartitionsInfo.shuffleMergePartitions));
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
        return newAppShufflePartitionInfo(appShuffleInfo, shuffleId, shuffleMergeId, reduceId,
            dataFile, indexFile, metaFile);
      } catch (IOException e) {
        logger.error("{} attempt {} shuffle {} shuffleMerge {}: cannot create merged shuffle " +
          "partition with data file {}, index file {}, and meta file {}",
          MDC.of(LogKeys.APP_ID$.MODULE$, appShuffleInfo.appId),
          MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, appShuffleInfo.attemptId),
          MDC.of(LogKeys.SHUFFLE_ID$.MODULE$, shuffleId),
          MDC.of(LogKeys.SHUFFLE_MERGE_ID$.MODULE$, shuffleMergeId),
          MDC.of(LogKeys.DATA_FILE$.MODULE$, dataFile.getAbsolutePath()),
          MDC.of(LogKeys.INDEX_FILE$.MODULE$, indexFile.getAbsolutePath()),
          MDC.of(LogKeys.META_FILE$.MODULE$, metaFile.getAbsolutePath()));
        throw new RuntimeException(
          String.format("Cannot initialize merged shuffle partition for appId %s shuffleId %s "
            + "shuffleMergeId %s reduceId %s", appShuffleInfo.appId, shuffleId, shuffleMergeId,
              reduceId), e);
      }
    });
  }

  @VisibleForTesting
  AppShufflePartitionInfo newAppShufflePartitionInfo(
      AppShuffleInfo appShuffleInfo,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      File dataFile,
      File indexFile,
      File metaFile) throws IOException {
    return new AppShufflePartitionInfo(new AppAttemptShuffleMergeId(
        appShuffleInfo.appId, appShuffleInfo.attemptId, shuffleId, shuffleMergeId),
        reduceId, dataFile, new MergeShuffleFile(indexFile), new MergeShuffleFile(metaFile));
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
    if (numChunks <= 0) {
      throw new RuntimeException(String.format(
          "Merged shuffle index file %s is empty", indexFile.getPath()));
    }
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
        conf, dataFile, shuffleIndexRecord.offset(), shuffleIndexRecord.length());
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

  private void removeOldApplicationAttemptsFromDb(AppShuffleInfo info) {
    if (info.attemptId != UNDEFINED_ATTEMPT_ID) {
      for (int formerAttemptId = 0; formerAttemptId < info.attemptId; formerAttemptId++) {
        removeAppAttemptPathInfoFromDB(info.appId, formerAttemptId);
      }
    }
  }

  @Override
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    logger.info("Application {} removed, cleanupLocalDirs = {}",
      MDC.of(LogKeys.APP_ID$.MODULE$, appId),
      MDC.of(LogKeys.CLEANUP_LOCAL_DIRS$.MODULE$, cleanupLocalDirs));
    // Cleanup the DB within critical section to gain the consistency between
    // DB and in-memory hashmap.
    AtomicReference<AppShuffleInfo> ref = new AtomicReference<>(null);
    appsShuffleInfo.compute(appId, (id, info) -> {
      if (null != info) {
        // Try cleaning up this application attempt local paths information
        // and also the local paths information from former attempts in DB.
        removeAppAttemptPathInfoFromDB(info.appId, info.attemptId);
        removeOldApplicationAttemptsFromDb(info);
        ref.set(info);
      }
      // Return null to remove the entry
      return null;
    });
    AppShuffleInfo appShuffleInfo = ref.get();
    if (null != appShuffleInfo) {
      submitCleanupTask(
        () -> closeAndDeletePartitionsIfNeeded(appShuffleInfo, cleanupLocalDirs));
    }
  }

  @Override
  public void removeShuffleMerge(RemoveShuffleMerge msg) {
    AppShuffleInfo appShuffleInfo = validateAndGetAppShuffleInfo(msg.appId);
    if (appShuffleInfo.attemptId != msg.appAttemptId) {
      throw new IllegalArgumentException(
          String.format("The attempt id %s in this RemoveShuffleMerge message does not match "
                  + "with the current attempt id %s stored in shuffle service for application %s",
              msg.appAttemptId, appShuffleInfo.attemptId, msg.appId));
    }
    appShuffleInfo.shuffles.compute(msg.shuffleId, (shuffleId, mergePartitionsInfo) -> {
      if (mergePartitionsInfo == null) {
        if (msg.shuffleMergeId == DELETE_ALL_MERGED_SHUFFLE) {
          return null;
        } else {
          writeAppAttemptShuffleMergeInfoToDB(new AppAttemptShuffleMergeId(
              msg.appId, msg.appAttemptId, msg.shuffleId, msg.shuffleMergeId));
          return new AppShuffleMergePartitionsInfo(msg.shuffleMergeId, true);
        }
      }
      boolean deleteCurrentMergedShuffle =
          msg.shuffleMergeId == DELETE_ALL_MERGED_SHUFFLE ||
              msg.shuffleMergeId == mergePartitionsInfo.shuffleMergeId;
      int shuffleMergeIdToDelete = msg.shuffleMergeId != DELETE_ALL_MERGED_SHUFFLE ?
          msg.shuffleMergeId : mergePartitionsInfo.shuffleMergeId;
      if (deleteCurrentMergedShuffle ||
          shuffleMergeIdToDelete > mergePartitionsInfo.shuffleMergeId) {
        AppAttemptShuffleMergeId currentAppAttemptShuffleMergeId =
            new AppAttemptShuffleMergeId(
                msg.appId, msg.appAttemptId, msg.shuffleId, mergePartitionsInfo.shuffleMergeId);
        if (!mergePartitionsInfo.isFinalized()) {
          // Clean up shuffle data before the shuffle was finalized. Close and delete all the open
          // files.
          submitCleanupTask(() ->
              closeAndDeleteOutdatedPartitions(
                  currentAppAttemptShuffleMergeId, mergePartitionsInfo.shuffleMergePartitions));
        } else {
          // Current shuffle was finalized, delete all the merged files through reduceIds set
          // in finalizeShuffleMerge method.
          submitCleanupTask(() ->
              deleteMergedFiles(currentAppAttemptShuffleMergeId, appShuffleInfo,
                  mergePartitionsInfo.getReduceIds(), false));
        }
      } else {
        throw new RuntimeException(String.format("Asked to remove old shuffle merged data for " +
                "application %s shuffleId %s shuffleMergeId %s, but current shuffleMergeId %s ",
            msg.appId, msg.shuffleId, shuffleMergeIdToDelete, mergePartitionsInfo.shuffleMergeId));
      }
      writeAppAttemptShuffleMergeInfoToDB(new AppAttemptShuffleMergeId(
          msg.appId, msg.appAttemptId, msg.shuffleId, shuffleMergeIdToDelete));
      return new AppShuffleMergePartitionsInfo(shuffleMergeIdToDelete, true);
    });
  }

  /**
   * Clean up the AppShufflePartitionInfo for a specific AppShuffleInfo.
   * If cleanupLocalDirs is true, the merged shuffle files will also be deleted.
   * The cleanup will be executed in the mergedShuffleCleaner thread.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @VisibleForTesting
  void closeAndDeletePartitionsIfNeeded(
      AppShuffleInfo appShuffleInfo,
      boolean cleanupLocalDirs) {
    appShuffleInfo.shuffles.forEach((shuffleId, shuffleInfo) -> shuffleInfo.shuffleMergePartitions
      .forEach((shuffleMergeId, partitionInfo) -> {
        synchronized (partitionInfo) {
          partitionInfo.cleanable.clean();
        }
      }));
    if (cleanupLocalDirs) {
      deleteExecutorDirs(appShuffleInfo);
    }
    removeAppShuffleInfoFromDB(appShuffleInfo);
  }

  /**
   * Remove the application attempt local paths information from the DB. This method is being
   * invoked within the lock from the ConcurrentHashmap appsShuffleInfo on the specific
   * applicationId.
   */
  @VisibleForTesting
  void removeAppAttemptPathInfoFromDB(String appId, int attemptId) {
    AppAttemptId appAttemptId = new AppAttemptId(appId, attemptId);
    if (db != null && AppsWithRecoveryDisabled.isRecoveryEnabledForApp(appId)) {
      try {
        byte[] key = getDbAppAttemptPathsKey(appAttemptId);
        db.delete(key);
      } catch (Exception e) {
        logger.error("Failed to remove the application attempt {} local path in DB", e,
          MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, appAttemptId));
      }
    }
  }

  /**
   * Remove the finalized shuffle partitions information for an application attempt from the DB
   */
  @VisibleForTesting
  void removeAppShuffleInfoFromDB(AppShuffleInfo appShuffleInfo) {
    if (db != null) {
      appShuffleInfo.shuffles.forEach((shuffleId, shuffleInfo) ->
          removeAppShufflePartitionInfoFromDB(
              new AppAttemptShuffleMergeId(
                  appShuffleInfo.appId, appShuffleInfo.attemptId,
                  shuffleId, shuffleInfo.shuffleMergeId)));
    }
  }

  /**
   * Clean up all the AppShufflePartitionInfo and the finalized shuffle partitions in DB for
   * a specific shuffleMergeId. This is done since there is a higher shuffleMergeId request made
   * for a shuffleId, therefore clean up older shuffleMergeId partitions. The cleanup will be
   * executed the mergedShuffleCleaner thread.
   */
  @VisibleForTesting
  void closeAndDeleteOutdatedPartitions(
      AppAttemptShuffleMergeId appAttemptShuffleMergeId,
      Map<Integer, AppShufflePartitionInfo> partitions) {
    removeAppShufflePartitionInfoFromDB(appAttemptShuffleMergeId);
    partitions
      .forEach((partitionId, partitionInfo) -> {
        synchronized (partitionInfo) {
          partitionInfo.cleanable.clean();
          partitionInfo.deleteAllFiles();
        }
      });
  }

  void deleteMergedFiles(
      AppAttemptShuffleMergeId appAttemptShuffleMergeId,
      AppShuffleInfo appShuffleInfo,
      int[] reduceIds,
      boolean deleteFromDB) {
    if (deleteFromDB) {
      removeAppShufflePartitionInfoFromDB(appAttemptShuffleMergeId);
    }
    int shuffleId = appAttemptShuffleMergeId.shuffleId;
    int shuffleMergeId = appAttemptShuffleMergeId.shuffleMergeId;
    int dataFilesDeleteCnt = 0;
    int indexFilesDeleteCnt = 0;
    int metaFilesDeleteCnt = 0;
    for (int reduceId : reduceIds) {
      File dataFile =
          appShuffleInfo.getMergedShuffleDataFile(shuffleId, shuffleMergeId, reduceId);
      if (dataFile.delete()) {
        dataFilesDeleteCnt++;
      }
      File indexFile = new File(
          appShuffleInfo.getMergedShuffleIndexFilePath(shuffleId, shuffleMergeId, reduceId));
      if (indexFile.delete()) {
        indexFilesDeleteCnt++;
      }
      File metaFile =
          appShuffleInfo.getMergedShuffleMetaFile(shuffleId, shuffleMergeId, reduceId);
      if (metaFile.delete()) {
        metaFilesDeleteCnt++;
      }
    }
    logger.info("Delete {} data files, {} index files, {} meta files for {}",
      MDC.of(LogKeys.NUM_DATA_FILES$.MODULE$, dataFilesDeleteCnt),
      MDC.of(LogKeys.NUM_INDEX_FILES$.MODULE$, indexFilesDeleteCnt),
      MDC.of(LogKeys.NUM_META_FILES$.MODULE$, metaFilesDeleteCnt),
      MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId));
  }

  /**
   * Remove the finalized shuffle partition information for a specific appAttemptShuffleMergeId
   * @param appAttemptShuffleMergeId
   */
  void removeAppShufflePartitionInfoFromDB(AppAttemptShuffleMergeId appAttemptShuffleMergeId) {
    if (db != null) {
      try {
        db.delete(getDbAppAttemptShufflePartitionKey(appAttemptShuffleMergeId));
      } catch (Exception e) {
        logger.error("Error deleting {} from application shuffle merged partition info in DB", e,
          MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId));
      }
    }
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
        logger.error("Failed to delete directory: {}", e,
          MDC.of(LogKeys.PATH$.MODULE$, localDir));
      }
    }
  }

  public MetricSet getMetrics() {
    return pushMergeMetrics;
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
      // The block would be considered as too late if it received after shuffle merge finalize,
      // and hence mark it as a late block push to the pushMergeMetrics
      pushMergeMetrics.lateBlockPushes.mark();
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
          pushMergeMetrics.ignoredBlockBytes.mark(buf.remaining());
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
      MDC.of(LogKeys.APP_ID$.MODULE$, msg.appId),
      MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, msg.appAttemptId),
      MDC.of(LogKeys.SHUFFLE_ID$.MODULE$, msg.shuffleId),
      MDC.of(LogKeys.SHUFFLE_MERGE_ID$.MODULE$, msg.shuffleMergeId));
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
    AppAttemptShuffleMergeId appAttemptShuffleMergeId = new AppAttemptShuffleMergeId(
        msg.appId, msg.appAttemptId, msg.shuffleId, msg.shuffleMergeId);
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
          AppAttemptShuffleMergeId currentAppAttemptShuffleMergeId = new AppAttemptShuffleMergeId(
                  msg.appId, msg.appAttemptId, msg.shuffleId, mergePartitionsInfo.shuffleMergeId);
          submitCleanupTask(() ->
              closeAndDeleteOutdatedPartitions(
                  currentAppAttemptShuffleMergeId, mergePartitionsInfo.shuffleMergePartitions));
        } else {
          // This block covers:
          //  1. finalization of determinate stage
          //  2. finalization of indeterminate stage if the shuffleMergeId related to it is the one
          //  for which the message is received.
          shuffleMergePartitionsRef.set(mergePartitionsInfo.shuffleMergePartitions);
        }
      }
      // Update the DB for the finalized shuffle
      writeAppAttemptShuffleMergeInfoToDB(appAttemptShuffleMergeId);
      // Even when the mergePartitionsInfo is null, we mark the shuffle as finalized but the results
      // sent to the driver will be empty. This can happen when the service didn't receive any
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
            if (!partition.mapTracker.isEmpty()) {
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
              "finalizing shuffle partition {}. Exception message: {}",
              MDC.of(LogKeys.APP_ID$.MODULE$, msg.appId),
              MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, msg.appAttemptId),
              MDC.of(LogKeys.SHUFFLE_ID$.MODULE$, msg.shuffleId),
              MDC.of(LogKeys.SHUFFLE_MERGE_ID$.MODULE$, msg.shuffleMergeId),
              MDC.of(LogKeys.REDUCE_ID$.MODULE$, partition.reduceId),
              MDC.of(LogKeys.EXCEPTION$.MODULE$, ioe.getMessage()));
          } finally {
            partition.cleanable.clean();
          }
        }
      }
      mergeStatuses = new MergeStatuses(msg.shuffleId, msg.shuffleMergeId,
        bitmaps.toArray(new RoaringBitmap[bitmaps.size()]), Ints.toArray(reduceIds),
        Longs.toArray(sizes));
      appShuffleInfo.shuffles.get(msg.shuffleId).setReduceIds(Ints.toArray(reduceIds));
    }
    logger.info("{} attempt {} shuffle {} shuffleMerge {}: finalization of shuffle merge completed",
      MDC.of(LogKeys.APP_ID$.MODULE$, msg.appId),
      MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$,  msg.appAttemptId),
      MDC.of(LogKeys.SHUFFLE_ID$.MODULE$, msg.shuffleId),
      MDC.of(LogKeys.SHUFFLE_MERGE_ID$.MODULE$, msg.shuffleMergeId));
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
          // Only the first ExecutorRegister message can register the merge dirs.
          // DB will also get updated with the registered local path information.
          appsShuffleInfo.computeIfAbsent(appId, id -> {
            AppPathsInfo appPathsInfo = new AppPathsInfo(appId, executorInfo.localDirs,
                mergeDir, executorInfo.subDirsPerLocalDir);
            writeAppPathsInfoToDb(appId, UNDEFINED_ATTEMPT_ID, appPathsInfo);
            return new AppShuffleInfo(appId, UNDEFINED_ATTEMPT_ID, appPathsInfo);
          });
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
              AppPathsInfo appPathsInfo = new AppPathsInfo(appId, executorInfo.localDirs,
                  mergeDir, executorInfo.subDirsPerLocalDir);
              // Clean up the outdated App Attempt local path info in the DB and
              // put the newly registered local path info from newer attempt into the DB.
              if (appShuffleInfo != null) {
                removeAppAttemptPathInfoFromDB(appId, appShuffleInfo.attemptId);
              }
              writeAppPathsInfoToDb(appId, attemptId, appPathsInfo);
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
              "application attempt registered", MDC.of(LogKeys.APP_ID$.MODULE$, appId),
              MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, appShuffleInfo.attemptId));
            // Clean up all the merge shuffle related information in the DB for the former attempt
            submitCleanupTask(
              () -> closeAndDeletePartitionsIfNeeded(appShuffleInfo, true)
            );
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
   * Shutdown mergedShuffleCleaner and close the DB during shutdown
   */
  @Override
  public void close() {
    if (!mergedShuffleCleaner.isShutdown()) {
      // SPARK-40186：Use two phases shutdown refer to
      // https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ExecutorService.html
      // Use two phases shutdown can prevent new tasks and wait for executing tasks to
      // complete gracefully, and once timeout is reached, we want to interrupt running tasks,
      // so that they fail. This is to prevent updates to shuffle state db after it is closed.
      try {
        mergedShuffleCleaner.shutdown();
        // Wait a while for existing tasks to terminate
        if (!mergedShuffleCleaner.awaitTermination(cleanerShutdownTimeout, TimeUnit.SECONDS)) {
          shutdownMergedShuffleCleanerNow();
        }
      } catch (InterruptedException e) {
        logger.info("mergedShuffleCleaner is interrupted in the process of graceful shutdown", e);
        shutdownMergedShuffleCleanerNow();
        Thread.currentThread().interrupt();
      }
    }
    if (db != null) {
      try {
        db.close();
      } catch (IOException e) {
        logger.error("Exception closing leveldb with registered app paths info and "
            + "shuffle partition info", e);
      }
    }
  }

  /**
   * Call `shutdownNow` to stop all actively executing tasks and halts the
   * processing of waiting tasks in `mergedShuffleCleaner`.
   */
  private void shutdownMergedShuffleCleanerNow() {
    try {
      List<Runnable> unfinishedTasks = mergedShuffleCleaner.shutdownNow();
      logger.warn("There are still {} tasks not completed in mergedShuffleCleaner " +
        "after {} ms.",
         MDC.of(LogKeys.COUNT$.MODULE$, unfinishedTasks.size()),
         MDC.of(LogKeys.TIMEOUT$.MODULE$, cleanerShutdownTimeout * 1000L));
      // Wait a while for tasks to respond to being cancelled
      if (!mergedShuffleCleaner.awaitTermination(cleanerShutdownTimeout, TimeUnit.SECONDS)) {
        logger.warn("mergedShuffleCleaner did not terminate in {} ms.",
          MDC.of(LogKeys.TIMEOUT$.MODULE$, cleanerShutdownTimeout * 1000L));
      }
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Write the application attempt's local path information to the DB
   */
  private void writeAppPathsInfoToDb(String appId, int attemptId, AppPathsInfo appPathsInfo) {
    if (db != null && AppsWithRecoveryDisabled.isRecoveryEnabledForApp(appId)) {
      AppAttemptId appAttemptId = new AppAttemptId(appId, attemptId);
      try {
        byte[] key = getDbAppAttemptPathsKey(appAttemptId);
        String valueStr = mapper.writeValueAsString(appPathsInfo);
        byte[] value = valueStr.getBytes(StandardCharsets.UTF_8);
        db.put(key, value);
      } catch (Exception e) {
        logger.error("Error saving registered app paths info for {}", e,
          MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, appAttemptId));
      }
    }
  }

  /**
   * Write the finalized shuffle merge partition information into the DB
   */
  private void writeAppAttemptShuffleMergeInfoToDB(
      AppAttemptShuffleMergeId appAttemptShuffleMergeId) {
    if (db != null && AppsWithRecoveryDisabled.isRecoveryEnabledForApp(
        appAttemptShuffleMergeId.appId)) {
      // Write AppAttemptShuffleMergeId into LevelDB for finalized shuffles
      try{
        byte[] dbKey = getDbAppAttemptShufflePartitionKey(appAttemptShuffleMergeId);
        db.put(dbKey, new byte[0]);
      } catch (Exception e) {
        logger.error("Error saving active app shuffle partition {}", e,
          MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId));
      }
    }
  }

  /**
   * Parse the DB key with the prefix and the expected return value type
   */
  private <T> T parseDbKey(String key, String prefix, Class<T> valueType) throws IOException {
    String json = key.substring(prefix.length() + 1);
    return mapper.readValue(json, valueType);
  }

  /**
   * Generate AppAttemptId from the DB key
   */
  private AppAttemptId parseDbAppAttemptPathsKey(String key) throws IOException {
    return parseDbKey(key, APP_ATTEMPT_PATH_KEY_PREFIX, AppAttemptId.class);
  }

  /**
   * Generate AppAttemptShuffleMergeId from the DB key
   */
  private AppAttemptShuffleMergeId parseDbAppAttemptShufflePartitionKey(
      String key) throws IOException {
    return parseDbKey(
        key, APP_ATTEMPT_SHUFFLE_FINALIZE_STATUS_KEY_PREFIX, AppAttemptShuffleMergeId.class);
  }

  /**
   * Generate the DB key with the key object and the specified string prefix
   */
  private byte[] getDbKey(Object key, String prefix) throws IOException {
    // We add a common prefix on all the keys so we can find them in the DB
    String keyJsonString = prefix + DB_KEY_DELIMITER + mapper.writeValueAsString(key);
    return keyJsonString.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Generate the DB key from AppAttemptShuffleMergeId object
   */
  private byte[] getDbAppAttemptShufflePartitionKey(
      AppAttemptShuffleMergeId appAttemptShuffleMergeId) throws IOException {
    return getDbKey(appAttemptShuffleMergeId, APP_ATTEMPT_SHUFFLE_FINALIZE_STATUS_KEY_PREFIX);
  }

  /**
   * Generate the DB key from AppAttemptId object
   */
  private byte[] getDbAppAttemptPathsKey(AppAttemptId appAttemptId) throws IOException {
    return getDbKey(appAttemptId, APP_ATTEMPT_PATH_KEY_PREFIX);
  }

  /**
   * Reload the DB to recover the meta data stored in the hashmap for merged shuffles.
   * The application attempts local paths information will be firstly reloaded, and then
   * the finalized shuffle merges will be updated.
   * This method will also try deleting dangling key/values in DB, which includes:
   * 1) Outdated application attempt local paths information as of some DB deletion failures
   * 2) The deletion of finalized shuffle merges are triggered asynchronously, there can be cases
   * that deletions miss the execution during restart. These finalized shuffle merges should have
   * no relevant application attempts local paths information registered in the DB and the hashmap.
   */
  @VisibleForTesting
  void reloadAndCleanUpAppShuffleInfo(DB db) throws IOException {
    logger.info("Reload applications merged shuffle information from DB");
    List<byte[]> dbKeysToBeRemoved = new ArrayList<>();
    dbKeysToBeRemoved.addAll(reloadActiveAppAttemptsPathInfo(db));
    dbKeysToBeRemoved.addAll(reloadFinalizedAppAttemptsShuffleMergeInfo(db));
    removeOutdatedKeyValuesInDB(dbKeysToBeRemoved);
  }

  /**
   * Reload application attempts local paths information.
   */
  @VisibleForTesting
  List<byte[]> reloadActiveAppAttemptsPathInfo(DB db) throws IOException {
    List<byte[]> dbKeysToBeRemoved = new ArrayList<>();
    if (db != null) {
      try (DBIterator itr = db.iterator()) {
        itr.seek(APP_ATTEMPT_PATH_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
        while (itr.hasNext()) {
          Map.Entry<byte[], byte[]> entry = itr.next();
          String key = new String(entry.getKey(), StandardCharsets.UTF_8);
          if (!key.startsWith(APP_ATTEMPT_PATH_KEY_PREFIX)) {
            break;
          }
          AppAttemptId appAttemptId = parseDbAppAttemptPathsKey(key);
          AppPathsInfo appPathsInfo = mapper.readValue(entry.getValue(), AppPathsInfo.class);
          logger.debug("Reloading Application paths info for application {}", appAttemptId);
          appsShuffleInfo.compute(appAttemptId.appId,
              (appId, existingAppShuffleInfo) -> {
                if (existingAppShuffleInfo == null ||
                    existingAppShuffleInfo.attemptId < appAttemptId.attemptId) {
                  if (existingAppShuffleInfo != null) {
                    AppAttemptId existingAppAttemptId = new AppAttemptId(
                        existingAppShuffleInfo.appId, existingAppShuffleInfo.attemptId);
                    try {
                      // Add the former outdated DB key to deletion list
                      dbKeysToBeRemoved.add(getDbAppAttemptPathsKey(existingAppAttemptId));
                    } catch (IOException e) {
                      logger.error("Failed to get the DB key for {}", e,
                        MDC.of(LogKeys.APP_ATTEMPT_ID$.MODULE$, existingAppAttemptId));
                    }
                  }
                  return new AppShuffleInfo(
                      appAttemptId.appId, appAttemptId.attemptId, appPathsInfo);
                } else {
                  // Add the current DB key to deletion list as it is outdated
                  dbKeysToBeRemoved.add(entry.getKey());
                  return existingAppShuffleInfo;
                }
          });
        }
      }
    }
    return dbKeysToBeRemoved;
  }

  /**
   * Reload the finalized shuffle merges.
   */
  @VisibleForTesting
  List<byte[]> reloadFinalizedAppAttemptsShuffleMergeInfo(DB db) throws IOException {
    List<byte[]> dbKeysToBeRemoved = new ArrayList<>();
    if (db != null) {
      try (DBIterator itr = db.iterator()) {
        itr.seek(APP_ATTEMPT_SHUFFLE_FINALIZE_STATUS_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
        while (itr.hasNext()) {
          Map.Entry<byte[], byte[]> entry = itr.next();
          String key = new String(entry.getKey(), StandardCharsets.UTF_8);
          if (!key.startsWith(APP_ATTEMPT_SHUFFLE_FINALIZE_STATUS_KEY_PREFIX)) {
            break;
          }
          AppAttemptShuffleMergeId partitionId = parseDbAppAttemptShufflePartitionKey(key);
          logger.debug("Reloading finalized shuffle info for partitionId {}", partitionId);
          AppShuffleInfo appShuffleInfo = appsShuffleInfo.get(partitionId.appId);
          if (appShuffleInfo != null && appShuffleInfo.attemptId == partitionId.attemptId) {
            appShuffleInfo.shuffles.compute(partitionId.shuffleId,
                (shuffleId, existingMergePartitionInfo) -> {
                  if (existingMergePartitionInfo == null ||
                      existingMergePartitionInfo.shuffleMergeId < partitionId.shuffleMergeId) {
                    if (existingMergePartitionInfo != null) {
                      AppAttemptShuffleMergeId appAttemptShuffleMergeId =
                          new AppAttemptShuffleMergeId(
                              appShuffleInfo.appId, appShuffleInfo.attemptId,
                              shuffleId, existingMergePartitionInfo.shuffleMergeId);
                      try{
                        dbKeysToBeRemoved.add(
                            getDbAppAttemptShufflePartitionKey(appAttemptShuffleMergeId));
                      } catch (Exception e) {
                        logger.error("Error getting the DB key for {}", e, MDC.of(
                          LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId));
                      }
                    }
                    return new AppShuffleMergePartitionsInfo(partitionId.shuffleMergeId, true);
                  } else {
                    dbKeysToBeRemoved.add(entry.getKey());
                    return existingMergePartitionInfo;
                  }
            });
          } else {
            dbKeysToBeRemoved.add(entry.getKey());
          }
        }
      }
    }
    return dbKeysToBeRemoved;
  }

  /**
   * Clean up DB with a list of outdated keys collected during DB reload
   */
  @VisibleForTesting
  void removeOutdatedKeyValuesInDB(List<byte[]> dbKeysToBeRemoved) {
      dbKeysToBeRemoved.forEach(
          (key) -> {
            try {
              db.delete(key);
            } catch (Exception e) {
              logger.error("Error deleting dangling key {} in DB", e,
                MDC.of(LogKeys.KEY$.MODULE$, key));
            }
          }
      );
  }

  /**
   * Submit a runnable task to the single thread cleanup executor service
   */
  @VisibleForTesting
  void submitCleanupTask(Runnable task) {
    mergedShuffleCleaner.execute(task);
  }

  /**
   * Check `mergedShuffleCleaner` is already shutdown.
   */
  @VisibleForTesting
  boolean isCleanerShutdown() {
    return mergedShuffleCleaner.isShutdown();
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

    // This collects the total pushed block bytes received in the onData method. Once these bytes
    // are not being used, we add them to the ignoredBlockBytes of the pushMergeMetrics.
    private long receivedBytes = 0;

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
        int bytesWritten = partitionInfo.dataChannel.write(buf, updatedPos);
        length += bytesWritten;
        mergeManager.pushMergeMetrics.blockBytesWritten.mark(bytesWritten);
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
      long totalSize = 0;
      for (ByteBuffer deferredBuf : deferredBufs) {
        totalSize += deferredBuf.limit();
        writeBuf(deferredBuf);
        mergeManager.pushMergeMetrics.deferredBlocks.mark(-1);
      }
      mergeManager.pushMergeMetrics.deferredBlockBytes.dec(totalSize);
      deferredBufs = null;
    }

    private void freeDeferredBufs() {
      if (deferredBufs != null && !deferredBufs.isEmpty()) {
        long totalSize = 0;
        for (ByteBuffer deferredBuf : deferredBufs) {
          totalSize += deferredBuf.limit();
          mergeManager.pushMergeMetrics.deferredBlocks.mark(-1);
        }
        mergeManager.pushMergeMetrics.deferredBlockBytes.dec(totalSize);
      }
      deferredBufs = null;
    }

    /**
     * @throws IllegalStateException if the number of IOExceptions have exceeded threshold.
     */
    private void abortIfNecessary() {
      if (partitionInfo.shouldAbort(mergeManager.ioExceptionsThresholdDuringMerge)) {
        freeDeferredBufs();
        throw new IllegalStateException(String.format("%s when merging %s",
          ErrorHandler.BlockPushErrorHandler.IOEXCEPTIONS_EXCEEDED_THRESHOLD_PREFIX,
          streamId));
      }
    }

    /**
     * Update ignoredBlockBytes in pushMergeMetrics.
     */
    private void updateIgnoredBlockBytes() {
      if (receivedBytes > 0) {
        mergeManager.pushMergeMetrics.ignoredBlockBytes.mark(receivedBytes);
        receivedBytes = 0;
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
      receivedBytes += buf.remaining();
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
        AppShuffleMergePartitionsInfo info =
            appShuffleInfo.shuffles.get(partitionInfo.appAttemptShuffleMergeId.shuffleId);
        boolean isStaleBlockPush =
            isStale(info, partitionInfo.appAttemptShuffleMergeId.shuffleMergeId);
        boolean isTooLateBlockPush = isTooLate(info, partitionInfo.reduceId);
        if (isStaleBlockPush || isTooLateBlockPush) {
          freeDeferredBufs();
          if (isTooLateBlockPush) {
            mergeManager.pushMergeMetrics.lateBlockPushes.mark();
          } else {
            mergeManager.pushMergeMetrics.staleBlockPushes.mark();
          }
          return;
        }
        // Check whether we can write to disk
        if (allowedToWrite()) {
          // Identify duplicate block generated by speculative tasks. We respond success to
          // the client in cases of duplicate even though no data is written.
          if (isDuplicateBlock()) {
            freeDeferredBufs();
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
          int deferredLen = buf.remaining();
          ByteBuffer deferredBuf = ByteBuffer.allocate(deferredLen);
          deferredBuf.put(buf);
          deferredBuf.flip();
          deferredBufs.add(deferredBuf);
          mergeManager.pushMergeMetrics.deferredBlockBytes.inc(deferredLen);
          mergeManager.pushMergeMetrics.deferredBlocks.mark();
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
        AppShuffleMergePartitionsInfo info =
            appShuffleInfo.shuffles.get(partitionInfo.appAttemptShuffleMergeId.shuffleId);
        if (isTooLate(info, partitionInfo.reduceId)) {
          freeDeferredBufs();
          mergeManager.pushMergeMetrics.lateBlockPushes.mark();
          throw new BlockPushNonFatalFailure(
            new BlockPushReturnCode(ReturnCode.TOO_LATE_BLOCK_PUSH.id(), streamId).toByteBuffer(),
            BlockPushNonFatalFailure.getErrorMsg(streamId, ReturnCode.TOO_LATE_BLOCK_PUSH));
        }
        if (isStale(info, partitionInfo.appAttemptShuffleMergeId.shuffleMergeId)) {
          freeDeferredBufs();
          mergeManager.pushMergeMetrics.staleBlockPushes.mark();
          throw new BlockPushNonFatalFailure(
            new BlockPushReturnCode(ReturnCode.STALE_BLOCK_PUSH.id(), streamId).toByteBuffer(),
            BlockPushNonFatalFailure.getErrorMsg(streamId, ReturnCode.STALE_BLOCK_PUSH));
        }

        // Check if we can commit this block
        if (allowedToWrite()) {
          // Identify duplicate block generated by speculative tasks. We respond success to
          // the client in cases of duplicate even though no data is written.
          if (isDuplicateBlock()) {
            freeDeferredBufs();
            // Since we just return without throwing exception, and the received bytes are ignored,
            // thus we need to add them to ignoredBlockBytes in pushMergeMetrics.
            updateIgnoredBlockBytes();
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
          freeDeferredBufs();
          mergeManager.pushMergeMetrics.blockAppendCollisions.mark();
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
        logger.error("Encountered issue when merging {}", throwable,
          MDC.of(LogKeys.STREAM_ID$.MODULE$, streamId));
      } else {
        logger.debug("Encountered issue when merging {}", streamId, throwable);
      }
      // The block was received by ESS but didn't get merged, so it is considered as "ignored".
      // Capturing them in ignoredBlockBytes would help measure any server side improvement.
      updateIgnoredBlockBytes();
      // Only update partitionInfo if the failure corresponds to a valid request. If the
      // request is too late, i.e. received after shuffle merge finalize or stale block push,
      // #onFailure will also be triggered, and we can just ignore. Also, if we couldn't find
      // an opportunity to write the block data to disk, we should also ignore here.
      if (isWriting) {
        synchronized (partitionInfo) {
          AppShuffleMergePartitionsInfo info =
            appShuffleInfo.shuffles.get(partitionInfo.appAttemptShuffleMergeId.shuffleId);
          if (!isTooLate(info, partitionInfo.reduceId) &&
              !isStale(info, partitionInfo.appAttemptShuffleMergeId.shuffleMergeId)) {
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
   * Encodes an application attempt ID.
   */
  public static class AppAttemptId {
    public final String appId;
    public final int attemptId;

    @JsonCreator
    public AppAttemptId(
        @JsonProperty("appId") String appId,
        @JsonProperty("attemptId") int attemptId) {
      this.appId = appId;
      this.attemptId = attemptId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppAttemptId appAttemptId = (AppAttemptId) o;
      return attemptId == appAttemptId.attemptId &&
          Objects.equals(appId, appAttemptId.appId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, attemptId);
    }

    @Override
    public String toString() {
      return String.format("Application %s_%s", appId, attemptId);
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

    private final AtomicReference<int[]> reduceIds = new AtomicReference<>(new int[0]);

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

    public void setReduceIds(int[] reduceIds) {
      this.reduceIds.set(reduceIds);
    }

    public int[] getReduceIds() {
      return this.reduceIds.get();
    }
  }

  /**
   * Encodes an application attempt shuffle merge ID.
   */
  public static class AppAttemptShuffleMergeId {
    public final String appId;
    public final int attemptId;
    public final int shuffleId;
    public final int shuffleMergeId;

    @JsonCreator
    public AppAttemptShuffleMergeId(
        @JsonProperty("appId") String appId,
        @JsonProperty("attemptId") int attemptId,
        @JsonProperty("shuffleId") int shuffleId,
        @JsonProperty("shuffleMergeId") int shuffleMergeId) {
      Preconditions.checkArgument(appId != null, "app id is null");
      this.appId = appId;
      this.attemptId = attemptId;
      this.shuffleId = shuffleId;
      this.shuffleMergeId = shuffleMergeId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppAttemptShuffleMergeId appAttemptShuffleMergeId = (AppAttemptShuffleMergeId) o;
      return attemptId == appAttemptShuffleMergeId.attemptId &&
          shuffleId == appAttemptShuffleMergeId.shuffleId &&
          shuffleMergeId == appAttemptShuffleMergeId.shuffleMergeId &&
          Objects.equals(appId, appAttemptShuffleMergeId.appId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, attemptId, shuffleId, shuffleMergeId);
    }

    @Override
    public String toString() {
      return String.format("Application %s_%s shuffleId %s shuffleMergeId %s",
        appId, attemptId, shuffleId, shuffleMergeId);
    }
  }

  /** Metadata tracked for an actively merged shuffle partition */
  public static class AppShufflePartitionInfo {
    private final AppAttemptShuffleMergeId appAttemptShuffleMergeId;
    private final int reduceId;
    private final File dataFile;
    // The merged shuffle data file channel
    public final FileChannel dataChannel;
    // The index file for a particular merged shuffle contains the chunk offsets.
    private final MergeShuffleFile indexFile;
    // The meta file for a particular merged shuffle contains all the map indices that belong to
    // every chunk. The entry per chunk is a serialized bitmap.
    private final MergeShuffleFile metaFile;
    private final Cleaner.Cleanable cleanable;
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
        AppAttemptShuffleMergeId appAttemptShuffleMergeId,
        int reduceId,
        File dataFile,
        MergeShuffleFile indexFile,
        MergeShuffleFile metaFile) throws IOException {
      this.appAttemptShuffleMergeId = appAttemptShuffleMergeId;
      this.reduceId = reduceId;
      // Create FileOutputStream with append mode set to false by default.
      // This ensures that the file is always overwritten and not appended to even after the
      // service is restarted. This is required as non-finalized merged shuffle blocks will be
      // discarded during service restart.
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
      this.cleanable = CLEANER.register(this, new ResourceCleaner(dataChannel, indexFile,
        metaFile, appAttemptShuffleMergeId, reduceId));
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
        logger.warn("{} reduceId {} update to index/meta failed",
          MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId),
          MDC.of(LogKeys.REDUCE_ID$.MODULE$, reduceId));
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
      logger.trace("{} reduceId {} truncating files data {} index {} meta {}",
          appAttemptShuffleMergeId, reduceId, lastChunkOffset,
          indexFile.getPos(), metaFile.getPos());
      dataChannel.truncate(lastChunkOffset);
      indexFile.getChannel().truncate(indexFile.getPos());
      metaFile.getChannel().truncate(metaFile.getPos());
    }

    private void deleteAllFiles() {
      if (!dataFile.delete()) {
        logger.info("Error deleting data file for {} reduceId {}",
          MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId),
          MDC.of(LogKeys.REDUCE_ID$.MODULE$, reduceId));
      }
      metaFile.delete();
      indexFile.delete();
    }

    @Override
    public String toString() {
      return String.format("Application %s_%s shuffleId %s shuffleMergeId %s reduceId %s",
          appAttemptShuffleMergeId.appId, appAttemptShuffleMergeId.attemptId,
          appAttemptShuffleMergeId.shuffleId, appAttemptShuffleMergeId.shuffleMergeId,
          reduceId);
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
    public RoaringBitmap getMapTracker() {
      return mapTracker;
    }

    @VisibleForTesting
    int getNumIOExceptions() {
      return numIOExceptions;
    }

    @VisibleForTesting
    Cleaner.Cleanable getCleanable() {
      return cleanable;
    }

    private record ResourceCleaner(
        FileChannel dataChannel,
        MergeShuffleFile indexFile,
        MergeShuffleFile metaFile,
        AppAttemptShuffleMergeId appAttemptShuffleMergeId,
        int reduceId) implements Runnable {

      @Override
      public void run() {
        closeAllFiles(dataChannel, indexFile, metaFile, appAttemptShuffleMergeId,
          reduceId);
      }

      private void closeAllFiles(
          FileChannel dataChannel,
          MergeShuffleFile indexFile,
          MergeShuffleFile metaFile,
          AppAttemptShuffleMergeId appAttemptShuffleMergeId,
          int reduceId) {
        try {
          if (dataChannel.isOpen()) {
            dataChannel.close();
          }
        } catch (IOException ioe) {
          logger.warn("Error closing data channel for {} reduceId {}",
            MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId),
            MDC.of(LogKeys.REDUCE_ID$.MODULE$, reduceId));
        }
        try {
          metaFile.close();
        } catch (IOException ioe) {
          logger.warn("Error closing meta file for {} reduceId {}",
            MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId),
            MDC.of(LogKeys.REDUCE_ID$.MODULE$, reduceId));
        }
        try {
          indexFile.close();
        } catch (IOException ioe) {
          logger.warn("Error closing index file for {} reduceId {}",
            MDC.of(LogKeys.APP_ATTEMPT_SHUFFLE_MERGE_ID$.MODULE$, appAttemptShuffleMergeId),
            MDC.of(LogKeys.REDUCE_ID$.MODULE$, reduceId));
        }
      }
    }
  }

  /**
   * Wraps all the information related to the merge directory of an application.
   */
  @VisibleForTesting
  public static class AppPathsInfo {

    @JsonFormat(shape = JsonFormat.Shape.ARRAY)
    @JsonProperty("activeLocalDirs")
    private final String[] activeLocalDirs;
    @JsonProperty("subDirsPerLocalDir")
    private final int subDirsPerLocalDir;

    @JsonCreator
    public AppPathsInfo(
        @JsonFormat(shape = JsonFormat.Shape.ARRAY)
        @JsonProperty("activeLocalDirs") String[] activeLocalDirs,
        @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir
      ) {
      this.activeLocalDirs = activeLocalDirs;
      this.subDirsPerLocalDir = subDirsPerLocalDir;
    }

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
          MDC.of(LogKeys.PATHS$.MODULE$, Arrays.toString(activeLocalDirs)),
          MDC.of(LogKeys.NUM_SUB_DIRS$.MODULE$, subDirsPerLocalDir),
          MDC.of(LogKeys.APP_ID$.MODULE$, appId));
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppPathsInfo appPathsInfo = (AppPathsInfo) o;
      return subDirsPerLocalDir == appPathsInfo.subDirsPerLocalDir &&
          Arrays.equals(activeLocalDirs, appPathsInfo.activeLocalDirs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subDirsPerLocalDir) * 41 + Arrays.hashCode(activeLocalDirs);
    }
  }

  /** Merged Shuffle related information tracked for a specific application attempt */
  public static class AppShuffleInfo {

    @VisibleForTesting
    final String appId;
    @VisibleForTesting
    final int attemptId;
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
    public AppPathsInfo getAppPathsInfo() {
      return appPathsInfo;
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
    @VisibleForTesting
    String getFilePath(String filename) {
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
  public static class MergeShuffleFile {
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

    private void updatePos(long numBytes) {
      pos += numBytes;
    }

    void close() throws IOException {
      if (channel.isOpen()) {
        dos.close();
      }
    }

    void delete() {
      try {
        if (null != file) {
          file.delete();
        }
      } finally {
        file = null;
      }
    }

    @VisibleForTesting
    public DataOutputStream getDos() {
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

  /**
   * A class that wraps all the push-based shuffle service metrics.
   */
  static class PushMergeMetrics implements MetricSet {
    // blockAppendCollisions tracks the number of shuffle push blocks collided in shuffle services
    // as another block for the same reduce partition were being written
    static final String BLOCK_APPEND_COLLISIONS_METRIC = "blockAppendCollisions";
    // lateBlockPushes tracks the number of shuffle push blocks that are received in shuffle
    // service after the specific shuffle merge has been finalized
    static final String LATE_BLOCK_PUSHES_METRIC = "lateBlockPushes";
    // blockBytesWritten tracks the size of the pushed block data written to file in bytes
    static final String BLOCK_BYTES_WRITTEN_METRIC = "blockBytesWritten";
    // deferredBlockBytes tracks the size of the current deferred block parts buffered in memory
    static final String DEFERRED_BLOCK_BYTES_METRIC = "deferredBlockBytes";
    // deferredBlocks tracks the number of the current deferred block parts buffered in memory
    static final String DEFERRED_BLOCKS_METRIC = "deferredBlocks";
    // staleBlockPushes tracks the number of stale shuffle block push requests
    static final String STALE_BLOCK_PUSHES_METRIC = "staleBlockPushes";
    // ignoredBlockBytes tracks the size of the blocks that are ignored. The pushed block data are
    // considered as ignored for these cases: 1. received after the shuffle file is finalized;
    // 2. when a request is for a duplicate block; 3. the part that ESS failed to write.
    static final String IGNORED_BLOCK_BYTES_METRIC = "ignoredBlockBytes";

    private final Map<String, Metric> allMetrics;
    private final Meter blockAppendCollisions;
    private final Meter lateBlockPushes;
    private final Meter blockBytesWritten;
    private final Counter deferredBlockBytes;
    private final Meter deferredBlocks;
    private final Meter staleBlockPushes;
    private final Meter ignoredBlockBytes;

    private PushMergeMetrics() {
      allMetrics = new HashMap<>();
      blockAppendCollisions = new Meter();
      allMetrics.put(BLOCK_APPEND_COLLISIONS_METRIC, blockAppendCollisions);
      lateBlockPushes = new Meter();
      allMetrics.put(LATE_BLOCK_PUSHES_METRIC, lateBlockPushes);
      blockBytesWritten = new Meter();
      allMetrics.put(BLOCK_BYTES_WRITTEN_METRIC, blockBytesWritten);
      deferredBlockBytes = new Counter();
      allMetrics.put(DEFERRED_BLOCK_BYTES_METRIC, deferredBlockBytes);
      deferredBlocks = new Meter();
      allMetrics.put(DEFERRED_BLOCKS_METRIC, deferredBlocks);
      staleBlockPushes = new Meter();
      allMetrics.put(STALE_BLOCK_PUSHES_METRIC, staleBlockPushes);
      ignoredBlockBytes = new Meter();
      allMetrics.put(IGNORED_BLOCK_BYTES_METRIC, ignoredBlockBytes);
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return allMetrics;
    }
  }
}
