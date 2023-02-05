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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.shuffle.checksum.Cause;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffledb.DB;
import org.apache.spark.network.shuffledb.DBBackend;
import org.apache.spark.network.shuffledb.DBIterator;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.apache.spark.network.util.DBProvider;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Manages converting shuffle BlockIds into physical segments of local files, from a process outside
 * of Executors. Each Executor must register its own configuration about where it stores its files
 * (local dirs) and how (shuffle manager). The logic for retrieval of individual files is replicated
 * from Spark's IndexShuffleBlockResolver.
 */
public class ExternalShuffleBlockResolver {
  private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleBlockResolver.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * This a common prefix to the key for each app registration we stick in leveldb, so they
   * are easy to find, since leveldb lets you search based on prefix.
   */
  private static final String APP_KEY_PREFIX = "AppExecShuffleInfo";
  private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);

  // Map containing all registered executors' metadata.
  @VisibleForTesting
  final ConcurrentMap<AppExecId, ExecutorShuffleInfo> executors;

  /**
   *  Caches index file information so that we can avoid open/close the index files
   *  for each block fetch.
   */
  private final LoadingCache<String, ShuffleIndexInformation> shuffleIndexCache;

  // Single-threaded Java executor used to perform expensive recursive directory deletion.
  private final Executor directoryCleaner;

  private final TransportConf conf;

  private final boolean rddFetchEnabled;

  @VisibleForTesting
  final File registeredExecutorFile;
  @VisibleForTesting
  final DB db;

  public ExternalShuffleBlockResolver(TransportConf conf, File registeredExecutorFile)
      throws IOException {
    this(conf, registeredExecutorFile, Executors.newSingleThreadExecutor(
        // Add `spark` prefix because it will run in NM in Yarn mode.
        NettyUtils.createThreadFactory("spark-shuffle-directory-cleaner")));
  }

  // Allows tests to have more control over when directories are cleaned up.
  @VisibleForTesting
  ExternalShuffleBlockResolver(
      TransportConf conf,
      File registeredExecutorFile,
      Executor directoryCleaner) throws IOException {
    this.conf = conf;
    this.rddFetchEnabled =
      Boolean.parseBoolean(conf.get(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, "false"));
    this.registeredExecutorFile = registeredExecutorFile;
    String indexCacheSize = conf.get("spark.shuffle.service.index.cache.size", "100m");
    CacheLoader<String, ShuffleIndexInformation> indexCacheLoader =
        new CacheLoader<String, ShuffleIndexInformation>() {
          @Override
          public ShuffleIndexInformation load(String filePath) throws IOException {
            return new ShuffleIndexInformation(filePath);
          }
        };
    shuffleIndexCache = CacheBuilder.newBuilder()
      .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
      .weigher((Weigher<String, ShuffleIndexInformation>)
        (filePath, indexInfo) -> indexInfo.getRetainedMemorySize())
      .build(indexCacheLoader);
    String dbBackendName =
      conf.get(Constants.SHUFFLE_SERVICE_DB_BACKEND, DBBackend.LEVELDB.name());
    DBBackend dbBackend = DBBackend.byName(dbBackendName);
    db = DBProvider.initDB(dbBackend, this.registeredExecutorFile, CURRENT_VERSION, mapper);
    if (db != null) {
      logger.info("Use {} as the implementation of {}",
        dbBackend, Constants.SHUFFLE_SERVICE_DB_BACKEND);
      executors = reloadRegisteredExecutors(db);
    } else {
      executors = Maps.newConcurrentMap();
    }
    this.directoryCleaner = directoryCleaner;
  }

  public int getRegisteredExecutorsSize() {
    return executors.size();
  }

  /** Registers a new Executor with all the configuration we need to find its shuffle files. */
  public void registerExecutor(
      String appId,
      String execId,
      ExecutorShuffleInfo executorInfo) {
    AppExecId fullId = new AppExecId(appId, execId);
    logger.info("Registered executor {} with {}", fullId, executorInfo);
    try {
      if (db != null) {
        byte[] key = dbAppExecKey(fullId);
        byte[] value = mapper.writeValueAsString(executorInfo).getBytes(StandardCharsets.UTF_8);
        db.put(key, value);
      }
    } catch (Exception e) {
      logger.error("Error saving registered executors", e);
    }
    executors.put(fullId, executorInfo);
  }

  /**
   * Obtains a FileSegmentManagedBuffer from a single block (shuffleId, mapId, reduceId).
   */
  public ManagedBuffer getBlockData(
      String appId,
      String execId,
      int shuffleId,
      long mapId,
      int reduceId) {
    return getContinuousBlocksData(appId, execId, shuffleId, mapId, reduceId, reduceId + 1);
  }

  /**
   * Obtains a FileSegmentManagedBuffer from (shuffleId, mapId, [startReduceId, endReduceId)).
   * We make assumptions about how the hash and sort based shuffles store their data.
   */
  public ManagedBuffer getContinuousBlocksData(
      String appId,
      String execId,
      int shuffleId,
      long mapId,
      int startReduceId,
      int endReduceId) {
    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }
    return getSortBasedShuffleBlockData(executor, shuffleId, mapId, startReduceId, endReduceId);
  }

  public ManagedBuffer getRddBlockData(
      String appId,
      String execId,
      int rddId,
      int splitIndex) {
    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }
    return getDiskPersistedRddBlockData(executor, rddId, splitIndex);
  }
  /**
   * Removes our metadata of all executors registered for the given application, and optionally
   * also deletes the local directories associated with the executors of that application in a
   * separate thread.
   *
   * It is not valid to call registerExecutor() for an executor with this appId after invoking
   * this method.
   */
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    logger.info("Application {} removed, cleanupLocalDirs = {}", appId, cleanupLocalDirs);
    Iterator<Map.Entry<AppExecId, ExecutorShuffleInfo>> it = executors.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<AppExecId, ExecutorShuffleInfo> entry = it.next();
      AppExecId fullId = entry.getKey();
      final ExecutorShuffleInfo executor = entry.getValue();

      // Only touch executors associated with the appId that was removed.
      if (appId.equals(fullId.appId)) {
        it.remove();
        if (db != null) {
          try {
            db.delete(dbAppExecKey(fullId));
          } catch (IOException e) {
            logger.error("Error deleting {} from executor state db", appId, e);
          }
        }

        if (cleanupLocalDirs) {
          logger.info("Cleaning up executor {}'s {} local dirs", fullId, executor.localDirs.length);

          // Execute the actual deletion in a different thread, as it may take some time.
          directoryCleaner.execute(() -> deleteExecutorDirs(executor.localDirs));
        }
      }
    }
  }

  /**
   * Removes all the files which cannot be served by the external shuffle service (non-shuffle and
   * non-RDD files) in any local directories associated with the finished executor.
   */
  public void executorRemoved(String executorId, String appId) {
    logger.info("Clean up non-shuffle and non-RDD files associated with the finished executor {}",
      executorId);
    AppExecId fullId = new AppExecId(appId, executorId);
    final ExecutorShuffleInfo executor = executors.get(fullId);
    if (executor == null) {
      // Executor not registered, skip clean up of the local directories.
      logger.info("Executor is not registered (appId={}, execId={})", appId, executorId);
    } else {
      logger.info("Cleaning up non-shuffle and non-RDD files in executor {}'s {} local dirs",
        fullId, executor.localDirs.length);

      // Execute the actual deletion in a different thread, as it may take some time.
      directoryCleaner.execute(() -> deleteNonShuffleServiceServedFiles(executor.localDirs));
    }
  }

  /**
   * Synchronously deletes each directory one at a time.
   * Should be executed in its own thread, as this may take a long time.
   */
  private void deleteExecutorDirs(String[] dirs) {
    for (String localDir : dirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir));
        logger.debug("Successfully cleaned up directory: {}", localDir);
      } catch (Exception e) {
        logger.error("Failed to delete directory: " + localDir, e);
      }
    }
  }

  /**
   * Synchronously deletes files not served by shuffle service in each directory recursively.
   * Should be executed in its own thread, as this may take a long time.
   */
  private void deleteNonShuffleServiceServedFiles(String[] dirs) {
    FilenameFilter filter = (dir, name) -> {
      // Don't delete shuffle data, shuffle index files or cached RDD files.
      return !name.endsWith(".index") && !name.endsWith(".data")
        && (!rddFetchEnabled || !name.startsWith("rdd_"));
    };

    for (String localDir : dirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir), filter);
        logger.debug("Successfully cleaned up files not served by shuffle service in directory: {}",
          localDir);
      } catch (Exception e) {
        logger.error("Failed to delete files not served by shuffle service in directory: "
          + localDir, e);
      }
    }
  }

  /**
   * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
   * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockResolver,
   * and the block id format is from ShuffleDataBlockId and ShuffleIndexBlockId.
   */
  private ManagedBuffer getSortBasedShuffleBlockData(
    ExecutorShuffleInfo executor, int shuffleId, long mapId, int startReduceId, int endReduceId) {
    String indexFilePath =
      ExecutorDiskUtils.getFilePath(
        executor.localDirs,
        executor.subDirsPerLocalDir,
        "shuffle_" + shuffleId + "_" + mapId + "_0.index");

    try {
      ShuffleIndexInformation shuffleIndexInformation = shuffleIndexCache.get(indexFilePath);
      ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(
        startReduceId, endReduceId);
      return new FileSegmentManagedBuffer(
        conf,
        new File(
          ExecutorDiskUtils.getFilePath(
            executor.localDirs,
            executor.subDirsPerLocalDir,
            "shuffle_" + shuffleId + "_" + mapId + "_0.data")),
        shuffleIndexRecord.getOffset(),
        shuffleIndexRecord.getLength());
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to open file: " + indexFilePath, e);
    }
  }

  public ManagedBuffer getDiskPersistedRddBlockData(
      ExecutorShuffleInfo executor, int rddId, int splitIndex) {
    File file = new File(
      ExecutorDiskUtils.getFilePath(
        executor.localDirs, executor.subDirsPerLocalDir, "rdd_" + rddId + "_" + splitIndex));
    long fileLength = file.length();
    ManagedBuffer res = null;
    if (file.exists()) {
      res = new FileSegmentManagedBuffer(conf, file, 0, fileLength);
    }
    return res;
  }

  void close() {
    if (db != null) {
      try {
        db.close();
      } catch (IOException e) {
        logger.error("Exception closing leveldb with registered executors", e);
      }
    }
  }

  public int removeBlocks(String appId, String execId, String[] blockIds) {
    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }
    int numRemovedBlocks = 0;
    for (String blockId : blockIds) {
      File file = new File(
        ExecutorDiskUtils.getFilePath(executor.localDirs, executor.subDirsPerLocalDir, blockId));
      if (file.delete()) {
        numRemovedBlocks++;
      } else {
        logger.warn("Failed to delete block: " + file.getAbsolutePath());
      }
    }
    return numRemovedBlocks;
  }

  public Map<String, String[]> getLocalDirs(String appId, Set<String> execIds) {
    return execIds.stream()
      .map(exec -> {
        ExecutorShuffleInfo info = executors.get(new AppExecId(appId, exec));
        if (info == null) {
          throw new RuntimeException(
            String.format("Executor is not registered (appId=%s, execId=%s)", appId, exec));
        }
        return Pair.of(exec, info.localDirs);
      })
      .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Diagnose the possible cause of the shuffle data corruption by verifying the shuffle checksums
   */
  public Cause diagnoseShuffleBlockCorruption(
      String appId,
      String execId,
      int shuffleId,
      long mapId,
      int reduceId,
      long checksumByReader,
      String algorithm) {
    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    // This should be in sync with IndexShuffleBlockResolver.getChecksumFile
    String fileName = "shuffle_" + shuffleId + "_" + mapId + "_0.checksum." + algorithm;
    File checksumFile = new File(
      ExecutorDiskUtils.getFilePath(executor.localDirs, executor.subDirsPerLocalDir, fileName));
    ManagedBuffer data = getBlockData(appId, execId, shuffleId, mapId, reduceId);
    return ShuffleChecksumHelper.diagnoseCorruption(
      algorithm, checksumFile, reduceId, data, checksumByReader);
  }

  /** Simply encodes an executor's full ID, which is appId + execId. */
  public static class AppExecId {
    public final String appId;
    public final String execId;

    @JsonCreator
    public AppExecId(@JsonProperty("appId") String appId, @JsonProperty("execId") String execId) {
      this.appId = appId;
      this.execId = execId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppExecId appExecId = (AppExecId) o;
      return Objects.equals(appId, appExecId.appId) && Objects.equals(execId, appExecId.execId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, execId);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("appId", appId)
        .append("execId", execId)
        .toString();
    }
  }

  private static byte[] dbAppExecKey(AppExecId appExecId) throws IOException {
    // we stick a common prefix on all the keys so we can find them in the DB
    String appExecJson = mapper.writeValueAsString(appExecId);
    String key = (APP_KEY_PREFIX + ";" + appExecJson);
    return key.getBytes(StandardCharsets.UTF_8);
  }

  private static AppExecId parseDbAppExecKey(String s) throws IOException {
    if (!s.startsWith(APP_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + APP_KEY_PREFIX);
    }
    String json = s.substring(APP_KEY_PREFIX.length() + 1);
    AppExecId parsed = mapper.readValue(json, AppExecId.class);
    return parsed;
  }

  @VisibleForTesting
  static ConcurrentMap<AppExecId, ExecutorShuffleInfo> reloadRegisteredExecutors(DB db)
      throws IOException {
    ConcurrentMap<AppExecId, ExecutorShuffleInfo> registeredExecutors = Maps.newConcurrentMap();
    if (db != null) {
      try (DBIterator itr = db.iterator()) {
        itr.seek(APP_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
        while (itr.hasNext()) {
          Map.Entry<byte[], byte[]> e = itr.next();
          String key = new String(e.getKey(), StandardCharsets.UTF_8);
          if (!key.startsWith(APP_KEY_PREFIX)) {
            break;
          }
          AppExecId id = parseDbAppExecKey(key);
          logger.info("Reloading registered executors: " +  id.toString());
          ExecutorShuffleInfo shuffleInfo =
            mapper.readValue(e.getValue(), ExecutorShuffleInfo.class);
          registeredExecutors.put(id, shuffleInfo);
        }
      }
    }
    return registeredExecutors;
  }
}
