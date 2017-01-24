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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.LevelDBProvider;
import org.apache.spark.network.util.LevelDBProvider.StoreVersion;
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
  private final LoadingCache<File, ShuffleIndexInformation> shuffleIndexCache;

  // Single-threaded Java executor used to perform expensive recursive directory deletion.
  private final Executor directoryCleaner;

  private final TransportConf conf;

  @VisibleForTesting
  final File registeredExecutorFile;
  @VisibleForTesting
  final DB db;

  private final List<String> knownManagers = Arrays.asList(
    "org.apache.spark.shuffle.sort.SortShuffleManager",
    "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager");

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
    this.registeredExecutorFile = registeredExecutorFile;
    int indexCacheEntries = conf.getInt("spark.shuffle.service.index.cache.entries", 1024);
    CacheLoader<File, ShuffleIndexInformation> indexCacheLoader =
        new CacheLoader<File, ShuffleIndexInformation>() {
          public ShuffleIndexInformation load(File file) throws IOException {
            return new ShuffleIndexInformation(file);
          }
        };
    shuffleIndexCache = CacheBuilder.newBuilder()
                                    .maximumSize(indexCacheEntries).build(indexCacheLoader);
    db = LevelDBProvider.initLevelDB(this.registeredExecutorFile, CURRENT_VERSION, mapper);
    if (db != null) {
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
    if (!knownManagers.contains(executorInfo.shuffleManager)) {
      throw new UnsupportedOperationException(
        "Unsupported shuffle manager of executor: " + executorInfo);
    }
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

    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }

    return getSortBasedShuffleBlockData(executor, shuffleId, mapId, reduceId);
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
          directoryCleaner.execute(new Runnable() {
            @Override
            public void run() {
              deleteExecutorDirs(executor.localDirs);
            }
          });
        }
      }
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
   * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
   * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockResolver,
   * and the block id format is from ShuffleDataBlockId and ShuffleIndexBlockId.
   */
  private ManagedBuffer getSortBasedShuffleBlockData(
    ExecutorShuffleInfo executor, int shuffleId, int mapId, int reduceId) {
    File indexFile = getFile(executor.localDirs, executor.subDirsPerLocalDir,
      "shuffle_" + shuffleId + "_" + mapId + "_0.index");

    try {
      ShuffleIndexInformation shuffleIndexInformation = shuffleIndexCache.get(indexFile);
      ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(reduceId);
      return new FileSegmentManagedBuffer(
        conf,
        getFile(executor.localDirs, executor.subDirsPerLocalDir,
          "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
        shuffleIndexRecord.getOffset(),
        shuffleIndexRecord.getLength());
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to open file: " + indexFile, e);
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

  void close() {
    if (db != null) {
      try {
        db.close();
      } catch (IOException e) {
        logger.error("Exception closing leveldb with registered executors", e);
      }
    }
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
      return Objects.equal(appId, appExecId.appId) && Objects.equal(execId, appExecId.execId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId, execId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("appId", appId)
        .add("execId", execId)
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
      DBIterator itr = db.iterator();
      itr.seek(APP_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
      while (itr.hasNext()) {
        Map.Entry<byte[], byte[]> e = itr.next();
        String key = new String(e.getKey(), StandardCharsets.UTF_8);
        if (!key.startsWith(APP_KEY_PREFIX)) {
          break;
        }
        AppExecId id = parseDbAppExecKey(key);
        logger.info("Reloading registered executors: " +  id.toString());
        ExecutorShuffleInfo shuffleInfo = mapper.readValue(e.getValue(), ExecutorShuffleInfo.class);
        registeredExecutors.put(id, shuffleInfo);
      }
    }
    return registeredExecutors;
  }
}
