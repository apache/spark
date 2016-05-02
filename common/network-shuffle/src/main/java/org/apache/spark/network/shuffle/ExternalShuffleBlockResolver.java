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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
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
    if (registeredExecutorFile != null) {
      Options options = new Options();
      options.createIfMissing(false);
      options.logger(new LevelDBLogger());
      DB tmpDb;
      try {
        tmpDb = JniDBFactory.factory.open(registeredExecutorFile, options);
      } catch (NativeDB.DBException e) {
        if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
          logger.info("Creating state database at " + registeredExecutorFile);
          options.createIfMissing(true);
          try {
            tmpDb = JniDBFactory.factory.open(registeredExecutorFile, options);
          } catch (NativeDB.DBException dbExc) {
            throw new IOException("Unable to create state store", dbExc);
          }
        } else {
          // the leveldb file seems to be corrupt somehow.  Lets just blow it away and create a new
          // one, so we can keep processing new apps
          logger.error("error opening leveldb file {}.  Creating new file, will not be able to " +
            "recover state for existing applications", registeredExecutorFile, e);
          if (registeredExecutorFile.isDirectory()) {
            for (File f : registeredExecutorFile.listFiles()) {
              if (!f.delete()) {
                logger.warn("error deleting {}", f.getPath());
              }
            }
          }
          if (!registeredExecutorFile.delete()) {
            logger.warn("error deleting {}", registeredExecutorFile.getPath());
          }
          options.createIfMissing(true);
          try {
            tmpDb = JniDBFactory.factory.open(registeredExecutorFile, options);
          } catch (NativeDB.DBException dbExc) {
            throw new IOException("Unable to create state store", dbExc);
          }

        }
      }
      // if there is a version mismatch, we throw an exception, which means the service is unusable
      checkVersion(tmpDb);
      executors = reloadRegisteredExecutors(tmpDb);
      db = tmpDb;
    } else {
      db = null;
      executors = Maps.newConcurrentMap();
    }
    this.directoryCleaner = directoryCleaner;
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
        logger.debug("Successfully cleaned up directory: " + localDir);
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

    DataInputStream in = null;
    try {
      in = new DataInputStream(new FileInputStream(indexFile));
      in.skipBytes(reduceId * 8);
      long offset = in.readLong();
      long nextOffset = in.readLong();
      return new FileSegmentManagedBuffer(
        conf,
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
        ExecutorShuffleInfo shuffleInfo = mapper.readValue(e.getValue(), ExecutorShuffleInfo.class);
        registeredExecutors.put(id, shuffleInfo);
      }
    }
    return registeredExecutors;
  }

  private static class LevelDBLogger implements org.iq80.leveldb.Logger {
    private static final Logger LOG = LoggerFactory.getLogger(LevelDBLogger.class);

    @Override
    public void log(String message) {
      LOG.info(message);
    }
  }

  /**
   * Simple major.minor versioning scheme.  Any incompatible changes should be across major
   * versions.  Minor version differences are allowed -- meaning we should be able to read
   * dbs that are either earlier *or* later on the minor version.
   */
  private static void checkVersion(DB db) throws IOException {
    byte[] bytes = db.get(StoreVersion.KEY);
    if (bytes == null) {
      storeVersion(db);
    } else {
      StoreVersion version = mapper.readValue(bytes, StoreVersion.class);
      if (version.major != CURRENT_VERSION.major) {
        throw new IOException("cannot read state DB with version " + version + ", incompatible " +
          "with current version " + CURRENT_VERSION);
      }
      storeVersion(db);
    }
  }

  private static void storeVersion(DB db) throws IOException {
    db.put(StoreVersion.KEY, mapper.writeValueAsBytes(CURRENT_VERSION));
  }


  public static class StoreVersion {

    static final byte[] KEY = "StoreVersion".getBytes(StandardCharsets.UTF_8);

    public final int major;
    public final int minor;

    @JsonCreator public StoreVersion(
      @JsonProperty("major") int major,
      @JsonProperty("minor") int minor) {
      this.major = major;
      this.minor = minor;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StoreVersion that = (StoreVersion) o;

      return major == that.major && minor == that.minor;
    }

    @Override
    public int hashCode() {
      int result = major;
      result = 31 * result + minor;
      return result;
    }
  }

}
