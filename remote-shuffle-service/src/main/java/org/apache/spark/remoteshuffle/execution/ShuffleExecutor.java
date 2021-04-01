/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.execution;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.FilePathAndLength;
import org.apache.spark.remoteshuffle.common.PartitionFilePathAndLength;
import org.apache.spark.remoteshuffle.exceptions.RssShuffleCorruptedException;
import org.apache.spark.remoteshuffle.exceptions.RssShuffleStageNotStartedException;
import org.apache.spark.remoteshuffle.exceptions.RssTooMuchDataException;
import org.apache.spark.remoteshuffle.messages.*;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage;
import org.apache.spark.remoteshuffle.storage.ShuffleFileUtils;
import org.apache.spark.remoteshuffle.storage.ShuffleStorage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/***
 * This class wraps the logic to write shuffle data to underlying storage. It uses a thread 
 * pool (AsyncOperationExecutor) to execute operations in the background asynchronously.
 */
public class ShuffleExecutor {
  private static final Logger logger =
      LoggerFactory.getLogger(ShuffleExecutor.class);

  private static final long MAX_STATE_LOAD_MILLIS = TimeUnit.SECONDS.toMillis(30);

  private static final Gauge stateLoadTime = M3Stats.getDefaultScope().gauge("stateLoadTime");
  private static final Counter stateLoadWarnings =
      M3Stats.getDefaultScope().counter("stateLoadWarnings");
  private static final Counter stateLoadErrors =
      M3Stats.getDefaultScope().counter("stateLoadErrors");
  private static final Counter statePartialLoads =
      M3Stats.getDefaultScope().counter("statePartialLoads");

  private static final Gauge numLiveApplications =
      M3Stats.getDefaultScope().gauge("numLiveApplications");
  private static final Counter numExpiredApplications =
      M3Stats.getDefaultScope().counter("numExpiredApplications");

  // number of applications which are stopped due to writing too much data
  private static final Counter numTruncatedApplications =
      M3Stats.getDefaultScope().counter("numTruncatedApplications");

  // time to keep application in memory since last time is was accessed by shuffle client
  public static final long DEFAULT_APP_MEMORY_RETENTION_MILLIS = TimeUnit.HOURS.toMillis(6);

  // time to keep application files on disk
  public static final long DEFAULT_APP_FILE_RETENTION_MILLIS = TimeUnit.HOURS.toMillis(36);

  public static final long DEFAULT_APP_MAX_WRITE_BYTES = 3 * 1024L * 1024L * 1024L * 1024L; // 3TB

  private final int INTERNAL_WAKEUP_MILLIS = 1000;

  private final String rootDir;

  // This field stores states for different application
  private final ConcurrentHashMap<String, ExecutorAppState> appStates
      = new ConcurrentHashMap<>();

  // This field stores states for different shuffle stages
  private final ConcurrentHashMap<AppShuffleId, ExecutorShuffleStageState> stageStates
      = new ConcurrentHashMap<>();

  private final StateStore stateStore;

  private final ShuffleStorage storage;

  private final long appRetentionMillis;

  private final long appMaxWriteBytes;

  // a background executor service doing clean up work
  private final ScheduledExecutorService lowPriorityExecutorService = new DefaultEventLoop();

  /***
   * Create an instance.
   * @param rootDir root directory.
   */
  public ShuffleExecutor(String rootDir) {
    this(rootDir, new ShuffleFileStorage(), DEFAULT_APP_MEMORY_RETENTION_MILLIS,
        DEFAULT_APP_MAX_WRITE_BYTES);
  }

  /***
   * Create an instance.
   * @param rootDir
   */
  public ShuffleExecutor(String rootDir,
                         ShuffleStorage storage,
                         long appRetentionMillis,
                         long appMaxWriteBytes) {
    logger.info("Started with rootDir={}, storage={}, appRetentionMillis={}",
        rootDir, storage, appRetentionMillis);
    this.rootDir = rootDir;
    this.stateStore = new LocalFileStateStore(rootDir);
    this.storage = storage;
    this.appRetentionMillis = appRetentionMillis;
    this.appMaxWriteBytes = appMaxWriteBytes;

    loadStateStore();

    this.lowPriorityExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          removeExpiredApplications();
        } catch (Throwable ex) {
          M3Stats.addException(ex, this.getClass().getSimpleName());
          logger.warn("Failed to remove expired applications", ex);
        }
      }
    }, 60, 60, TimeUnit.SECONDS);
  }

  /**
   * Get root directory.
   *
   * @return
   */
  public String getRootDir() {
    return rootDir;
  }

  /**
   * Get background executor service.
   *
   * @return
   */
  public ScheduledExecutorService getLowPriorityExecutorService() {
    return lowPriorityExecutorService;
  }

  public void loadStateStore() {
    long startTime = System.currentTimeMillis();
    StateStoreLoadResult loadResult = null;
    try {
      loadResult = loadStateStoreImpl();
    } catch (Throwable ex) {
      M3Stats.addException(ex, this.getClass().getSimpleName());
      stateLoadErrors.inc(1);
      logger.warn("Failed to load state", ex);
    } finally {
      long durationMillis = System.currentTimeMillis() - startTime;
      stateLoadTime.update(durationMillis);
      logger.info(
          "Finished loading state, duration: {} milliseconds, {}",
          durationMillis, loadResult);
    }
  }

  public void registerShuffle(AppShuffleId appShuffleId, int numPartitions,
                              ShuffleWriteConfig config) {
    ExecutorShuffleStageState stageState = stageStates.get(appShuffleId);
    if (stageState != null) {
      if (stageState.getNumPartitions() != numPartitions) {
        stageState.setFileCorrupted();
        throw new RssShuffleCorruptedException(String.format(
            "Hit mismatched numPartitions (%s vs %s) for %s",
            numPartitions, stageState.getNumPartitions(), appShuffleId));
      }
      if (stageState.getWriteConfig() == null) {
        stageState.setFileCorrupted();
        throw new RssShuffleCorruptedException(String.format(
            "Hit null shuffle write config for %s",
            appShuffleId));
      }
      return;
    }

    ExecutorShuffleStageState newState = new ExecutorShuffleStageState(appShuffleId, config);
    newState.setNumMapsPartitions(numPartitions);
    ExecutorShuffleStageState oldState = stageStates.putIfAbsent(appShuffleId, newState);

    if (oldState == null) {
      // this is the first time to register this shuffle stage, add it to state store
      StagePersistentInfo info =
          new StagePersistentInfo(numPartitions, newState.getFileStartIndex(),
              newState.getWriteConfig(), newState.getFileStatus());
      stateStore.storeStageInfo(appShuffleId, info);
    }
  }

  public void startUpload(AppShuffleId appShuffleId, long taskAttemptId) {
    logger.debug("startUpload, {}, {}", appShuffleId, taskAttemptId);

    ExecutorAppState appState = updateLiveness(appShuffleId.getAppId());

    long appWriteBytes = appState.getNumWriteBytes();
    checkAppMaxWriteBytes(appShuffleId, appWriteBytes);

    ExecutorShuffleStageState stageState = getStageState(appShuffleId);
    stageState.markMapAttemptStartUpload(taskAttemptId);
  }

  /***
   * Add an operation to write shuffle record data.
   * This method will make sure the ByteBuf inside writeOp gets released.
   * @param writeOp
   */
  public void writeData(ShuffleDataWrapper writeOp) {
    // We need to make sure releasing ByteBuf inside writeOp, thus use try/finally
    boolean byteBufReleased = false;
    try {
      ExecutorAppState appState = getAppState(writeOp.getShuffleId().getAppId());
      appState.updateLivenessTimestamp();

      AppShuffleId appShuffleId = writeOp.getShuffleId();
      ByteBuf bytes = writeOp.getBytes();
      long appWriteBytes = appState.addNumWriteBytes(bytes.readableBytes());
      checkAppMaxWriteBytes(appShuffleId, appWriteBytes);

      int partition = writeOp.getPartition();

      ShufflePartitionWriter partitionWriter
          = getOrCreatePartitionWriter(appShuffleId, partition);
      // streamer.writeRecord makes sure the bytes is released, thus setting byteBufReleased to true
      byteBufReleased = true;
      partitionWriter.writeData(writeOp.getTaskAttemptId(), bytes);
    } catch (Throwable ex) {
      M3Stats.addException(ex, this.getClass().getSimpleName());
      ExecutorShuffleStageState stageState = getStageState(writeOp.getShuffleId());
      stageState.setFileCorrupted();
      stateStore.storeStageCorruption(stageState.getAppShuffleId());
      logger.warn(String.format("Set file corrupted in during writing data for shuffle stage %s",
          writeOp.getShuffleId()), ex);
      throw ex;
    } finally {
      if (!byteBufReleased) {
        if (writeOp.getBytes() != null) {
          writeOp.getBytes().release();
        }
      }
    }
  }

  /***
   * Add an operation to indicate a map task finishing upload data.
   * @return true when things are good, false when hitting stale task attempt (old task attempt tries
   * to finish upload, but there is a new task attempt uploading data, which may happen when there is
   * task retry);
   */
  public void finishUpload(AppShuffleId appShuffleId, long taskAttemptId) {
    try {
      finishUploadOperationImpl(appShuffleId, taskAttemptId);
    } catch (Throwable ex) {
      M3Stats.addException(ex, this.getClass().getSimpleName());
      ExecutorShuffleStageState stageState = getStageState(appShuffleId);
      stageState.setFileCorrupted();
      stateStore.storeStageCorruption(stageState.getAppShuffleId());
      logger.warn(String
              .format("Set file corrupted during finishing upload for shuffle stage %s", appShuffleId),
          ex);
      throw ex;
    }
  }

  private void finishUploadOperationImpl(AppShuffleId appShuffleId, long taskAttemptId) {
    ExecutorAppState appState = getAppState(appShuffleId.getAppId());
    appState.updateLivenessTimestamp();

    ExecutorShuffleStageState stageState = getStageState(appShuffleId);
    synchronized (stageState) {
      stageState.commitMapTask(taskAttemptId);
    }

    logger.info("CommitTask: {}, {}", appShuffleId, taskAttemptId);
  }

  /***
   * Stop the execution. This method will also be called by JVM shutdown hook when the server shuts down.
   */
  public void stop() {
    stop(true);
  }

  /***
   * Stop the execution. This method will also be called by JVM shutdown hook when the server shuts down.
   */
  public void stop(boolean wait) {
    // Logging mechanism (e.g. log4j, kafka) may not work in shutdown hook, thus use println() to log.
    System.out.println(
        String.format("%s Stop shuffle executor during shutdown", System.currentTimeMillis()));

    if (wait) {
      lowPriorityExecutorService.shutdown();
      try {
        lowPriorityExecutorService.awaitTermination(3, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } else {
      lowPriorityExecutorService.shutdown();
    }

    stageStates.values().parallelStream().forEach(stageState -> saveShuffleStage(stageState));

    System.out.println(
        String.format("%s Close state store during shutdown", System.currentTimeMillis()));

    stateStore.close();

    System.out.println(
        String.format("%s Stopped shuffle executor during shutdown", System.currentTimeMillis()));
  }

  /**
   * Get persisted bytes for the given partition
   *
   * @return list of files and their length
   */
  public List<FilePathAndLength> getPersistedBytes(AppShuffleId appShuffleId, int partition) {
    updateLiveness(appShuffleId.getAppId());

    ExecutorShuffleStageState stageState = getStageState(appShuffleId);
    List<FilePathAndLength> persistedBytes = stageState.getPersistedBytesSnapshot(partition);
    return persistedBytes;
  }

  public void finishShuffleStage(AppShuffleId appShuffleId) {
    ExecutorShuffleStageState stageState = getStageState(appShuffleId);
    saveShuffleStage(stageState);
  }

  /**
   * Update liveness indicator for the given application.
   *
   * @param appId
   */
  public ExecutorAppState updateLiveness(String appId) {
    ExecutorAppState appState = getAppState(appId);
    appState.updateLivenessTimestamp();
    numLiveApplications.update(appStates.size());
    return appState;
  }

  /***
   * Get config for the given shuffle stage.
   * @param appShuffleId
   * @return
   */
  public ShuffleWriteConfig getShuffleWriteConfig(AppShuffleId appShuffleId) {
    return getStageState(appShuffleId).getWriteConfig();
  }

  /***
   * Get shuffle stage status which contains map task commit status (last successful map task attempt id).
   * @param appShuffleId the shuffle id to fetch the status for.
   * @return the commit status, or null if the given shuffle does not exist.
   */
  @Nullable
  public ShuffleStageStatus getShuffleStageStatus(AppShuffleId appShuffleId) {
    ExecutorShuffleStageState stageState = stageStates.get(appShuffleId);
    if (stageState == null) {
      return new ShuffleStageStatus(ShuffleStageStatus.FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED,
          null);
    }
    return stageState.getShuffleStageStatus();
  }

  /***
   * This is a test utility method to wait for the map attempt finished upload.
   * It prints out internal state. So make sure not use it in production
   * code.
   * @param appTaskAttemptId
   * @param maxWaitMillis
   */
  public void pollAndWaitMapAttemptFinishedUpload(AppTaskAttemptId appTaskAttemptId,
                                                  long maxWaitMillis) {
    long startTime = System.currentTimeMillis();
    boolean finished = false;
    while (System.currentTimeMillis() - startTime <= maxWaitMillis) {
      printInternalState();

      ExecutorShuffleStageState stageState = getStageState(appTaskAttemptId.getAppShuffleId());
      finished = stageState.isMapAttemptCommitted(appTaskAttemptId);
      if (finished) {
        break;
      }

      try {
        Thread.sleep(INTERNAL_WAKEUP_MILLIS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    if (!finished) {
      throw new RuntimeException("AppTaskAttemptId not finished: "
          + appTaskAttemptId);
    }
  }

  /***
   * This is a test utility method to wait for the map attempt committed.
   * It prints out internal state. So make sure not use it in production
   * code.
   * @param appTaskAttemptId
   * @param maxWaitMillis
   */
  public void pollAndWaitMapAttemptCommitted(AppTaskAttemptId appTaskAttemptId,
                                             long maxWaitMillis) {
    long startTime = System.currentTimeMillis();
    boolean finished = false;
    while (System.currentTimeMillis() - startTime <= maxWaitMillis) {
      printInternalState();

      finished = getStageState(appTaskAttemptId.getAppShuffleId())
          .isMapAttemptCommitted(appTaskAttemptId);
      if (finished) {
        break;
      }

      try {
        Thread.sleep(INTERNAL_WAKEUP_MILLIS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    if (!finished) {
      throw new RuntimeException("AppTaskAttemptId not finished: "
          + appTaskAttemptId);
    }
  }

  public void checkAppMaxWriteBytes(String appId) {
    ExecutorAppState appState = getAppState(appId);
    long appWriteBytes = appState.getNumWriteBytes();
    checkAppMaxWriteBytes(appId, appWriteBytes);
  }

  private void checkAppMaxWriteBytes(AppShuffleId appShuffleId, long currentAppWriteBytes) {
    if (currentAppWriteBytes > appMaxWriteBytes) {
      numTruncatedApplications.inc(1);
      ExecutorShuffleStageState stageState = stageStates.get(appShuffleId);
      if (stageState != null) {
        stageState.setFileCorrupted();
        stateStore.storeStageCorruption(appShuffleId);
      }
      throw new RssTooMuchDataException(String.format(
          "Application %s wrote too much data (%s bytes exceeding max allowed %s)",
          appShuffleId.getAppId(), currentAppWriteBytes, appMaxWriteBytes));
    }
  }

  private void checkAppMaxWriteBytes(String appId, long currentAppWriteBytes) {
    if (currentAppWriteBytes > appMaxWriteBytes) {
      throw new RssTooMuchDataException(String.format(
          "Application %s wrote too much data (%s bytes exceeding max allowed %s)",
          appId, currentAppWriteBytes, appMaxWriteBytes));
    }
  }

  private ExecutorAppState getAppState(String appId) {
    ExecutorAppState state = appStates.get(appId);
    if (state != null) {
      return state;
    }
    ExecutorAppState newState = new ExecutorAppState(appId);
    state = appStates.putIfAbsent(appId, newState);
    if (state == null) {
      return newState;
    } else {
      return state;
    }
  }

  private ExecutorShuffleStageState getStageState(AppShuffleId appShuffleId) {
    ExecutorShuffleStageState state = stageStates.get(appShuffleId);
    if (state != null) {
      return state;
    } else {
      throw new RssShuffleStageNotStartedException("No shuffle stage found: " + appShuffleId);
    }
  }

  private void saveShuffleStage(ExecutorShuffleStageState stageState) {
    synchronized (stageState) {
      if (!stageState.isStateSaved()) {
        stageState.closeWriters();

        List<PartitionFilePathAndLength> persistedBytes = stageState.getPersistedBytesSnapshots();
        stateStore
            .storeTaskAttemptCommit(stageState.getAppShuffleId(), stageState.getCommittedTaskIds(),
                persistedBytes);
        stateStore.commit();

        stageState.markStateSaved();
      }
    }
  }

  private void printInternalState() {
    StringBuilder sb = new StringBuilder();
    sb.append("===== Internal state =====");

    sb.append(System.lineSeparator());
    sb.append("===== stageStates =====");
    convertMapToString(sb, stageStates);

    logger.info(sb.toString());
  }

  private void convertMapToString(StringBuilder sb, Map<?, ?> map) {
    map.entrySet().forEach(t -> {
      sb.append(System.lineSeparator());
      sb.append(t.getKey());
      sb.append(": ");
      sb.append(t.getValue());
    });
  }

  private ShufflePartitionWriter getOrCreatePartitionWriter(
      AppShuffleId appShuffleId,
      int partition) {
    return getStageState(appShuffleId).getOrCreateWriter(partition, rootDir, storage);
  }

  private void removeExpiredApplications() {
    long currentMillis = System.currentTimeMillis();

    List<String> expiredAppIds = new ArrayList<>();
    for (Map.Entry<String, ExecutorAppState> entry : appStates.entrySet()) {
      if (entry.getValue().getLivenessTimestamp() < currentMillis - appRetentionMillis) {
        String appId = entry.getKey();
        expiredAppIds.add(appId);
        logger.info("Found expired application: {}", appId);
      }
    }

    numExpiredApplications.inc(expiredAppIds.size());

    for (String appId : expiredAppIds) {
      appStates.remove(appId);

      List<AppShuffleId> expiredAppShuffleIds = stageStates.keySet()
          .stream()
          .filter(t -> t.getAppId().equals(appId))
          .collect(Collectors.toList());
      List<ExecutorShuffleStageState> removedAppShuffleStageStates =
          expiredAppShuffleIds.stream()
              .map(t -> stageStates.remove(t))
              .filter(t -> t != null)
              .collect(Collectors.toList());

      // Close writers in case there are still writers not closed
      removedAppShuffleStageStates.stream().forEach(t -> t.closeWriters());

      try {
        stateStore.storeAppDeletion(appId);
      } catch (Throwable ex) {
        logger.warn("Failed to add app deletion in state store when removing expired application",
            ex);
      }

      logger.info(
          "Removed expired application from internal state: {}, number of app shuffle id: {}",
          appId,
          expiredAppShuffleIds.size());
    }

    numLiveApplications.update(appStates.size());

    for (String appId : expiredAppIds) {
      String appDir = ShuffleFileUtils.getAppShuffleDir(rootDir, appId);
      try {
        logger.info("Deleting expired application directory: {}", appDir);
        storage.deleteDirectory(appDir);
      } catch (Throwable ex) {
        logger
            .warn(String.format("Failed to delete expired application directory: %s", appDir), ex);
      }
    }
  }

  private StateStoreLoadResult loadStateStoreImpl() {
    long startTime = System.currentTimeMillis();
    boolean partialLoad = false;

    long totalDataItems = 0;
    Set<String> appIds = new HashSet<>();
    Set<String> deletedApps = new HashSet<>();
    Set<AppShuffleId> stages = new HashSet<>();
    Set<AppShuffleId> corruptedStages = new HashSet<>();
    LocalFileStateStoreIterator stateItemIterator = stateStore.loadData();
    try {
      while (stateItemIterator.hasNext()) {
        BaseMessage item = stateItemIterator.next();
        loadStateImpl(item, appIds, deletedApps, stages, corruptedStages);
        totalDataItems++;

        if (System.currentTimeMillis() - startTime > MAX_STATE_LOAD_MILLIS) {
          partialLoad = true;
          statePartialLoads.inc(1);
          break;
        }
      }
    } finally {
      stateItemIterator.close();
    }

    for (AppShuffleId corruptedStage : corruptedStages) {
      ExecutorShuffleStageState stageState = stageStates.get(corruptedStage);
      if (stageState != null) {
        stageState.setFileCorrupted();
        stateStore.storeStageCorruption(stageState.getAppShuffleId());
      }
    }

    int deletedStageCount = 0;
    for (String appId : deletedApps) {
      List<AppShuffleId> appShuffleIdsToDelete = stageStates.keySet().stream()
          .filter(t -> t.getAppId().equals(appId)).collect(Collectors.toList());
      deletedStageCount += appShuffleIdsToDelete.size();
      for (AppShuffleId entry : appShuffleIdsToDelete) {
        stageStates.remove(entry);
      }
      stateStore.storeAppDeletion(appId);
    }

    stateStore.commit();

    appIds.removeAll(deletedApps);
    for (String appId : appIds) {
      ExecutorAppState appState = new ExecutorAppState(appId);
      appState.updateLivenessTimestamp();
      appStates.put(appId, appState);
    }

    return new StateStoreLoadResult(partialLoad, totalDataItems, appIds.size(), deletedApps.size(),
        stages.size(), corruptedStages.size(), deletedStageCount);
  }

  private void loadStateImpl(BaseMessage stateItem, Set<String> appIds, Set<String> deletedApps,
                             Set<AppShuffleId> stages, Set<AppShuffleId> corruptedStages) {
    if (stateItem instanceof StageInfoStateItem) {
      StageInfoStateItem stageInfoStateItem = (StageInfoStateItem) stateItem;
      AppShuffleId appShuffleId = stageInfoStateItem.getAppShuffleId();
      appIds.add(appShuffleId.getAppId());
      stages.add(appShuffleId);
      int numPartitions = stageInfoStateItem.getNumPartitions();
      ShuffleWriteConfig writeConfig = stageInfoStateItem.getWriteConfig();
      int newStartIndex = stageInfoStateItem.getFileStartIndex() + writeConfig.getNumSplits();
      byte fileStatus = stageInfoStateItem.getFileStatus();
      // check whether stage state is already set, if not, set stage state
      ExecutorShuffleStageState oldStageState = stageStates.get(appShuffleId);
      ExecutorShuffleStageState effectiveStageState;
      if (oldStageState == null) {
        // stage state is not set, add stage state
        ExecutorShuffleStageState newStageState =
            new ExecutorShuffleStageState(appShuffleId, writeConfig, newStartIndex);
        newStageState.setNumMapsPartitions(stageInfoStateItem.getNumPartitions());
        stageStates.put(appShuffleId, newStageState);
        effectiveStageState = newStageState;
      } else {
        effectiveStageState = oldStageState;
        // stage state is already set, check against values loaded from state
        if (oldStageState.getNumPartitions() != numPartitions) {
          oldStageState.setFileCorrupted();
          stateLoadWarnings.inc(1);
          logger.warn(String.format(
              "Got different numPartitions when loading state for %s, old value: %s, new value: %s",
              appShuffleId, oldStageState.getNumPartitions(), numPartitions));
          corruptedStages.add(appShuffleId);
        }
        if (!oldStageState.getWriteConfig().equals(writeConfig)) {
          oldStageState.setFileCorrupted();
          stateLoadWarnings.inc(1);
          logger.warn(String.format(
              "Got different stage write config when loading state for %s, old value: %s, new value: %s",
              appShuffleId, oldStageState.getWriteConfig(), writeConfig));
          corruptedStages.add(appShuffleId);
        }
        if (oldStageState.getFileStartIndex() < newStartIndex) {
          int oldStartIndex = oldStageState.getFileStartIndex();
          oldStageState.setFileStartIndex(newStartIndex);
          logger.info(
              "Bump file start index for {} from {} to {}, splits: {}",
              appShuffleId, oldStartIndex, newStartIndex, writeConfig.getNumSplits());
        }
      }
      if (fileStatus == ShuffleStageStatus.FILE_STATUS_CORRUPTED) {
        effectiveStageState.setFileCorrupted();
        logger.info("Mark stage {} as corrupted due to loaded state marking it as corrupted",
            appShuffleId);
        corruptedStages.add(appShuffleId);
      }
      if (corruptedStages.contains(appShuffleId)) {
        effectiveStageState.setFileCorrupted();
      }
      // store stage info to make sure next time run will use new file start index
      stateStore.storeStageInfo(appShuffleId, new StagePersistentInfo(
          effectiveStageState.getNumPartitions(),
          effectiveStageState.getFileStartIndex(),
          effectiveStageState.getWriteConfig(),
          effectiveStageState.getFileStatus()));
    } else if (stateItem instanceof AppDeletionStateItem) {
      AppDeletionStateItem appDeletionStateItem = (AppDeletionStateItem) stateItem;
      String appId = appDeletionStateItem.getAppId();
      appIds.add(appId);
      deletedApps.add(appId);
    } else if (stateItem instanceof TaskAttemptCommitStateItem) {
      TaskAttemptCommitStateItem taskAttemptCommitStateItem =
          (TaskAttemptCommitStateItem) stateItem;
      AppShuffleId appShuffleId = taskAttemptCommitStateItem.getAppShuffleId();
      appIds.add(appShuffleId.getAppId());
      stages.add(appShuffleId);
      ExecutorShuffleStageState stageState = stageStates.get(appShuffleId);
      if (stageState == null) {
        stateLoadWarnings.inc(1);
        logger.warn(String.format(
            "Got TaskAttemptCommitStateItem: %s, but there is no stage stage for %s",
            taskAttemptCommitStateItem, appShuffleId));
        corruptedStages.add(appShuffleId);
      } else {
        for (Long mapTaskAttemptId : taskAttemptCommitStateItem.getMapTaskAttemptIds()) {
          stageState.commitMapTask(mapTaskAttemptId);
        }
        stageState.addFinalizedFiles(taskAttemptCommitStateItem.getPartitionFilePathAndLengths());
        if (corruptedStages.contains(appShuffleId)) {
          stageState.setFileCorrupted();
        }
      }
    } else if (stateItem instanceof StageCorruptionStateItem) {
      StageCorruptionStateItem stageCorruptionStateItem = (StageCorruptionStateItem) stateItem;
      corruptedStages.add(stageCorruptionStateItem.getAppShuffleId());
    } else {
      stateLoadWarnings.inc(1);
      logger.warn(String.format("Got unsupported state item: %s", stateItem));
    }
  }
}
