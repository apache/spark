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

import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;
import org.apache.spark.remoteshuffle.common.*;
import org.apache.spark.remoteshuffle.exceptions.RssFileCorruptedException;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.apache.spark.remoteshuffle.messages.ShuffleStageStatus;
import org.apache.spark.remoteshuffle.storage.ShuffleFileUtils;
import org.apache.spark.remoteshuffle.storage.ShuffleStorage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/***
 * This class stores state for a shuffle stage when the shuffle executor is writing shuffle data
 */
public class ExecutorShuffleStageState {
  private static final Logger logger =
      LoggerFactory.getLogger(ExecutorShuffleStageState.class);

  private final AppShuffleId appShuffleId;
  private final ShuffleWriteConfig appConfig;

  // previously save files before server restarts
  // key is partition, value is files and their lengths
  private final Map<Integer, Map<String, Long>> finalizedFiles = new HashMap<>();

  private int fileStartIndex;

  private int numPartitions;

  // This field stores shuffle output writers. There is one writer per
  // shuffle partition.
  private final Map<Integer, ShufflePartitionWriter> writers = new HashMap<>();

  private byte fileStatus = ShuffleStageStatus.FILE_STATUS_OK;

  private final TaskAttemptCollection taskAttempts = new TaskAttemptCollection();

  private boolean stateSaved = false;

  /***
   * Create an stage state instance
   * @param appShuffleId app shuffle id
   * @param appConfig shuffle write config
   */
  public ExecutorShuffleStageState(AppShuffleId appShuffleId, ShuffleWriteConfig appConfig) {
    this(appShuffleId, appConfig, 0);
  }

  /***
   * Create an stage state instance
   * @param appShuffleId app shuffle id
   * @param appConfig shuffle write config
   * @param fileStartIndex start index for file name suffix
   */
  public ExecutorShuffleStageState(AppShuffleId appShuffleId, ShuffleWriteConfig appConfig,
                                   int fileStartIndex) {
    this.appShuffleId = appShuffleId;
    this.appConfig = appConfig;
    this.fileStartIndex = fileStartIndex;
  }

  public synchronized AppShuffleId getAppShuffleId() {
    return appShuffleId;
  }

  public synchronized ShuffleWriteConfig getWriteConfig() {
    return appConfig;
  }

  public synchronized int getFileStartIndex() {
    return fileStartIndex;
  }

  public synchronized void setFileStartIndex(int value) {
    if (value < this.fileStartIndex + appConfig.getNumSplits()) {
      throw new RssInvalidStateException(String.format(
          "New file start index %s cannot be less than current start index %s plus num of splits %s",
          value, this.fileStartIndex, appConfig.getNumSplits()
      ));
    }
    this.fileStartIndex = value;
  }

  public synchronized int getNumPartitions() {
    return numPartitions;
  }

  public synchronized void setNumMapsPartitions(int numPartitions) {
    if (this.numPartitions != 0 && this.numPartitions != numPartitions) {
      throw new RssInvalidStateException(String.format(
          "Inconsistent value for number of partitions, old value: %s, new value %s, app shuffle %s",
          this.numPartitions, numPartitions, appShuffleId));
    }

    this.numPartitions = numPartitions;
  }

  public synchronized void addFinalizedFiles(
      Collection<PartitionFilePathAndLength> finalizedFiles) {
    for (PartitionFilePathAndLength entry : finalizedFiles) {
      Map<String, Long> map = this.finalizedFiles.get(entry.getPartition());
      if (map == null) {
        map = new HashMap<>();
        this.finalizedFiles.put(entry.getPartition(), map);
      }
      long length = map.getOrDefault(entry.getPath(), 0L);
      if (entry.getLength() >= length) {
        map.put(entry.getPath(), entry.getLength());
      }
    }
  }

  public synchronized void markMapAttemptStartUpload(long taskAttemptId) {
    stateSaved = false;

    TaskAttemptIdAndState taskState = getTaskState(taskAttemptId);
    taskState.markStartUpload();
  }

  public synchronized boolean isMapAttemptCommitted(AppTaskAttemptId appTaskAttemptId) {
    return getTaskState(appTaskAttemptId.getTaskAttemptId()).isCommitted();
  }

  public synchronized ShufflePartitionWriter getOrCreateWriter(int partition, String rootDir,
                                                               ShuffleStorage storage) {
    if (partition < 0) {
      throw new RssInvalidDataException("Invalid partition: " + partition);
    }

    // TODO use array for writers instead of using map

    ShufflePartitionWriter writer = writers.get(partition);
    if (writer != null) {
      return writer;
    }

    AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
        appShuffleId, partition);

    return writers.computeIfAbsent(partition, p -> {
      String path = ShuffleFileUtils.getShuffleFilePath(
          rootDir, appShuffleId, partition);
      ShufflePartitionWriter streamer
          = new ShufflePartitionWriter(appShufflePartitionId,
          path, fileStartIndex, storage, appConfig.getNumSplits());
      return streamer;
    });
  }

  public synchronized void closeWriters() {
    for (ShufflePartitionWriter writer : writers.values()) {
      writer.close();
    }
  }

  /**
   * Get persisted bytes for the given partition
   *
   * @return list of files and their length
   */
  public synchronized List<FilePathAndLength> getPersistedBytesSnapshot(int partition) {
    List<FilePathAndLength> result = new ArrayList<>();

    Map<String, Long> map = finalizedFiles.get(partition);
    if (map != null) {
      for (Map.Entry<String, Long> entry : map.entrySet()) {
        result.add(new FilePathAndLength(entry.getKey(), entry.getValue()));
      }
    }

    ShufflePartitionWriter writer = writers.get(partition);
    if (writer == null) {
      return result;
    }

    result.addAll(writer.getPersistedBytesSnapshot());

    // Check whether there is duplicated files
    checkDuplicateFiles(result, partition);

    return result;
  }

  /**
   * Get persisted bytes for all partitions
   *
   * @return list of partition files and their length
   */
  public synchronized List<PartitionFilePathAndLength> getPersistedBytesSnapshots() {
    List<PartitionFilePathAndLength> result = new ArrayList<>();

    for (Map.Entry<Integer, Map<String, Long>> finalizedFileEntry : finalizedFiles.entrySet()) {
      int partition = finalizedFileEntry.getKey();
      Map<String, Long> files = finalizedFileEntry.getValue();
      for (Map.Entry<String, Long> fileEntry : files.entrySet()) {
        result.add(
            new PartitionFilePathAndLength(partition, fileEntry.getKey(), fileEntry.getValue()));
      }
    }

    for (Map.Entry<Integer, ShufflePartitionWriter> entry : writers.entrySet()) {
      Integer partition = entry.getKey();
      ShufflePartitionWriter writer = entry.getValue();
      List<FilePathAndLength> list = writer.getPersistedBytesSnapshot();
      for (FilePathAndLength filePathAndLength : list) {
        result.add(new PartitionFilePathAndLength(partition, filePathAndLength.getPath(),
            filePathAndLength.getLength()));
      }
    }

    // Check whether there is duplicated files
    checkDuplicateFiles(result);

    return result;
  }

  /***
   * Get total persisted bytes for all partitions.
   * @return
   */
  public synchronized long getPersistedBytes() {
    long result = 0;
    for (ShufflePartitionWriter writer : writers.values()) {
      result += writer.getPersistedBytes();
    }
    return result;
  }

  public synchronized List<Long> getCommittedTaskIds() {
    return taskAttempts.getCommittedTaskIds();
  }

  /***
   * Set last successful task attempt id for a given map id
   * @param taskId
   */
  public synchronized void commitMapTask(long taskId) {
    TaskAttemptIdAndState taskState = getTaskState(taskId);
    taskState.markCommitted();
  }

  /***
   * Get stage status, which contains map task commit status (last successful map task attempt id)
   * @return
   */
  public synchronized ShuffleStageStatus getShuffleStageStatus() {
    List<Long> committedMapTaskIds = taskAttempts.getCommittedTaskIds();
    MapTaskCommitStatus mapTaskCommitStatus =
        new MapTaskCommitStatus(new HashSet<>(committedMapTaskIds));
    return new ShuffleStageStatus(fileStatus, mapTaskCommitStatus);
  }

  public synchronized void setFileCorrupted() {
    fileStatus = ShuffleStageStatus.FILE_STATUS_CORRUPTED;
  }

  public synchronized byte getFileStatus() {
    return fileStatus;
  }

  public synchronized boolean isStateSaved() {
    return stateSaved;
  }

  public synchronized void markStateSaved() {
    this.stateSaved = true;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("ExecutorShuffleStageState %s:", appShuffleId));

    sb.append(String.format(", write config: %s", appConfig));
    sb.append(String.format(", file start index: %s", fileStartIndex));
    sb.append(String.format(", partitions: %s", numPartitions));

    sb.append(System.lineSeparator());
    sb.append("Writers:");
    for (Map.Entry<Integer, ShufflePartitionWriter> entry : writers.entrySet()) {
      sb.append(System.lineSeparator());
      sb.append(entry.getKey());
      sb.append("->");
      sb.append(entry.getValue());
    }
    return sb.toString();
  }

  private TaskAttemptIdAndState getTaskState(Long taskAttemptId) {
    return taskAttempts.getTask(taskAttemptId);
  }

  private void checkDuplicateFiles(List<FilePathAndLength> result, int partition) {
    List<String> filePaths = result.stream().map(t -> t.getPath()).collect(Collectors.toList());
    List<String> distinctFilePaths = filePaths.stream().distinct().collect(Collectors.toList());
    if (filePaths.size() != distinctFilePaths.size()) {
      throw new RssFileCorruptedException(String.format(
          "Found duplicate files in partition %s file list: %s",
          partition,
          StringUtils.join(filePaths, ',')));
    }
  }

  private void checkDuplicateFiles(List<PartitionFilePathAndLength> result) {
    List<String> filePaths = result.stream().map(t -> t.getPath()).collect(Collectors.toList());
    List<String> distinctFilePaths = filePaths.stream().distinct().collect(Collectors.toList());
    if (filePaths.size() != distinctFilePaths.size()) {
      throw new RssFileCorruptedException(String.format(
          "Found duplicate files in all partition file list: %s",
          StringUtils.join(filePaths, ',')));
    }
  }
}
