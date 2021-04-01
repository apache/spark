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

import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.PartitionFilePathAndLength;
import org.apache.spark.remoteshuffle.exceptions.RssFileCorruptedException;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.apache.spark.remoteshuffle.messages.*;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import org.apache.spark.remoteshuffle.util.FileUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalFileStateStore implements StateStore {
  private static final Logger logger = LoggerFactory.getLogger(LocalFileStateStore.class);

  public final static String STATE_DIR_NAME = "state";
  public final static String STATE_FILE_PREFIX = "v1_";

  public final static long DEFAULT_ROTATION_MILLIS = 60 * 60 * 1000L;
  public final static long DEFAULT_RETENTION_MILLIS =
      ShuffleExecutor.DEFAULT_APP_FILE_RETENTION_MILLIS;

  private final static TimeZone utcTimeZone;
  private final static DateFormat dateFormat;

  static {
    utcTimeZone = TimeZone.getTimeZone("UTC");
    dateFormat = new SimpleDateFormat("yyyyMMddHH");
    dateFormat.setTimeZone(utcTimeZone);
  }

  private final String stateDir;
  private final long fileRotationMillis;
  private final long fileRetentionMillis;

  private String currentFilePath;
  private FileOutputStream currentFileStream;
  private long currentFileCreateTime = 0;

  private boolean closed = false;

  public LocalFileStateStore(String rootDir) {
    this(rootDir, DEFAULT_ROTATION_MILLIS, DEFAULT_RETENTION_MILLIS);
  }

  public LocalFileStateStore(String rootDir, long fileRotationMillis, long fileRetentionMillis) {
    this.stateDir = Paths.get(rootDir, STATE_DIR_NAME).toString();
    this.fileRotationMillis = fileRotationMillis;
    this.fileRetentionMillis = fileRetentionMillis;
    Paths.get(rootDir, STATE_DIR_NAME).toFile().mkdirs();
    createNewFileIfNecessary();
  }

  public void storeStageInfo(AppShuffleId appShuffleId, StagePersistentInfo info) {
    StageInfoStateItem item = new StageInfoStateItem(appShuffleId,
        info.getNumPartitions(),
        info.getFileStartIndex(),
        info.getShuffleWriteConfig(),
        info.getFileStatus());
    writeState(item);
  }

  public void storeTaskAttemptCommit(AppShuffleId appShuffleId,
                                     Collection<Long> committedTaskAttempts,
                                     Collection<PartitionFilePathAndLength> partitionFilePathAndLengths) {
    TaskAttemptCommitStateItem item =
        new TaskAttemptCommitStateItem(appShuffleId, committedTaskAttempts,
            partitionFilePathAndLengths);
    writeState(item);
  }

  public void storeAppDeletion(String appId) {
    AppDeletionStateItem item = new AppDeletionStateItem(appId);
    writeState(item);
  }

  public void storeStageCorruption(AppShuffleId appShuffleId) {
    StageCorruptionStateItem item = new StageCorruptionStateItem(appShuffleId);
    writeState(item);
  }

  public void commit() {
    synchronized (this) {
      try {
        currentFileStream.flush();
      } catch (IOException e) {
        throw new RssFileCorruptedException(
            String.format("Failed to flush state file %s", currentFilePath));
      }
    }

    if (System.currentTimeMillis() - currentFileCreateTime >= fileRotationMillis) {
      deleteOldFiles();
    }

    createNewFileIfNecessary();
  }

  public LocalFileStateStoreIterator loadData() {
    List<String> files;
    try (Stream<Path> stream = Files.list(Paths.get(stateDir))) {
      files = stream.sorted(new Comparator<Path>() {
        @Override
        public int compare(Path o1, Path o2) {
          return StringUtils.compare(o1.toString(), o2.toString());
        }
      })
          .map(Path::toString)
          .collect(Collectors.toList());
    } catch (IOException e) {
      logger.warn(String.format("Failed to load state from directory %s", stateDir), e);
      files = Collections.emptyList();
    }
    logger
        .info(String.format("Creating iterator to load state: %s", StringUtils.join(files, ',')));
    return new LocalFileStateStoreIterator(files);
  }

  @Override
  public void close() {
    try {
      synchronized (this) {
        closed = true;
        closeFileNoLock();
      }
    } catch (Throwable ex) {
      logger.warn("Failed to close state file", ex);
    }
  }

  @Override
  public String toString() {
    synchronized (this) {
      return "StateStore{" +
          "currentFilePath='" + currentFilePath + '\'' +
          '}';
    }
  }

  private void createNewFileIfNecessary() {
    synchronized (this) {
      if (closed) {
        logger.info(String.format("State store already closed, do not create new file, %s", this));
        return;
      }

      if (System.currentTimeMillis() - currentFileCreateTime >= fileRotationMillis) {
        closeFileNoLock();

        String fileBaseName =
            String.format("%s%s", STATE_FILE_PREFIX, dateFormat.format(new Date()));
        for (int i = 0; i < 10000; i++) {
          String fileName = String.format("%s.%04d", fileBaseName, i);
          Path path = Paths.get(stateDir, fileName);
          if (!Files.exists(path)) {
            try {
              String pathStr = path.toString();
              currentFileStream = new FileOutputStream(pathStr, true);
              currentFilePath = pathStr;
              currentFileCreateTime = System.currentTimeMillis();
              logger.info(String.format("Created state file: %s", pathStr));
              return;
            } catch (FileNotFoundException e) {
              throw new RssFileCorruptedException(
                  String.format("Failed to create state file: %s", path));
            }
          }
        }

        throw new RssInvalidStateException("Failed to create new state file");
      }
    }
  }

  private void deleteOldFiles() {
    try {
      FileUtils.cleanupOldFiles(stateDir, System.currentTimeMillis() - fileRetentionMillis);
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to clean up old state files in %s", stateDir), ex);
    }
  }

  private void writeState(BaseMessage item) {
    ByteBuf buf = Unpooled.buffer();
    byte[] bytes;
    try {
      item.serialize(buf);
      bytes = ByteBufUtils.readBytes(buf);
    } finally {
      buf.release();
    }
    byte[] messageTypeBytes = ByteBufUtils.convertIntToBytes(item.getMessageType());
    byte[] lengthBytes = ByteBufUtils.convertIntToBytes(bytes.length);
    synchronized (this) {
      try {
        currentFileStream.write(messageTypeBytes);
        currentFileStream.write(lengthBytes);
        currentFileStream.write(bytes);
      } catch (IOException e) {
        throw new RssFileCorruptedException(
            String.format("Failed to write %s to state file %s", item, this));
      }
    }
  }

  private void closeFileNoLock() {
    if (currentFileStream != null) {
      logger.info(String.format("Closing state file: %s", currentFilePath));
      try {
        currentFileStream.close();
        currentFileStream = null;
      } catch (IOException e) {
        throw new RssFileCorruptedException(String
            .format("Failed to close old state file %s when trying to create new one",
                currentFilePath));
      }
      currentFilePath = null;
      currentFileCreateTime = 0;
    }
  }
}
