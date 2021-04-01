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
import org.apache.spark.remoteshuffle.common.AppMapId;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.FilePathAndLength;
import org.apache.spark.remoteshuffle.messages.ShuffleStageStatus;
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage;
import org.apache.spark.remoteshuffle.tools.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ShuffleExecutorTest {
  @Test
  public void singleMapWriting() throws Exception {
    String rootDir = Files.createTempDirectory("ShuffleExecutorTest_").toString();
    ShuffleExecutor executor = new ShuffleExecutor(rootDir);

    AppShuffleId appShuffleId = new AppShuffleId(String.valueOf(System.nanoTime()), "exec1", 10);
    int numMaps = 1;
    int mapId1 = 1;
    int numPartitions = 1;

    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appShuffleId, mapId1, 0L);

    executor.registerShuffle(appShuffleId, numPartitions, new ShuffleWriteConfig((short) 1));

    executor.startUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

    {
      ShuffleDataWrapper writeOp = new ShuffleDataWrapper(
          appShuffleId, 0L, 1, serialize("str1"));
      executor.writeData(writeOp);
    }
    {
      ShuffleDataWrapper writeOp = new ShuffleDataWrapper(
          appShuffleId, 0L, 1, serialize("str2"));
      executor.writeData(writeOp);
    }

    executor.finishUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

    executor.pollAndWaitMapAttemptCommitted(new AppTaskAttemptId(appShuffleId, mapId1, 0L), 10000);

    executor.stop();

    List<String> files = Files.walk(Paths.get(rootDir)).filter(Files::isRegularFile)
        .filter(t -> !t.getFileName().toString().startsWith(LocalFileStateStore.STATE_FILE_PREFIX))
        .map(t -> t.toString()).collect(Collectors.toList());
    Assert.assertEquals(files.size(), 1);

    List<String> values = readFile(files.get(0));
    Assert.assertEquals(values.size(), 2);
    Assert.assertEquals(values, Arrays.asList("str1", "str2"));
  }

  @Test
  public void multiMapWriting() throws Exception {
    short numSplits = 10;

    String rootDir = Files.createTempDirectory("ShuffleExecutorTest_").toString();
    ShuffleExecutor executor = new ShuffleExecutor(
        rootDir, new ShuffleFileStorage(), 60 * 1000L,
        ShuffleExecutor.DEFAULT_APP_MAX_WRITE_BYTES);

    AppShuffleId appShuffleId = new AppShuffleId(String.valueOf(System.nanoTime()), "exec1", 10);
    int numMaps = 3;
    int mapId1 = 1;
    int mapId2 = 2;
    int mapId3 = 3;
    int numPartitions = 2;

    executor.registerShuffle(appShuffleId, numPartitions, new ShuffleWriteConfig(numSplits));

    List<AppMapId> appMapIds = new ArrayList<>();
    List<AppTaskAttemptId> appTaskAttemptIds = new ArrayList<>();

    List<String> writtenValues = new ArrayList<>();

    {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appShuffleId, mapId1, 0L);
      appTaskAttemptIds.add(appTaskAttemptId);

      executor
          .startUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

      String str = "str1";
      writtenValues.add(str);

      ShuffleDataWrapper writeOp = new ShuffleDataWrapper(
          appShuffleId, 0L, 1, serialize(str));
      executor.writeData(writeOp);

      executor
          .finishUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

      appMapIds.add(appTaskAttemptId.getAppMapId());
    }
    {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appShuffleId, mapId2, 1L);
      appTaskAttemptIds.add(appTaskAttemptId);

      executor
          .startUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

      String str = "str22";
      writtenValues.add(str);

      ShuffleDataWrapper writeOp = new ShuffleDataWrapper(
          appShuffleId, 0L, 1, serialize(str));
      executor.writeData(writeOp);

      executor
          .finishUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

      appMapIds.add(appTaskAttemptId.getAppMapId());
    }
    {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appShuffleId, mapId3, 2L);
      appTaskAttemptIds.add(appTaskAttemptId);

      executor
          .startUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

      String str = "str333";
      writtenValues.add(str);

      ShuffleDataWrapper writeOp = new ShuffleDataWrapper(
          appShuffleId, 0L, 2, serialize(str));
      executor.writeData(writeOp);

      executor
          .finishUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

      appMapIds.add(appTaskAttemptId.getAppMapId());
    }

    executor.pollAndWaitMapAttemptCommitted(new AppTaskAttemptId(appShuffleId, mapId1, 0L), 10000);
    executor.pollAndWaitMapAttemptCommitted(new AppTaskAttemptId(appShuffleId, mapId2, 0L), 10000);
    executor.pollAndWaitMapAttemptCommitted(new AppTaskAttemptId(appShuffleId, mapId3, 0L), 10000);

    Assert.assertEquals(appMapIds.size(), numMaps);

    executor.stop();

    List<String> files = Files.walk(Paths.get(rootDir)).filter(Files::isRegularFile)
        .filter(t -> !t.getFileName().toString().startsWith(LocalFileStateStore.STATE_FILE_PREFIX))
        .map(t -> t.toString()).collect(Collectors.toList());
    Assert.assertEquals(files.size(), 2 * numSplits);

    List<String> readValues =
        files.stream().flatMap(t -> readFile(t).stream()).collect(Collectors.toList());
    Assert.assertEquals(readValues.size(), 3);

    Collections.sort(writtenValues);
    Collections.sort(readValues);

    Assert.assertEquals(readValues, writtenValues);
  }

  @Test
  public void loadState() throws Exception {
    String rootDir = Files.createTempDirectory("ShuffleExecutorTest_").toString();
    ShuffleExecutor executor = new ShuffleExecutor(rootDir);

    AppShuffleId appShuffleId = new AppShuffleId(String.valueOf(System.nanoTime()), "exec1", 10);
    int numMaps = 1;
    int mapId1 = 1;
    int numPartitions = 1;
    long taskAttemptId = 10;
    int partition = 1;

    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appShuffleId, mapId1, taskAttemptId);

    executor.registerShuffle(appShuffleId, numPartitions, new ShuffleWriteConfig((short) 1));

    executor.startUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

    {
      ShuffleDataWrapper writeOp = new ShuffleDataWrapper(
          appShuffleId, 0L, partition, serialize("str1"));
      executor.writeData(writeOp);
    }
    {
      ShuffleDataWrapper writeOp = new ShuffleDataWrapper(
          appShuffleId, 0L, partition, serialize("str2"));
      executor.writeData(writeOp);
    }

    executor.finishUpload(appTaskAttemptId.getAppShuffleId(), appTaskAttemptId.getTaskAttemptId());

    executor
        .pollAndWaitMapAttemptCommitted(new AppTaskAttemptId(appShuffleId, mapId1, taskAttemptId),
            10000);

    executor.finishShuffleStage(appShuffleId);

    List<FilePathAndLength> writtenPartitionFiles =
        executor.getPersistedBytes(appShuffleId, partition);

    // create new executor which should load state from state files
    executor = new ShuffleExecutor(rootDir);

    ShuffleStageStatus status = executor.getShuffleStageStatus(appShuffleId);
    Assert.assertEquals(status.getFileStatus(), ShuffleStageStatus.FILE_STATUS_OK);
    Assert.assertEquals(status.getMapTaskCommitStatus().getTaskAttemptIds().size(), 1);
    Assert.assertEquals(
        status.getMapTaskCommitStatus().getTaskAttemptIds().stream().findFirst().get(),
        (Long) taskAttemptId);

    List<FilePathAndLength> pathAndLengths = executor.getPersistedBytes(appShuffleId, partition);
    Assert.assertEquals(pathAndLengths, writtenPartitionFiles);
  }

  private ByteBuf serialize(String str) {
    return Unpooled.wrappedBuffer(TestUtils.serializeString(str));
  }

  private List<String> readFile(String path) {
    List<String> result = new ArrayList<>();
    try (FileInputStream stream = new FileInputStream(path)) {
      String str = TestUtils.readString(stream);
      while (str != null) {
        result.add(str);
        str = TestUtils.readString(stream);
      }
      return result;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

}
