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
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.PartitionFilePathAndLength;
import org.apache.spark.remoteshuffle.messages.BaseMessage;
import org.apache.spark.remoteshuffle.messages.ShuffleStageStatus;
import org.apache.spark.remoteshuffle.messages.StageInfoStateItem;
import org.apache.spark.remoteshuffle.messages.TaskAttemptCommitStateItem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class LocalFileStateStoreTest {

  @Test
  public void testConstructor() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    new LocalFileStateStore(tempPath.toString());

    List<Path> fileList =
        Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
            .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 1);
    Assert.assertTrue(fileList.get(0).toString().endsWith(".0000"));

    new LocalFileStateStore(tempPath.toString());

    fileList = Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
        .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 2);
    Assert.assertTrue(fileList.get(0).toString().endsWith(".0000") ||
        fileList.get(0).toString().endsWith(".0001"));
    Assert.assertTrue(fileList.get(1).toString().endsWith(".0000") ||
        fileList.get(1).toString().endsWith(".0001"));
  }

  @Test
  public void testFileRotation() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    long fileRotationMillis = 0;
    LocalFileStateStore stateStore =
        new LocalFileStateStore(tempPath.toString(), fileRotationMillis,
            LocalFileStateStore.DEFAULT_RETENTION_MILLIS);

    List<Path> fileList =
        Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
            .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 1);
    Assert.assertTrue(fileList.get(0).toString().endsWith(".0000"));

    stateStore.commit();

    fileList = Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
        .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 2);
    Assert.assertTrue(fileList.get(0).toString().endsWith(".0000") ||
        fileList.get(0).toString().endsWith(".0001"));
    Assert.assertTrue(fileList.get(1).toString().endsWith(".0000") ||
        fileList.get(1).toString().endsWith(".0001"));

    stateStore.commit();

    fileList = Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
        .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 3);
  }

  @Test
  public void testFileRetention() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    long fileRotationMillis = 0;
    long fileRetentionMillis = -1000;
    LocalFileStateStore stateStore =
        new LocalFileStateStore(tempPath.toString(), fileRotationMillis, fileRetentionMillis);

    List<Path> fileList =
        Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
            .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 1);
    Assert.assertTrue(fileList.get(0).toString().endsWith(".0000"));

    stateStore.commit();

    fileList = Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
        .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 1);
    Assert.assertTrue(fileList.get(0).toString().endsWith(".0000"));

    stateStore.commit();

    fileList = Files.list(Paths.get(tempPath.toString(), LocalFileStateStore.STATE_DIR_NAME))
        .collect(Collectors.toList());
    Assert.assertEquals(fileList.size(), 1);
    Assert.assertTrue(fileList.get(0).toString().endsWith(".0000"));
  }

  @Test
  public void testLoadData() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    LocalFileStateStore stateStore = new LocalFileStateStore(tempPath.toString());
    Iterator<BaseMessage> iterator = stateStore.loadData();
    Assert.assertFalse(iterator.hasNext());

    AppShuffleId appShuffleId1 = new AppShuffleId("app1", "1", 2);
    AppShuffleId appShuffleId2 = new AppShuffleId("app1", "1", 20);
    ShuffleWriteConfig shuffleWriteConfig1 = new ShuffleWriteConfig((short) 6);
    ShuffleWriteConfig shuffleWriteConfig2 = new ShuffleWriteConfig((short) 60);
    StagePersistentInfo stageInfo1 =
        new StagePersistentInfo(20, 30, shuffleWriteConfig1, ShuffleStageStatus.FILE_STATUS_OK);
    StagePersistentInfo stageInfo2 = new StagePersistentInfo(200, 300, shuffleWriteConfig2,
        ShuffleStageStatus.FILE_STATUS_CORRUPTED);

    stateStore.storeStageInfo(appShuffleId1, stageInfo1);
    stateStore.storeTaskAttemptCommit(appShuffleId1,
        Arrays.asList(2L, 4L),
        Arrays.asList(new PartitionFilePathAndLength(1, "p1", 10),
            new PartitionFilePathAndLength(2, "p2", 11)));
    stateStore.storeStageInfo(appShuffleId2, stageInfo2);
    stateStore.storeTaskAttemptCommit(appShuffleId1,
        Arrays.asList(40L),
        Arrays.asList(new PartitionFilePathAndLength(9, "p9", 0),
            new PartitionFilePathAndLength(10, "p10", 100)));

    stateStore.commit();

    // load data
    iterator = stateStore.loadData();

    Assert.assertTrue(iterator.hasNext());
    StageInfoStateItem stageInfoStateItem = (StageInfoStateItem) iterator.next();
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 20);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 30);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);
    Assert.assertEquals(stageInfoStateItem.getFileStatus(), ShuffleStageStatus.FILE_STATUS_OK);

    Assert.assertTrue(iterator.hasNext());
    TaskAttemptCommitStateItem taskAttemptCommitStateItem =
        (TaskAttemptCommitStateItem) iterator.next();
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(2L, 4L));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
        Arrays.asList(new PartitionFilePathAndLength(1, "p1", 10),
            new PartitionFilePathAndLength(2, "p2", 11)));

    Assert.assertTrue(iterator.hasNext());
    stageInfoStateItem = (StageInfoStateItem) iterator.next();
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId2);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 200);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 300);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig2);
    Assert.assertEquals(stageInfoStateItem.getFileStatus(),
        ShuffleStageStatus.FILE_STATUS_CORRUPTED);

    Assert.assertTrue(iterator.hasNext());
    taskAttemptCommitStateItem = (TaskAttemptCommitStateItem) iterator.next();
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(40L));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
        Arrays.asList(new PartitionFilePathAndLength(9, "p9", 0),
            new PartitionFilePathAndLength(10, "p10", 100)));

    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());

    // write another batch of data without commit
    for (int i = 0; i < 10000; i++) {
      stateStore.storeStageInfo(appShuffleId1, stageInfo1);
      stateStore.storeTaskAttemptCommit(appShuffleId1,
          Arrays.asList(2000L),
          Arrays.asList(new PartitionFilePathAndLength(1000, "p1000", 10000),
              new PartitionFilePathAndLength(2000, "p2000", 11000)));
    }

    // load data
    iterator = stateStore.loadData();

    Assert.assertTrue(iterator.hasNext());
    stageInfoStateItem = (StageInfoStateItem) iterator.next();
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 20);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 30);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);

    Assert.assertTrue(iterator.hasNext());
    taskAttemptCommitStateItem = (TaskAttemptCommitStateItem) iterator.next();
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(2L, 4L));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
        Arrays.asList(new PartitionFilePathAndLength(1, "p1", 10),
            new PartitionFilePathAndLength(2, "p2", 11)));

    Assert.assertTrue(iterator.hasNext());
    stageInfoStateItem = (StageInfoStateItem) iterator.next();
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId2);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 200);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 300);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig2);

    Assert.assertTrue(iterator.hasNext());
    taskAttemptCommitStateItem = (TaskAttemptCommitStateItem) iterator.next();
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(40L));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
        Arrays.asList(new PartitionFilePathAndLength(9, "p9", 0),
            new PartitionFilePathAndLength(10, "p10", 100)));

    // load data
    iterator = stateStore.loadData();

    Assert.assertTrue(iterator.hasNext());
    stageInfoStateItem = (StageInfoStateItem) iterator.next();
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 20);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 30);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);

    Assert.assertTrue(iterator.hasNext());
    taskAttemptCommitStateItem = (TaskAttemptCommitStateItem) iterator.next();
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(2L, 4L));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
        Arrays.asList(new PartitionFilePathAndLength(1, "p1", 10),
            new PartitionFilePathAndLength(2, "p2", 11)));

    Assert.assertTrue(iterator.hasNext());
    stageInfoStateItem = (StageInfoStateItem) iterator.next();
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId2);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 200);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 300);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig2);

    Assert.assertTrue(iterator.hasNext());
    taskAttemptCommitStateItem = (TaskAttemptCommitStateItem) iterator.next();
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(40L));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
        Arrays.asList(new PartitionFilePathAndLength(9, "p9", 0),
            new PartitionFilePathAndLength(10, "p10", 100)));

    for (int i = 0; i < 10000; i++) {
      Assert.assertTrue(iterator.hasNext());
      stageInfoStateItem = (StageInfoStateItem) iterator.next();
      Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
      Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 20);
      Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 30);
      Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);

      Assert.assertTrue(iterator.hasNext());
      taskAttemptCommitStateItem = (TaskAttemptCommitStateItem) iterator.next();
      Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appShuffleId1);
      Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(2000L));
      Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
          Arrays.asList(new PartitionFilePathAndLength(1000, "p1000", 10000),
              new PartitionFilePathAndLength(2000, "p2000", 11000)));
    }

    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
  }
}
