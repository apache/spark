/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.PartitionFilePathAndLength;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import org.apache.spark.remoteshuffle.messages.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class LocalFileLocalFileStateStoreIteratorTest {

  @Test
  public void nonExistingFiles() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    LocalFileStateStoreIterator iterator = new LocalFileStateStoreIterator(
        Arrays.asList(Paths.get(tempPath.toString(), "nonExistFile").toString()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(Paths.get(tempPath.toString(), "nonExistFile1").toString(),
            Paths.get(tempPath.toString(), "nonExistFile2").toString()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void emptyFiles() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    File file1 = Paths.get(tempPath.toString(), "file1").toFile();
    file1.deleteOnExit();
    try (FileOutputStream fileOutputStream1 = new FileOutputStream(file1)) {
    }

    File file2 = Paths.get(tempPath.toString(), "file2").toFile();
    file2.deleteOnExit();
    try (FileOutputStream fileOutputStream2 = new FileOutputStream(file2)) {
    }

    LocalFileStateStoreIterator iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void corruptedFile() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    File file1 = Paths.get(tempPath.toString(), "file1").toFile();
    file1.deleteOnExit();
    try (FileOutputStream fileOutputStream1 = new FileOutputStream(file1)) {
      fileOutputStream1.write(1);
    }

    File file2 = Paths.get(tempPath.toString(), "file2").toFile();
    file2.deleteOnExit();
    try (FileOutputStream fileOutputStream2 = new FileOutputStream(file2)) {
      fileOutputStream2
          .write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(-1));
      fileOutputStream2.write(1);
    }

    File file3 = Paths.get(tempPath.toString(), "file3").toFile();
    file3.deleteOnExit();
    try (FileOutputStream fileOutputStream3 = new FileOutputStream(file3)) {
      fileOutputStream3
          .write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream3.write(ByteBufUtils.convertIntToBytes(2));
      fileOutputStream3.write(1);
    }

    File file4 = Paths.get(tempPath.toString(), "file4").toFile();
    file4.deleteOnExit();
    try (FileOutputStream fileOutputStream4 = new FileOutputStream(file4)) {
      fileOutputStream4
          .write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream4.write(ByteBufUtils.convertIntToBytes(2));
      fileOutputStream4.write(1);
      fileOutputStream4.write(1);
    }

    LocalFileStateStoreIterator iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath(), file3.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath(), file3.getAbsolutePath(),
            file4.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void writeAndReadData() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    AppShuffleId appShuffleId1 = new AppShuffleId("app1", "1", 2);
    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId(appShuffleId1, 1, 99L);
    ShuffleWriteConfig shuffleWriteConfig1 = new ShuffleWriteConfig((short) 6);
    PartitionFilePathAndLength partitionFilePathAndLength1 =
        new PartitionFilePathAndLength(1, "file1", 123);

    LocalFileStateStore store = new LocalFileStateStore(tempPath.toString());
    store.storeStageInfo(appShuffleId1,
        new StagePersistentInfo(4, 5, shuffleWriteConfig1, ShuffleStageStatus.FILE_STATUS_OK));
    store.storeTaskAttemptCommit(appShuffleId1,
        Arrays.asList(appTaskAttemptId1.getTaskAttemptId()),
        Arrays.asList(partitionFilePathAndLength1));
    store.storeAppDeletion("deletedApp");
    store.storeAppDeletion(appShuffleId1.getAppId());
    store.storeStageCorruption(appShuffleId1);
    store.commit();
    store.close();

    store = new LocalFileStateStore(tempPath.toString());
    LocalFileStateStoreIterator iterator = store.loadData();

    Assert.assertTrue(iterator.hasNext());
    BaseMessage dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof StageInfoStateItem);
    StageInfoStateItem stageInfoStateItem = (StageInfoStateItem) dataItem;
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 4);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 5);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);
    Assert.assertEquals(stageInfoStateItem.getFileStatus(), ShuffleStageStatus.FILE_STATUS_OK);

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof TaskAttemptCommitStateItem);
    TaskAttemptCommitStateItem taskAttemptCommitStateItem = (TaskAttemptCommitStateItem) dataItem;
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(),
        appTaskAttemptId1.getAppShuffleId());
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(),
        Arrays.asList(appTaskAttemptId1.getTaskAttemptId()));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(),
        Arrays.asList(partitionFilePathAndLength1));

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof AppDeletionStateItem);
    AppDeletionStateItem appDeletionStateItem = (AppDeletionStateItem) dataItem;
    Assert.assertEquals(appDeletionStateItem.getAppId(), "deletedApp");

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof AppDeletionStateItem);
    appDeletionStateItem = (AppDeletionStateItem) dataItem;
    Assert.assertEquals(appDeletionStateItem.getAppId(), appShuffleId1.getAppId());

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof StageCorruptionStateItem);
    StageCorruptionStateItem stageCorruptionStateItem = (StageCorruptionStateItem) dataItem;
    Assert.assertEquals(stageCorruptionStateItem.getAppShuffleId(), appShuffleId1);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());

    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());

    iterator.close();
  }
}
