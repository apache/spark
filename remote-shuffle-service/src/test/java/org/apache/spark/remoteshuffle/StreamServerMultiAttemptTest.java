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

package org.apache.spark.remoteshuffle;

import org.apache.spark.remoteshuffle.clients.SingleServerWriteClient;
import org.apache.spark.remoteshuffle.clients.TaskDataBlock;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.testutil.ClientTestUtils;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamServerMultiAttemptTest {
  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "data-provider")
  public void readRecordsInLastTaskAttempt(boolean waitShuffleFileClosed) {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);
    AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId(appTaskAttemptId1.getAppMapId(), 1L);

    List<SingleServerWriteClient> writeClientsToClose = new ArrayList<>();

    try {
      // Write with taskAttemptId=0
      {
        SingleServerWriteClient writeClient = ClientTestUtils
            .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
                appTaskAttemptId1.getAppAttempt());
        writeClientsToClose.add(writeClient);

        writeClient.startUpload(appTaskAttemptId1, numMaps, 20);

        writeClient.writeDataBlock(1, null);

        writeClient.writeDataBlock(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      List<TaskDataBlock> records;

      if (waitShuffleFileClosed) {
        records = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 1,
                Arrays.asList(appTaskAttemptId1.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 1);
      }

      // Write with taskAttemptId=1
      {
        SingleServerWriteClient writeClient = ClientTestUtils
            .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
                appTaskAttemptId1.getAppAttempt());
        writeClientsToClose.add(writeClient);

        writeClient.startUpload(appTaskAttemptId2, numMaps, 20);

        writeClient.writeDataBlock(2, null);

        writeClient.writeDataBlock(9,
            ByteBuffer.wrap("value9".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      Assert.assertEquals(records.get(0).getPayload(), new byte[0]);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 3,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 9,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      TaskDataBlock record = records.get(0);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value9");
    } finally {
      writeClientsToClose.forEach(SingleServerWriteClient::close);
      testServer.shutdown();
    }
  }

  @Test
  public void singleMapperCloseAndOpenSamePartition() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);
    AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId(appTaskAttemptId1.getAppMapId(), 1L);

    List<SingleServerWriteClient> writeClientsToClose = new ArrayList<>();

    try {
      // Write with taskAttemptId=0
      {
        SingleServerWriteClient writeClient = ClientTestUtils
            .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
                appTaskAttemptId1.getAppAttempt());
        writeClientsToClose.add(writeClient);

        writeClient.startUpload(appTaskAttemptId1, 1, 20);

        writeClient.writeDataBlock(1, null);

        writeClient.writeDataBlock(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      // Write with taskAttemptId=1
      {
        SingleServerWriteClient writeClient = ClientTestUtils
            .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
                appTaskAttemptId1.getAppAttempt());
        writeClientsToClose.add(writeClient);

        writeClient.startUpload(appTaskAttemptId2, 1, 20);

        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value3_1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 3,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      TaskDataBlock record = records.get(0);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value3_1");
    } finally {
      writeClientsToClose.forEach(SingleServerWriteClient::close);
      testServer.shutdown();
    }
  }

  @Test
  public void singleMapperOverwriteSamePartitionWithoutClose() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<SingleServerWriteClient> writeClientsToClose = new ArrayList<>();

    try {
      // Write with taskAttemptId=0
      {
        SingleServerWriteClient writeClient = ClientTestUtils
            .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
                appTaskAttemptId1.getAppAttempt());
        writeClientsToClose.add(writeClient);

        writeClient.startUpload(appTaskAttemptId1, 1, 20);

        writeClient.writeDataBlock(1, null);

        writeClient.writeDataBlock(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
      }

      // Write with taskAttemptId=1
      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appTaskAttemptId1.getAppMapId(), 1L);
      {
        SingleServerWriteClient writeClient = ClientTestUtils
            .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId2.getAppId(),
                appTaskAttemptId2.getAppAttempt());
        writeClientsToClose.add(writeClient);

        writeClient.startUpload(appTaskAttemptId2, 1, 20);

        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value3_1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();

        StreamServerTestUtils.waitTillDataAvailable(testServer.getShufflePort(),
            appTaskAttemptId2.getAppShuffleId(), Arrays.asList(3),
            Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      }

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 3,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      TaskDataBlock record = records.get(0);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value3_1");
    } finally {
      writeClientsToClose.forEach(SingleServerWriteClient::close);
      testServer.shutdown();
    }
  }

  @Test
  public void singleMapperWriteDataWithOldTaskAttemptId() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<SingleServerWriteClient> writeClientsToClose = new ArrayList<>();

    try {
      // Write with taskAttemptId=0
      SingleServerWriteClient writeClient1 = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
              appTaskAttemptId1.getAppAttempt());
      writeClientsToClose.add(writeClient1);

      writeClient1.startUpload(appTaskAttemptId1, 1, 20);

      writeClient1.writeDataBlock(1, null);

      writeClient1.writeDataBlock(2,
          ByteBuffer.wrap(new byte[0]));

      writeClient1.writeDataBlock(3,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

      // Write with taskAttemptId=1
      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appTaskAttemptId1.getAppMapId(), 1L);
      SingleServerWriteClient writeClient2 = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId2.getAppId(),
              appTaskAttemptId2.getAppAttempt());
      writeClientsToClose.add(writeClient2);

      writeClient2.startUpload(appTaskAttemptId2, 1, 20);

      writeClient2.writeDataBlock(3,
          ByteBuffer.wrap("value3_1".getBytes(StandardCharsets.UTF_8)));

      writeClient2.finishUpload();

      StreamServerTestUtils
          .waitTillDataAvailable(testServer.getShufflePort(), appTaskAttemptId2.getAppShuffleId(),
              Arrays.asList(1, 2, 3), Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));

      // Write with taskAttemptId=0 again
      writeClient1.writeDataBlock(3,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
      try {
        writeClient1.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      // Read records and verify
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 3,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      TaskDataBlock record = records.get(0);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value3_1");
    } finally {
      writeClientsToClose.forEach(SingleServerWriteClient::close);
      testServer.shutdown();
    }
  }

  @Test
  public void staleTaskAttemptWritingData() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);

    try {
      // Write with taskAttemptId=1
      try (SingleServerWriteClient writeClient = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        writeClient.startUpload(appTaskAttemptId, numMaps, 20);

        writeClient.writeDataBlock(9,
            ByteBuffer.wrap("value9".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();

        StreamServerTestUtils
            .waitTillDataAvailable(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(),
                Arrays.asList(9), Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      }

      // Write stale attempt with taskAttemptId=0
      try (SingleServerWriteClient writeClient = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        writeClient
            .startUpload(new AppTaskAttemptId(appTaskAttemptId.getAppMapId(), 0L), numMaps, 20);

        writeClient.writeDataBlock(1, null);

        writeClient.writeDataBlock(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 3,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 9,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      TaskDataBlock record = records.get(0);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value9");
    } finally {
      testServer.shutdown();
    }
  }
}
