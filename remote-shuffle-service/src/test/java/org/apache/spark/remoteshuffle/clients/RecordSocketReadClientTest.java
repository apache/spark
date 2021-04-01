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

package org.apache.spark.remoteshuffle.clients;

import org.apache.spark.remoteshuffle.StreamServerConfig;
import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.testutil.ClientTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RecordSocketReadClientTest {

  @Test
  public void writeAndReadRecords() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (
          ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
              testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
              appAttempt, new ShuffleWriteConfig())) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.writeDataBlock(1, null);
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap(new byte[0]));
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.writeDataBlock(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getPayload(), new byte[0]);

        record = readClient.readDataBlock();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getPayload(), new byte[0]);

        record = readClient.readDataBlock();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "");

        record = readClient.readDataBlock();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

        record = readClient.readDataBlock();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

        record = readClient.readDataBlock();
        Assert.assertNull(record);
      }

      appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
      try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getPayload(), new byte[0]);

        record = readClient.readDataBlock();
        Assert.assertNull(record);
      }

      appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
      try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

        record = readClient.readDataBlock();
        Assert.assertNull(record);
      }

      // read non-existing partition
      appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 999);
      try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNull(record);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void writeAndReadManyRecords() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    Map<Integer, List<Pair<String, String>>> mapTaskData1 = new HashMap<>();
    Map<Integer, List<Pair<String, String>>> mapTaskData2 = new HashMap<>();

    mapTaskData1.put(1, ClientTestUtils.writeRecords1);
    mapTaskData1.put(2, ClientTestUtils.writeRecords2);
    mapTaskData1.put(3, ClientTestUtils.writeRecords3);

    mapTaskData2.put(1, ClientTestUtils.writeRecords3);
    mapTaskData2.put(2, ClientTestUtils.writeRecords2);
    mapTaskData2.put(3, ClientTestUtils.writeRecords1);

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 2;
      int numPartitions = 10;

      // map task 1 writes data
      int mapId1 = 2;
      long taskAttemptId1 = 30;
      AppTaskAttemptId appTaskAttemptId1 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId1, taskAttemptId1);
      try (
          ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
              testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
              appAttempt, new ShuffleWriteConfig())) {
        ClientTestUtils
            .connectAndWriteData(mapTaskData1, numMaps, numPartitions, appTaskAttemptId1,
                writeClient);
      }

      testServer1.getShuffleExecutor().pollAndWaitMapAttemptFinishedUpload(appTaskAttemptId1,
          TestConstants.DATA_AVAILABLE_TIMEOUT);

      // map task 2 writes data
      int mapId2 = 3;
      long taskAttemptId2 = 40;
      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId2, taskAttemptId2);
      try (
          ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
              testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
              appAttempt, new ShuffleWriteConfig())) {
        ClientTestUtils
            .connectAndWriteData(mapTaskData2, numMaps, numPartitions, appTaskAttemptId2,
                writeClient);
      }

      // read data for each partition
      for (Integer partition : mapTaskData1.keySet()) {
        AppShufflePartitionId appShufflePartitionId =
            new AppShufflePartitionId(appId, appAttempt, shuffleId, partition);
        try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient(
            "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
            appShufflePartitionId, Arrays
            .asList(appTaskAttemptId1.getTaskAttemptId(), appTaskAttemptId2.getTaskAttemptId()),
            TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          List<TaskDataBlock> readRecords =
              ClientTestUtils.readData(appShufflePartitionId, readClient);
          Assert.assertTrue(readRecords.size() > 0);
          Assert.assertEquals(readRecords.size(),
              mapTaskData1.get(partition).size() + mapTaskData2.get(partition).size());
          for (int i = 0; i < mapTaskData1.get(partition).size(); i++) {
            Assert
                .assertEquals(new String(readRecords.get(i).getPayload(), StandardCharsets.UTF_8),
                    mapTaskData1.get(partition).get(i).getValue());
          }
          for (int i = 0; i < mapTaskData2.get(partition).size(); i++) {
            Assert.assertEquals(
                new String(readRecords.get(mapTaskData1.get(partition).size() + i).getPayload(),
                    StandardCharsets.UTF_8), mapTaskData2.get(partition).get(i).getValue());
          }
        }
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void serverRestartAfterWriteFinishUpload() throws IOException {
    String rootDir = Files.createTempDirectory("StreamServer_").toString();

    TestStreamServer testServer1 =
        TestStreamServer.createRunningServer(config -> config.setRootDirectory(rootDir));

    String appId = "app1";
    String appAttempt = "attempt1";
    int shuffleId = 1;
    int numMaps = 1;
    int numPartitions = 10;
    int mapId = 2;
    long taskAttemptId = 3;
    AppTaskAttemptId appTaskAttemptId =
        new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

    try (ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
        appAttempt, new ShuffleWriteConfig())) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

      writeClient.writeDataBlock(1, null);
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

      writeClient.writeDataBlock(2,
          ByteBuffer.wrap(new byte[0]));

      writeClient.writeDataBlock(3,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

      writeClient.finishUpload();
    }

    // read data to make sure data is flushed in server side
    AppShufflePartitionId appShufflePartitionId =
        new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNotNull(record);
    }

    // shutdown server and restart
    testServer1.shutdown();
    TestStreamServer testServer2 =
        TestStreamServer.createRunningServer(config -> config.setRootDirectory(rootDir));

    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNotNull(record);

      Assert.assertEquals(record.getPayload(), new byte[0]);

      record = readClient.readDataBlock();
      Assert.assertNotNull(record);

      Assert.assertEquals(record.getPayload(), new byte[0]);

      record = readClient.readDataBlock();
      Assert.assertNotNull(record);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "");

      record = readClient.readDataBlock();
      Assert.assertNotNull(record);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

      record = readClient.readDataBlock();
      Assert.assertNotNull(record);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

      record = readClient.readDataBlock();
      Assert.assertNull(record);
    }

    appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNotNull(record);

      Assert.assertEquals(record.getPayload(), new byte[0]);

      record = readClient.readDataBlock();
      Assert.assertNull(record);
    }

    appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNotNull(record);

      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

      record = readClient.readDataBlock();
      Assert.assertNull(record);
    }

    // read non-existing partition
    appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 999);
    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNull(record);
    }

    testServer2.shutdown();
  }

  @Test
  public void serverRestartAndWriteClientWriteWithNewTaskAttemptId() throws IOException {
    String rootDir = Files.createTempDirectory("StreamServer_").toString();

    short numSplits = 3;
    ShuffleWriteConfig shuffleWriteConfig = new ShuffleWriteConfig(numSplits);

    Consumer<StreamServerConfig> configModifier = config -> {
      config.setRootDirectory(rootDir);
    };

    TestStreamServer testServer1 = TestStreamServer.createRunningServer(configModifier);

    String appId = "app1";
    String appAttempt = "attempt1";
    int shuffleId = 1;
    int numMaps = 2;
    int numPartitions = 10;
    AppTaskAttemptId map1TaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, 1, 0);
    AppTaskAttemptId map2TaskAttemptId1 = new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 3);
    AppTaskAttemptId map2TaskAttemptId2 =
        new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 30);
    AppTaskAttemptId map2TaskAttemptId3 =
        new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 31);
    AppTaskAttemptId map2TaskAttemptId4 =
        new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 40);

    int numRecords = 1000;

    try (ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
        appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map1TaskAttemptId, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("map1Key_map1Value".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    try (ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
        appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId1, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    AppShufflePartitionId appShufflePartitionId =
        new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId,
        Arrays.asList(map1TaskAttemptId.getTaskAttemptId(), map2TaskAttemptId1.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNotNull(record);
    }

    // now task attempt 1 finishes, shutdown server and restart
    testServer1.shutdown();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer(configModifier);

    // write client writes with new task attempt id
    try (ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
        appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId2, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    // read data
    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId,
        Arrays.asList(map1TaskAttemptId.getTaskAttemptId(), map2TaskAttemptId2.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      List<Pair<String, String>> allRecords = new ArrayList<>();
      for (int i = 0; i < numRecords * 2; i++) {
        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNotNull(record);
        allRecords.add(Pair.of(null, new String(record.getPayload(), StandardCharsets.UTF_8)));
      }
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNull(record);

      List<Pair<String, String>> map1Records =
          allRecords.stream().filter(t -> t.getValue().startsWith("map1Key"))
              .collect(Collectors.toList());
      Assert.assertEquals(map1Records.size(), numRecords);
      map1Records.forEach(t -> {
        Assert.assertEquals(t.getKey(), null);
        Assert.assertEquals(t.getValue(), "map1Key_map1Value");
      });

      List<Pair<String, String>> otherRecords =
          allRecords.stream().filter(t -> !t.getValue().startsWith("map1Key"))
              .collect(Collectors.toList());
      Assert.assertEquals(otherRecords.size(), numRecords);
      otherRecords.forEach(t -> {
        Assert.assertEquals(t.getKey(), null);
        Assert.assertEquals(t.getValue(), "value2");
      });
    }

    // write client writes with new task attempt id, but does not finish upload
    try (ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
        appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId3, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));
      }
    }

    // shutdown server and restart
    testServer2.shutdown();
    TestStreamServer testServer3 = TestStreamServer.createRunningServer(configModifier);

    // write client writes with new task attempt id, and finishes upload
    try (ShuffleDataSyncWriteClient writeClient = new PlainShuffleDataSyncWriteClient("localhost",
        testServer3.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1",
        appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId4, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value4".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    // read data
    try (ShuffleDataSocketReadClient readClient = new PlainShuffleDataSocketReadClient("localhost",
        testServer3.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appShufflePartitionId,
        Arrays.asList(map1TaskAttemptId.getTaskAttemptId(), map2TaskAttemptId4.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      List<Pair<String, String>> allRecords = new ArrayList<>();
      for (int i = 0; i < numRecords * 2; i++) {
        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNotNull(record);
        allRecords.add(Pair.of(null, new String(record.getPayload(), StandardCharsets.UTF_8)));
      }
      TaskDataBlock record = readClient.readDataBlock();
      Assert.assertNull(record);

      List<Pair<String, String>> map1Records =
          allRecords.stream().filter(t -> t.getValue().startsWith("map1Key"))
              .collect(Collectors.toList());
      Assert.assertEquals(map1Records.size(), numRecords);
      map1Records.forEach(t -> {
        Assert.assertEquals(t.getKey(), null);
        Assert.assertEquals(t.getValue(), "map1Key_map1Value");
      });

      List<Pair<String, String>> otherRecords =
          allRecords.stream().filter(t -> !t.getValue().startsWith("map1Key"))
              .collect(Collectors.toList());
      Assert.assertEquals(otherRecords.size(), numRecords);
      otherRecords.forEach(t -> {
        Assert.assertEquals(t.getKey(), null);
        Assert.assertEquals(t.getValue(), "value4");
      });
    }

    testServer3.shutdown();
  }
}
