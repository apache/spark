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

package org.apache.spark.remoteshuffle.clients;

import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerReplicationGroup;
import org.apache.spark.remoteshuffle.exceptions.RssAggregateException;
import org.apache.spark.remoteshuffle.exceptions.RssMissingShuffleWriteConfigException;
import org.apache.spark.remoteshuffle.exceptions.RssShuffleDataNotAvailableException;
import org.apache.spark.remoteshuffle.exceptions.RssShuffleStageNotStartedException;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class MultiServerSocketReadClientTest {

  private boolean checkShuffleReplicaConsistency = true;

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "data-provider")
  public void oneServer(boolean finishUploadAck) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail =
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup =
        new ServerReplicationGroup(Arrays.asList(serverDetail));

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

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
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
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(
          Arrays.asList(serverReplicationGroup),
          TestConstants.NETWORK_TIMEOUT,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

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

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void fourServers_onlySecondServerHasData(boolean finishUploadAck) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();
    TestStreamServer testServer3 = TestStreamServer.createRunningServer();
    TestStreamServer testServer4 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 =
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 =
        new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 =
        new ServerDetail(testServer2.getServerId(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 =
        new ServerReplicationGroup(Arrays.asList(serverDetail2));

    ServerDetail serverDetail3 =
        new ServerDetail(testServer3.getServerId(), testServer3.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup3 =
        new ServerReplicationGroup(Arrays.asList(serverDetail3));

    ServerDetail serverDetail4 =
        new ServerDetail(testServer4.getServerId(), testServer4.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup4 =
        new ServerReplicationGroup(Arrays.asList(serverDetail4));

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

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup2,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
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

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup1,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup3,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup4,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays
          .asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3,
              serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

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

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
      testServer3.shutdown();
      testServer4.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void fourServers_twoServersHaveData(boolean finishUploadAck) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();
    TestStreamServer testServer3 = TestStreamServer.createRunningServer();
    TestStreamServer testServer4 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 =
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 =
        new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 =
        new ServerDetail(testServer2.getServerId(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 =
        new ServerReplicationGroup(Arrays.asList(serverDetail2));

    ServerDetail serverDetail3 =
        new ServerDetail(testServer3.getServerId(), testServer3.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup3 =
        new ServerReplicationGroup(Arrays.asList(serverDetail3));

    ServerDetail serverDetail4 =
        new ServerDetail(testServer4.getServerId(), testServer4.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup4 =
        new ServerReplicationGroup(Arrays.asList(serverDetail4));

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

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup2,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
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

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup3,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));

        writeClient.writeDataBlock(1,
            ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup1,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup4,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      // read partition 1
      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays
          .asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3,
              serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

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
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value2");

        record = readClient.readDataBlock();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value3");

        record = readClient.readDataBlock();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 2
      AppShufflePartitionId appShufflePartitionId2 =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays
          .asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3,
              serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          "user1",
          appShufflePartitionId2,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getPayload(), new byte[0]);

        record = readClient.readDataBlock();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 3
      AppShufflePartitionId appShufflePartitionId3 =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays
          .asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3,
              serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          "user1",
          appShufflePartitionId3,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

        record = readClient.readDataBlock();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 4 (no data)
      AppShufflePartitionId appShufflePartitionId4 =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 4);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays
          .asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3,
              serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          "user1",
          appShufflePartitionId4,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        TaskDataBlock record = readClient.readDataBlock();
        Assert.assertNull(record);

        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.close();

        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
      testServer3.shutdown();
      testServer4.shutdown();
    }
  }

  @Test(dataProvider = "data-provider",
      expectedExceptions = {RssMissingShuffleWriteConfigException.class,
          RssShuffleStageNotStartedException.class, RssShuffleDataNotAvailableException.class,
          RssAggregateException.class})
  public void twoServers_firstServerHasNoUpload(boolean finishUploadAck) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 =
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 =
        new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 =
        new ServerDetail(testServer2.getServerId(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 =
        new ServerReplicationGroup(Arrays.asList(serverDetail2));

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

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup2,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
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

      // use short wait time to make the test finish fast
      int timeoutMillis = 500;
      int dataAvailableMaxWaitTime = 500;
      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(
          Arrays.asList(serverReplicationGroup1, serverReplicationGroup2),
          timeoutMillis,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableMaxWaitTime),
          checkShuffleReplicaConsistency)) {
        readClient.connect();
        readClient.readDataBlock();
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider",
      expectedExceptions = {RssMissingShuffleWriteConfigException.class,
          RssShuffleStageNotStartedException.class, RssShuffleDataNotAvailableException.class,
          RssAggregateException.class})
  public void twoServers_secondServerHasNoUpload(boolean finishUploadAck) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 =
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 =
        new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 =
        new ServerDetail(testServer2.getServerId(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 =
        new ServerReplicationGroup(Arrays.asList(serverDetail2));

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

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup1,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
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

      // use short wait time to make the test finish fast
      int timeoutMillis = 500;
      int dataAvailableMaxWaitTime = 500;
      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(
          Arrays.asList(serverReplicationGroup1, serverReplicationGroup2),
          timeoutMillis,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableMaxWaitTime),
          checkShuffleReplicaConsistency)) {
        readClient.connect();
        TaskDataBlock record = readClient.readDataBlock();
        while (record != null) {
          record = readClient.readDataBlock();
        }
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }
}
