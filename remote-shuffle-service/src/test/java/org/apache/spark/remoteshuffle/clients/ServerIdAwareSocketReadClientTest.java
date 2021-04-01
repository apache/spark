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

import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidServerIdException;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ServerIdAwareSocketReadClientTest {

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "data-provider")
  public void readRecords(boolean finishUploadAck) {
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

      try (ShuffleDataSyncWriteClient writeClient = UnpooledWriteClientFactory.getInstance()
          .getOrCreateClient(
              "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT,
              finishUploadAck, "user1", "app1", appAttempt, TestConstants.SHUFFLE_WRITE_CONFIG)) {
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
      ServerDetail serverDetail =
          new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString());
      try (ServerIdAwareSocketReadClient readClient = new ServerIdAwareSocketReadClient(
          serverDetail, TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
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
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(expectedExceptions = RssInvalidServerIdException.class)
  public void invalidServerId() {
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

      try (ShuffleDataSyncWriteClient writeClient = UnpooledWriteClientFactory.getInstance()
          .getOrCreateClient(
              "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true,
              "user1", "app1", appAttempt, TestConstants.SHUFFLE_WRITE_CONFIG)) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.writeDataBlock(1, null);
        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      ServerDetail serverDetail =
          new ServerDetail("invalidServerId", testServer1.getShuffleConnectionString());
      try (ServerIdAwareSocketReadClient readClient = new ServerIdAwareSocketReadClient(
          serverDetail, TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
      }
    } finally {
      testServer1.shutdown();
    }
  }

}
