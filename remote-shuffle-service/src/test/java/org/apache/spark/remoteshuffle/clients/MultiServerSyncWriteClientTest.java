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

import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerReplicationGroup;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.apache.spark.remoteshuffle.util.ServerHostAndPort;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MultiServerSyncWriteClientTest {

  int networkTimeoutMillis = 5000;
  int maxTryingMillis = 10000;

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][]{{false, true}, {true, false}};
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_noRecord(boolean finishUploadAck, boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<ServerDetail> serverDetails = Arrays.asList(
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString()));

    try (MultiServerWriteClient writeClient = new MultiServerSyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(serverDetails)),
        networkTimeoutMillis,
        maxTryingMillis,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.finishUpload();

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void closeClientMultiTimes(boolean finishUploadAck, boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<ServerDetail> serverDetails = Arrays.asList(
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString()));

    try (MultiServerWriteClient writeClient = new MultiServerSyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(serverDetails)),
        networkTimeoutMillis,
        maxTryingMillis,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.writeDataBlock(0, null);

      writeClient.finishUpload();

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      writeClient.close();
      writeClient.close();
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_twoServers(boolean finishUploadAck,
                                             boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<ServerDetail> serverDetails1 = Arrays.asList(
        new ServerDetail(testServer1.getServerId(), testServer1.getShuffleConnectionString()));
    List<ServerDetail> serverDetails2 = Arrays.asList(
        new ServerDetail(testServer2.getServerId(), testServer2.getShuffleConnectionString()));

    try (MultiServerWriteClient writeClient = new MultiServerSyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(serverDetails1),
            new ServerReplicationGroup(serverDetails2)),
        networkTimeoutMillis,
        maxTryingMillis,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.writeDataBlock(0, null);

      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));

      for (int i = 0; i < 1000; i++) {
        writeClient.writeDataBlock(2,
            ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
        writeClient.writeDataBlock(3,
            ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));
      }

      writeClient.finishUpload();

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      Assert.assertEquals(records.get(0).getPayload(), new byte[0]);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      Assert.assertEquals(records.get(0).getPayload(), new byte[0]);

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1000);

      Assert.assertEquals(records.get(0).getPayload(), "value2".getBytes(StandardCharsets.UTF_8));

      Assert.assertEquals(records.get(records.size() - 1).getPayload(),
          "value2".getBytes(StandardCharsets.UTF_8));

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 3,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1000);

      Assert.assertEquals(records.get(0).getPayload(), "value3".getBytes(StandardCharsets.UTF_8));

      Assert.assertEquals(records.get(records.size() - 1).getPayload(),
          "value3".getBytes(StandardCharsets.UTF_8));

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 4,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_twoServersPerPartition(boolean finishUploadAck,
                                                         boolean usePooledConnection) {
    int numTestServers = 5;
    List<TestStreamServer> testServers = new ArrayList<>();
    for (int i = 0; i < numTestServers; i++) {
      testServers.add(TestStreamServer.createRunningServer());
    }

    List<ServerDetail> serverDetails = testServers.stream()
        .map(t -> new ServerDetail(t.getServerId(), t.getShuffleConnectionString()))
        .collect(Collectors.toList());

    List<ServerDetail> group1 = new ArrayList<>();
    List<ServerDetail> group2 = new ArrayList<>();
    for (int i = 0; i < serverDetails.size(); i++) {
      if (i % 2 == 0) {
        group1.add(serverDetails.get(i));
      } else {
        group2.add(serverDetails.get(i));
      }
    }

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    int numServersPerPartition = 2;
    int numRecords = 100000;

    List<TaskDataBlock> partition2WriteRecords = new ArrayList<>();
    List<TaskDataBlock> partition3WriteRecords = new ArrayList<>();

    try (MultiServerSyncWriteClient writeClient = new MultiServerSyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(group1), new ServerReplicationGroup(group2)),
        numServersPerPartition,
        networkTimeoutMillis,
        maxTryingMillis,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.writeDataBlock(0, null);

      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));

      for (int i = 0; i < numRecords; i++) {
        TaskDataBlock partition2Record = new TaskDataBlock(
            ("value2_" + i).getBytes(StandardCharsets.UTF_8),
            appTaskAttemptId.getTaskAttemptId());
        writeClient.writeDataBlock(2,
            ByteBuffer.wrap(partition2Record.getPayload()));
        partition2WriteRecords.add(partition2Record);

        TaskDataBlock partition3Record = new TaskDataBlock(
            ("value33333333333333333333333333333333_" + i).getBytes(StandardCharsets.UTF_8),
            appTaskAttemptId.getTaskAttemptId());
        writeClient.writeDataBlock(3,
            ByteBuffer.wrap(partition3Record.getPayload()));
        partition3WriteRecords.add(partition3Record);
      }

      writeClient.finishUpload();

      List<TaskDataBlock> readRecords = new ArrayList<>();

      // Read partition 0 from first sever in replication group 1
      List<TaskDataBlock> records;
      int port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 0,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Read partition 0 from last server in replication group 2
      port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString())
          .getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 0,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Verify records for partition 0
      Assert.assertEquals(readRecords.size(), 1);
      Assert.assertEquals(readRecords.get(0).getPayload(), new byte[0]);

      readRecords = new ArrayList<>();

      // Read partition 1 from first sever in replication group 1
      port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 1,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Read partition 1 from last server in replication group 2
      port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString())
          .getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 1,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Verify records for partition 1
      Assert.assertEquals(readRecords.size(), 2);
      Assert.assertEquals(readRecords.get(0).getPayload(), new byte[0]);
      Assert.assertEquals(readRecords.get(1).getPayload(), new byte[0]);

      readRecords = new ArrayList<>();

      // Read partition 2 from first sever in replication group 1
      port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 2,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Read partition 2 from last server in replication group 2
      port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString())
          .getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 2,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Verify records for partition 2
      Assert.assertEquals(readRecords.size(), numRecords);
      Assert.assertEquals(readRecords.size(), partition2WriteRecords.size());
      Assert.assertEquals(new HashSet<>(readRecords), new HashSet<>(partition2WriteRecords));

      readRecords = new ArrayList<>();

      // Read partition 3 from first sever in replication group 1
      port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 3,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Read partition 3 from last server in replication group 2
      port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString())
          .getPort();
      records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 3,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      readRecords.addAll(records);

      // Verify records for partition 3
      Assert.assertEquals(readRecords.size(), numRecords);
      Assert.assertEquals(readRecords.size(), partition3WriteRecords.size());
      Assert.assertEquals(new HashSet<>(readRecords), new HashSet<>(partition3WriteRecords));
    } finally {
      testServers.forEach(t -> t.shutdown());
    }
  }
}
