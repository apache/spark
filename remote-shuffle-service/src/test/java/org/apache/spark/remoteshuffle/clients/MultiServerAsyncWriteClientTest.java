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

import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerReplicationGroup;
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
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

public class MultiServerAsyncWriteClientTest {

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod2() {
    return new Object[][]{{1, false, true, 1}, {2, true, false, 10}};
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_noRecord(int numTestServers, boolean finishUploadAck,
                                           boolean usePooledConnection, int writeQueueSize) {
    List<TestStreamServer> testServers = new ArrayList<>();
    for (int i = 0; i < numTestServers; i++) {
      testServers.add(TestStreamServer.createRunningServer());
    }

    int numWriteThreads = 2;

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<ServerDetail> serverDetails = testServers.stream()
        .map(t -> new ServerDetail(t.getServerId(), t.getShuffleConnectionString()))
        .collect(Collectors.toList());

    try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(serverDetails)),
        TestConstants.NETWORK_TIMEOUT,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        writeQueueSize,
        numWriteThreads,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.finishUpload();

      for (TestStreamServer testServer : testServers) {
        List<TaskDataBlock> records = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 0);

        records = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 0);
      }
    } finally {
      testServers.forEach(t -> t.shutdown());
    }
  }

  @Test(dataProvider = "data-provider", expectedExceptions = {RssNetworkException.class})
  public void connectInvalidServer(int numTestServers, boolean finishUploadAck,
                                   boolean usePooledConnection, int writeQueueSize) {
    List<TestStreamServer> testServers = new ArrayList<>();
    for (int i = 0; i < numTestServers; i++) {
      testServers.add(TestStreamServer.createRunningServer());
    }

    int numWriteThreads = 2;

    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<ServerDetail> serverDetails =
        Arrays.asList(new ServerDetail("server1", "invalid_server:80"));

    int networkTimeout = 1000;
    try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(serverDetails)),
        networkTimeout,
        networkTimeout,
        finishUploadAck,
        usePooledConnection,
        writeQueueSize,
        numWriteThreads,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
    } finally {
      testServers.forEach(t -> t.shutdown());
    }
  }

  @Test(dataProvider = "data-provider")
  public void closeClientMultiTimes(int numTestServers, boolean finishUploadAck,
                                    boolean usePooledConnection, int writeQueueSize) {
    List<TestStreamServer> testServers = new ArrayList<>();
    for (int i = 0; i < numTestServers; i++) {
      testServers.add(TestStreamServer.createRunningServer());
    }

    int numWriteThreads = 2;

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    List<ServerDetail> serverDetails = testServers.stream()
        .map(t -> new ServerDetail(t.getServerId(), t.getShuffleConnectionString()))
        .collect(Collectors.toList());

    try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(serverDetails)),
        TestConstants.NETWORK_TIMEOUT,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        writeQueueSize,
        numWriteThreads,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.writeDataBlock(0, null);

      writeClient.finishUpload();

      writeClient.close();
      writeClient.close();
    } finally {
      testServers.forEach(t -> t.shutdown());
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords(int numTestServers, boolean finishUploadAck,
                                  boolean usePooledConnection, int writeQueueSize) {
    List<TestStreamServer> testServers = new ArrayList<>();
    for (int i = 0; i < numTestServers; i++) {
      testServers.add(TestStreamServer.createRunningServer());
    }

    List<ServerDetail> serverDetails = testServers.stream()
        .map(t -> new ServerDetail(t.getServerId(), t.getShuffleConnectionString()))
        .collect(Collectors.toList());

    int numWriteThreads = 2;

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(serverDetails)),
        TestConstants.NETWORK_TIMEOUT,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        writeQueueSize,
        numWriteThreads,
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

      writeClient.writeDataBlock(2,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
      writeClient.writeDataBlock(2,
          ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
      writeClient.writeDataBlock(2,
          ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));

      writeClient.finishUpload();

      for (TestStreamServer testServer : testServers) {
        List<TaskDataBlock> records = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 1);

        Assert.assertEquals(records.get(0).getPayload(), new byte[0]);

        records = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 2);

        Assert.assertEquals(records.get(0).getPayload(), new byte[0]);

        Assert.assertEquals(records.get(1).getPayload(), new byte[0]);

        records = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 3);

        Assert
            .assertEquals(records.get(0).getPayload(), "value1".getBytes(StandardCharsets.UTF_8));
        Assert
            .assertEquals(records.get(1).getPayload(), "value2".getBytes(StandardCharsets.UTF_8));
        Assert
            .assertEquals(records.get(2).getPayload(), "value3".getBytes(StandardCharsets.UTF_8));
      }
    } finally {
      testServers.forEach(t -> t.shutdown());
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_twoServerGroups(int numTestServers, boolean finishUploadAck,
                                                  boolean usePooledConnection,
                                                  int writeQueueSize) {
    if (numTestServers <= 1) {
      return;
    }

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

    int numWriteThreads = 2;

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    int numRecords = 100000;

    try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(group1), new ServerReplicationGroup(group2)),
        TestConstants.NETWORK_TIMEOUT,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        writeQueueSize,
        numWriteThreads,
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
        writeClient.writeDataBlock(2,
            ByteBuffer.wrap(("value2_" + i).getBytes(StandardCharsets.UTF_8)));
        writeClient.writeDataBlock(3,
            ByteBuffer.wrap(("value3_" + i).getBytes(StandardCharsets.UTF_8)));
      }

      writeClient.finishUpload();

      // Read from server group 1
      for (ServerDetail serverDetail : group1) {
        List<TaskDataBlock> records;
        int port = ServerHostAndPort.fromString(serverDetail.getConnectionString()).getPort();

        records = StreamServerTestUtils
            .readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 0,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 1);

        Assert.assertEquals(records.get(0).getPayload(), new byte[0]);

        records = StreamServerTestUtils
            .readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 2,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), numRecords);
        for (int i = 0; i < numRecords; i++) {

          Assert.assertEquals(new String(records.get(i).getPayload(), StandardCharsets.UTF_8),
              "value2_" + i);
        }
      }

      // Read from server group 2
      for (ServerDetail serverDetail : group2) {
        List<TaskDataBlock> records;
        int port = ServerHostAndPort.fromString(serverDetail.getConnectionString()).getPort();

        records = StreamServerTestUtils
            .readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 1,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 2);

        Assert.assertEquals(records.get(0).getPayload(), new byte[0]);

        Assert.assertEquals(records.get(1).getPayload(), new byte[0]);

        records = StreamServerTestUtils
            .readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 3,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), numRecords);
        for (int i = 0; i < numRecords; i++) {

          Assert.assertEquals(new String(records.get(i).getPayload(), StandardCharsets.UTF_8),
              "value3_" + i);
        }
      }
    } finally {
      testServers.forEach(t -> t.shutdown());
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_twoServersPerPartition(int numTestServers,
                                                         boolean finishUploadAck,
                                                         boolean usePooledConnection,
                                                         int writeQueueSize) {
    if (numTestServers <= 1) {
      return;
    }

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

    int numWriteThreads = 2;

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    int numServersPerPartition = 2;
    int numRecords = 100000;

    List<TaskDataBlock> partition2WriteRecords = new ArrayList<>();
    List<TaskDataBlock> partition3WriteRecords = new ArrayList<>();

    try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
        Arrays.asList(new ServerReplicationGroup(group1), new ServerReplicationGroup(group2)),
        numServersPerPartition,
        TestConstants.NETWORK_TIMEOUT,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        writeQueueSize,
        numWriteThreads,
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
