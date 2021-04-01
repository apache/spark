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
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ReplicatedWriteClientTest {

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][]{{true, true}, {false, false}};
  }

  @Test(dataProvider = "data-provider")
  public void closeClientMultiTimes(boolean finishUploadAck, boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort()))));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.close();
      writeClient.close();
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_oneServer(boolean finishUploadAck, boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort()))));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
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

      writeClient.finishUpload();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
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

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        new ServerDetail(testServer2.getServerId(),
            String.format("localhost:%s", testServer2.getShufflePort()))
    ));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
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

      writeClient.finishUpload();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      // read records from server 1
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      // read records from server 2
      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_oneBadServerBeforeConnect(boolean finishUploadAck,
                                                            boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        new ServerDetail(testServer2.getServerId(),
            String.format("localhost:%s", testServer2.getShufflePort()))
    ));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      testServer1.shutdown();

      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.writeDataBlock(0, null);

      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));

      writeClient.finishUpload();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      // read records from server 2
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_oneBadServerAfterConnect(boolean finishUploadAck,
                                                           boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        new ServerDetail(testServer2.getServerId(),
            String.format("localhost:%s", testServer2.getShufflePort()))
    ));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();

      testServer1.shutdown();

      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      writeClient.writeDataBlock(0, null);

      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));

      writeClient.finishUpload();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      // read records from server 2
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_oneBadServerAfterStartUpload(boolean finishUploadAck,
                                                               boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        new ServerDetail(testServer2.getServerId(),
            String.format("localhost:%s", testServer2.getShufflePort()))
    ));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();

      writeClient.startUpload(appTaskAttemptId, numMaps, 20);

      testServer1.shutdown();

      writeClient.writeDataBlock(0, null);

      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));

      writeClient.finishUpload();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      // read records from server 2
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_oneBadServerAfterSendRecord(boolean finishUploadAck,
                                                              boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        new ServerDetail(testServer2.getServerId(),
            String.format("localhost:%s", testServer2.getShufflePort()))
    ));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
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

      testServer1.shutdown();

      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));
      writeClient.writeDataBlock(1,
          ByteBuffer.wrap(new byte[0]));

      writeClient.finishUpload();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      // read records from server 2
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_oneBadServerAfterSendAllRecords(boolean finishUploadAck,
                                                                  boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        new ServerDetail(testServer2.getServerId(),
            String.format("localhost:%s", testServer2.getShufflePort()))
    ));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
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

      testServer1.shutdown();

      writeClient.finishUpload();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      // read records from server 2
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void writeAndReadRecords_oneBadServerAfterFinishUpload(boolean finishUploadAck,
                                                                boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        new ServerDetail(testServer2.getServerId(),
            String.format("localhost:%s", testServer2.getShufflePort()))
    ));

    try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
        serverReplicationGroup,
        TestConstants.NETWORK_TIMEOUT,
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

      writeClient.finishUpload();

      testServer1.shutdown();

      Assert.assertTrue(writeClient.getShuffleWriteBytes() > 0);

      // read records from server 2
      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 2);

      records = StreamServerTestUtils
          .readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);

      long shuffleWriteBytes = writeClient.getShuffleWriteBytes();
      Assert.assertTrue(shuffleWriteBytes > 0);

      writeClient.close();

      long shuffleWriteBytes2 = writeClient.getShuffleWriteBytes();
      Assert.assertEquals(shuffleWriteBytes2, shuffleWriteBytes);
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }
}
