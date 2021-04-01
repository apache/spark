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
import org.apache.spark.remoteshuffle.exceptions.RssInvalidServerIdException;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class ServerIdAwareSyncWriteClientTest {

  int networkTimeoutMillis = 5000;

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][]{{true, true}, {false, false}};
  }

  @Test(dataProvider = "data-provider")
  public void closeClientMultiTimes(boolean finishUploadAck, boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        networkTimeoutMillis,
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
  public void closeClientAfterServerShutdown(boolean finishUploadAck,
                                             boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
        new ServerDetail(testServer1.getServerId(),
            String.format("localhost:%s", testServer1.getShufflePort())),
        networkTimeoutMillis,
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

      StreamServerTestUtils
          .readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));

      testServer1.shutdown();

      writeClient.close();
    } finally {
    }
  }

  @Test(dataProvider = "data-provider", expectedExceptions = {RssInvalidServerIdException.class})
  public void writeAndReadRecords_invalidServerId(boolean finishUploadAck,
                                                  boolean usePooledConnection) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
        new ServerDetail("invalid_server_id",
            String.format("localhost:%s", testServer1.getShufflePort())),
        networkTimeoutMillis,
        finishUploadAck,
        usePooledConnection,
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, 20);
    } finally {
      testServer1.shutdown();
    }
  }
}
