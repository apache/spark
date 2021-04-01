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

package org.apache.spark.remoteshuffle;

import org.apache.spark.remoteshuffle.clients.SingleServerWriteClient;
import org.apache.spark.remoteshuffle.clients.TaskDataBlock;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.exceptions.RssShuffleStageNotStartedException;
import org.apache.spark.remoteshuffle.testutil.ClientTestUtils;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class StreamServerTest {
  private static final Logger logger = LoggerFactory.getLogger(StreamServerTest.class);

  @Test
  public void startAndShutdown() {
    StreamServer server1 = TestStreamServer.createRunningServer();
    StreamServer server2 = TestStreamServer.createRunningServer();

    server1.shutdown();
    server2.shutdown();
  }

  @Test
  public void readNonExistingShuffleData() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();
    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (SingleServerWriteClient writeclient = ClientTestUtils.
        getOrCreateWriteClient(testServer.getShufflePort(),
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt())) {
      writeclient.connect();
      writeclient.startUpload(appTaskAttemptId, numMaps, 20);
      writeclient.writeDataBlock(1, null);
      writeclient.finishUpload();

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      int dataAvailableWaitTime = 500;

      AppShuffleId invalidAppShuffleId =
          new AppShuffleId("not_existing_app", appTaskAttemptId.getAppAttempt(),
              appTaskAttemptId.getShuffleId());
      try {
        StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), invalidAppShuffleId, 1,
            Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataAvailableWaitTime);
        Assert.fail("The previous code shall throw exception and shall not run into here");
      } catch (Throwable ex) {
        Assert.assertEquals(ex.getClass(), RssShuffleStageNotStartedException.class);
      }

      invalidAppShuffleId = new AppShuffleId(appTaskAttemptId.getAppId(), "not_existing_exec",
          appTaskAttemptId.getShuffleId());
      try {
        StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), invalidAppShuffleId, 1,
            Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataAvailableWaitTime);
        Assert.fail("The previous code shall throw exception and shall not run into here");
      } catch (Throwable ex) {
        Assert.assertEquals(ex.getClass(), RssShuffleStageNotStartedException.class);
      }

      invalidAppShuffleId =
          new AppShuffleId(appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt(), 912345);
      try {
        StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), invalidAppShuffleId, 1,
            Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataAvailableWaitTime);
        Assert.fail("The previous code shall throw exception and shall not run into here");
      } catch (Throwable ex) {
        Assert.assertEquals(ex.getClass(), RssShuffleStageNotStartedException.class);
      }
    } finally {
      testServer.shutdown();
    }
  }

  @Test
  public void useLocalStandaloneRegistryServer() {
    TestStreamServer testServer =
        TestStreamServer.createRunningServerWithLocalStandaloneRegistryServer();

    int numMaps = 1;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (SingleServerWriteClient writeclient = ClientTestUtils
        .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt())) {
      writeclient.connect();
      writeclient.startUpload(appTaskAttemptId, numMaps, 20);

      writeclient.writeDataBlock(1, null);

      writeclient.writeDataBlock(2,
          ByteBuffer.wrap(new byte[0]));

      writeclient.writeDataBlock(3,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

      writeclient.finishUpload();

      // Verify read client able to read data from stream server.
      // We pass readQueueSize parameter to helper method readAllRecords, so it will use async read client.

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      TaskDataBlock record = records.get(0);
      Assert.assertEquals(record.getPayload(), new byte[0]);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      record = records.get(0);
      Assert.assertEquals(record.getPayload(), new byte[0]);

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 3,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 1);

      record = records.get(0);
      Assert.assertEquals(new String(record.getPayload(), StandardCharsets.UTF_8), "value1");

      records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 4,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);
    } finally {
      testServer.shutdown();
    }
  }

}
