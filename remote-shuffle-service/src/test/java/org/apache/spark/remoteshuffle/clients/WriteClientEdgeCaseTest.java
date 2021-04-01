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
import org.apache.spark.remoteshuffle.testutil.ClientTestUtils;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.apache.spark.remoteshuffle.util.RetryUtils;
import org.apache.spark.remoteshuffle.exceptions.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@Test
public class WriteClientEdgeCaseTest {
  @Test
  public void closeClientMultiTimes() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();
    try {
      AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
              appTaskAttemptId1.getAppAttempt())) {
        client.close();
        client.close();
      }

      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
              appTaskAttemptId1.getAppAttempt())) {
        client.startUpload(appTaskAttemptId1, 1, 20);

        client.close();
        client.close();
      }

      AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId("app1", "exec1", 1, 2, 2L);
      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId2.getAppId(),
              appTaskAttemptId2.getAppAttempt())) {
        client.startUpload(appTaskAttemptId2, 1, 20);
        client.writeDataBlock(1, null);

        client.close();
        client.close();
      }

      AppTaskAttemptId appTaskAttemptId3 = new AppTaskAttemptId("app1", "exec1", 1, 2, 3L);
      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId3.getAppId(),
              appTaskAttemptId3.getAppAttempt())) {
        client.startUpload(appTaskAttemptId3, 1, 20);
        client.writeDataBlock(1, null);
        client.finishUpload();

        client.close();
        client.close();
      }
    } finally {
      testServer.shutdown();
    }
  }

  @Test
  public void closeClientAfterServerShutdown() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.writeDataBlock(1, null);
        client.finishUpload();

        List<TaskDataBlock> records = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
                Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 1);

        // Shutdown server first
        testServer.shutdown();

        // Use another client to test and wait util the server has been shutdown
        boolean serverShutdown = RetryUtils.retryUntilTrue(100, 10000, () -> {
          try (SingleServerWriteClient clientToTestServerConnection = ClientTestUtils
              .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
                  appTaskAttemptId.getAppAttempt())) {
            clientToTestServerConnection.startUpload(appTaskAttemptId, 1, 20);
            return false;
          } catch (RssNetworkException ex) {
            return true;
          } catch (Throwable ex) {
            return false;
          }
        });

        Assert.assertTrue(serverShutdown);

        // Close client after server shutdown
        client.close();
        client.close();
      }
    } finally {
      testServer.shutdown();
    }
  }

  @Test(expectedExceptions = RssFinishUploadException.class)
  public void duplicateUploadWithsSameTaskAttemptId() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);
      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.writeDataBlock(1, null);
        client.finishUpload();
      }

      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.writeDataBlock(1, null);
        client.finishUpload();
      }

      List<TaskDataBlock> readRecords = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(readRecords.size(), 1);
      Assert.assertNull(readRecords.get(0).getPayload());
    } finally {
      testServer.shutdown();
    }
  }

  @Test
  // This is to test scenario:
  // 1. Task 1 is sending data.
  // 2. Spark driver thinks task 1 lost (e.g. lost its executor), and retries with task 2.
  // 3. Task 2 finishes sending data.
  // 4. Task 1 is still running and trying to finish upload.
  // 5. Read data, should get data written by task 2
  public void staleTaskAttemptThrowsExceptionOnFinishUpload() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      // task 1 sends data but does not finish upload
      AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client1 = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
              appTaskAttemptId1.getAppAttempt())) {
        client1.startUpload(appTaskAttemptId1, 1, 20);
        client1.writeDataBlock(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        // task 2 sends data and finish upload
        AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId("app1", "exec1", 1, 2, 2L);
        try (SingleServerWriteClient client2 = ClientTestUtils
            .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(),
                appTaskAttemptId1.getAppAttempt())) {
          client2.startUpload(appTaskAttemptId2, 1, 20);
          client2.writeDataBlock(1,
              ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
          client2.finishUpload();
        }

        // task 1 finishes upload
        client1.finishUpload();

        List<TaskDataBlock> readRecords = StreamServerTestUtils
            .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 1,
                Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
        Assert.assertEquals(readRecords.size(), 1);
        Assert.assertEquals(new String(readRecords.get(0).getPayload(), StandardCharsets.UTF_8),
            "value2");
      }
    } finally {
      testServer.shutdown();
    }
  }

  @Test
  public void writeNoRecord() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.finishUpload();
      }

      List<TaskDataBlock> records = StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);
    } finally {
      testServer.shutdown();
    }
  }

  @Test(expectedExceptions = {RssMissingShuffleWriteConfigException.class,
      RssShuffleStageNotStartedException.class, RssShuffleDataNotAvailableException.class})
  public void writeNoRecordWithoutFinishUpload() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
      }

      int dataAvailableWaitTime = 500;
      StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataAvailableWaitTime);
    } finally {
      testServer.shutdown();
    }
  }

}
