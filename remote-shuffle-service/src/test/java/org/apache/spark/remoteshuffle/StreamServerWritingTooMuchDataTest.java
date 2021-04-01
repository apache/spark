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
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.exceptions.RssFileCorruptedException;
import org.apache.spark.remoteshuffle.exceptions.RssFinishUploadException;
import org.apache.spark.remoteshuffle.metadata.InMemoryServiceRegistry;
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry;
import org.apache.spark.remoteshuffle.testutil.ClientTestUtils;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.annotations.Test;

import java.util.Arrays;

public class StreamServerWritingTooMuchDataTest {

  @Test(expectedExceptions = {RssFileCorruptedException.class, RssFinishUploadException.class})
  public void writeDataExceedingAppMaxWriteBytes() {
    long appRetentionMillis = 1000;

    StreamServerConfig config = new StreamServerConfig();
    config.setShufflePort(0);
    config.setHttpPort(0);
    config.setAppMemoryRetentionMillis(appRetentionMillis);
    config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
    config.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);
    config.setAppMaxWriteBytes(4);

    ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    TestStreamServer testServer = TestStreamServer.createRunningServer(config, serviceRegistry);

    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (SingleServerWriteClient writeClient = ClientTestUtils
        .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt())) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, 1, 20);

      writeClient.writeDataBlock(1, null);

      writeClient.finishUpload();

      StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
    } finally {
      testServer.shutdown();
    }
  }

  @Test(expectedExceptions = {RssFileCorruptedException.class, RssFinishUploadException.class})
  public void writeDataExceedingAppMaxWriteBytesAndConnectAgain() {
    long appRetentionMillis = 1000;

    StreamServerConfig config = new StreamServerConfig();
    config.setShufflePort(0);
    config.setHttpPort(0);
    config.setAppMemoryRetentionMillis(appRetentionMillis);
    config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
    config.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);
    config.setAppMaxWriteBytes(4);

    ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    TestStreamServer testServer = TestStreamServer.createRunningServer(config, serviceRegistry);

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

      try (SingleServerWriteClient writeClient = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, 1, 20);

        writeClient.writeDataBlock(1, null);

        writeClient.finishUpload();
      }

      try (SingleServerWriteClient writeClient = ClientTestUtils
          .getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(),
              appTaskAttemptId.getAppAttempt())) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, 1, 20);

        writeClient.writeDataBlock(1, null);

        writeClient.finishUpload();
      }

      StreamServerTestUtils
          .readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1,
              Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
    } finally {
      testServer.shutdown();
    }
  }
}
