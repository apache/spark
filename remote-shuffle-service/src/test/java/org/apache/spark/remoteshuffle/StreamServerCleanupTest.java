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

import org.apache.spark.remoteshuffle.clients.DataBlockSyncWriteClient;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.ShuffleMapTaskAttemptId;
import org.apache.spark.remoteshuffle.execution.LocalFileStateStore;
import org.apache.spark.remoteshuffle.metadata.InMemoryServiceRegistry;
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry;
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage;
import org.apache.spark.remoteshuffle.storage.ShuffleFileUtils;
import org.apache.spark.remoteshuffle.testutil.StreamServerTestUtils;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.apache.spark.remoteshuffle.util.RetryUtils;
import io.netty.buffer.Unpooled;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StreamServerCleanupTest {
  @Test
  public void cleanupExpiredApplicationFiles() throws Exception {
    long appRetentionMillis = 1000;

    StreamServerConfig config = new StreamServerConfig();
    config.setShufflePort(0);
    config.setHttpPort(0);
    config.setAppMemoryRetentionMillis(appRetentionMillis);
    config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
    config.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);

    ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    TestStreamServer testServer = TestStreamServer.createRunningServer(config, serviceRegistry);

    String rootDir = testServer.getRootDir();

    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
        testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
        appTaskAttemptId1.getAppId(), appTaskAttemptId1.getAppAttempt())) {
      writeClient.connect();
      ShuffleMapTaskAttemptId shuffleMapTaskAttemptId =
          new ShuffleMapTaskAttemptId(appTaskAttemptId1.getShuffleId(),
              appTaskAttemptId1.getMapId(), appTaskAttemptId1.getTaskAttemptId());
      writeClient.startUpload(shuffleMapTaskAttemptId, 1, 20, TestConstants.SHUFFLE_WRITE_CONFIG);

      writeClient
          .writeData(1, appTaskAttemptId1.getTaskAttemptId(), Unpooled.wrappedBuffer(new byte[0]));

      writeClient
          .writeData(2, appTaskAttemptId1.getTaskAttemptId(), Unpooled.wrappedBuffer(new byte[0]));

      writeClient.writeData(3, appTaskAttemptId1.getTaskAttemptId(),
          Unpooled.wrappedBuffer("value1".getBytes(StandardCharsets.UTF_8)));

      writeClient.finishUpload(appTaskAttemptId1.getTaskAttemptId());

      StreamServerTestUtils
          .waitTillDataAvailable(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(),
              Arrays.asList(1, 2, 3), Arrays.asList(appTaskAttemptId1.getTaskAttemptId()));

      // The shuffle files should be still there. Those files will only be deleted when they expire and
      // there is new application shuffle upload.
      ShuffleFileStorage shuffleFileStorage = new ShuffleFileStorage();
      List<String> files = shuffleFileStorage.listAllFiles(rootDir).stream()
          .filter(t -> !Paths.get(t).getFileName().toString()
              .startsWith(LocalFileStateStore.STATE_FILE_PREFIX))
          .collect(Collectors.toList());
      Assert.assertEquals(files.size(), 3 * TestConstants.SHUFFLE_WRITE_CONFIG.getNumSplits());
      Assert.assertTrue(shuffleFileStorage
          .exists(ShuffleFileUtils.getAppShuffleDir(rootDir, appTaskAttemptId1.getAppId())));

      // Start a new application shuffle upload to trigger deleting the old files

      Thread.sleep(appRetentionMillis);

      AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId("app2", "exec1", 1, 2, 0L);

      try (DataBlockSyncWriteClient writeClient2 = new DataBlockSyncWriteClient("localhost",
          testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appTaskAttemptId2.getAppId(), appTaskAttemptId2.getAppAttempt())) {
        writeClient2.connect();
        ShuffleMapTaskAttemptId shuffleMapTaskAttemptId2 =
            new ShuffleMapTaskAttemptId(appTaskAttemptId2.getShuffleId(),
                appTaskAttemptId2.getMapId(), appTaskAttemptId2.getTaskAttemptId());
        writeClient2
            .startUpload(shuffleMapTaskAttemptId2, 1, 20, TestConstants.SHUFFLE_WRITE_CONFIG);
      }
      // Check there should no files
      boolean hasNoAppDir = RetryUtils.retryUntilTrue(
          100,
          10000,
          () -> shuffleFileStorage
              .exists(ShuffleFileUtils.getAppShuffleDir(rootDir, appTaskAttemptId1.getAppId())));
      Assert.assertTrue(hasNoAppDir);
    } finally {
      testServer.shutdown();
    }
  }

}
