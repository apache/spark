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
import org.apache.spark.remoteshuffle.exceptions.RssFinishUploadException;
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServerIdleTimeoutTest {
  @Test
  public void writeAfterServerIdleTimeout() {
    long serverIdleTimeoutMillis = 10;
    TestStreamServer testServer1 = TestStreamServer
        .createRunningServer(config -> config.setIdleTimeoutMillis(serverIdleTimeoutMillis));
    PooledWriteClientFactory writeClientFactory =
        new PooledWriteClientFactory(TestConstants.CONNECTION_IDLE_TIMEOUT_MILLIS);

    try {
      String appId = "app01";
      int shuffleId = 1;

      String appAttempt = "attempt1";
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 11;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      boolean finishUploadAck = true;

      try (ShuffleDataSyncWriteClient writeClient = writeClientFactory
          .getOrCreateClient("localhost", testServer1.getShufflePort(),
              TestConstants.NETWORK_TIMEOUT, finishUploadAck, "user1", appId, appAttempt,
              TestConstants.SHUFFLE_WRITE_CONFIG)) {
        // sleep sometime so the server thinks the connection is idle and timeout
        try {
          Thread.sleep(serverIdleTimeoutMillis * 2);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();

        Assert.fail(
            "The previous code should throw exception because the server should close the connection because of idle timeout");
      } catch (Throwable ex) {
        if (!ex.getClass().equals(RssNetworkException.class) &&
            !ex.getClass().equals(RssFinishUploadException.class)) {
          throw ex;
        }
      }

      Assert.assertEquals(writeClientFactory.getNumIdleClients(), 0);
    } finally {
      testServer1.shutdown();
      writeClientFactory.shutdown();
    }
  }
}
