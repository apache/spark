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

import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class NotifyClientTest {
  private static final Logger logger = LoggerFactory.getLogger(NotifyClientTest.class);

  @Test
  public void finishApplicationJob() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try (NotifyClient client = new NotifyClient("localhost", testServer.getShufflePort(),
        TestConstants.NETWORK_TIMEOUT, "user1")) {
      client.connect();
      // send same request twice to make sure it is still good
      client.finishApplicationJob("app1", "exec1", 1, "success", null, null);
      client.finishApplicationJob("app1", "exec1", 1, "success", "", "");
      // send different request to make sure it is still good
      client.finishApplicationJob("app1", "exec1", 2, "fail", "RuntimeException",
          "Exception \nStacktrace");
    } finally {
      testServer.shutdown();
    }
  }

  @Test
  public void finishApplicationAttempt() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try (NotifyClient client = new NotifyClient("localhost", testServer.getShufflePort(),
        TestConstants.NETWORK_TIMEOUT, "user1")) {
      client.connect();
      // send same request twice to make sure it is still good
      client.finishApplicationAttempt("app1", "exec1");
      client.finishApplicationAttempt("app1", "exec1");
      // send different request to make sure it is still good
      client.finishApplicationAttempt("app1", "exec2");
      client.finishApplicationAttempt("app2", "exec2");
    } finally {
      testServer.shutdown();
    }
  }

}
