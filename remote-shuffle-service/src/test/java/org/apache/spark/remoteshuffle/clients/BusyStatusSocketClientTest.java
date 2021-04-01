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

import org.apache.spark.remoteshuffle.messages.GetBusyStatusResponse;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BusyStatusSocketClientTest {
  @Test
  public void connectServer() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try (BusyStatusSocketClient client = new BusyStatusSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
      client.close();
      client.close();
    }

    testServer1.shutdown();
  }

  @Test
  public void getBusyStatus() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try (BusyStatusSocketClient client = new BusyStatusSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
      GetBusyStatusResponse response = client.getBusyStatus();
      Assert.assertNotNull(response);
      Assert.assertEquals(response.getMetrics().size(), 1);
    }

    testServer1.shutdown();
  }
}
