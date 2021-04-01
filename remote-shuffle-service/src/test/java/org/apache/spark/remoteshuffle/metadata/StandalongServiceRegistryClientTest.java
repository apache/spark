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

package org.apache.spark.remoteshuffle.metadata;

import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class StandalongServiceRegistryClientTest {

  @Test
  public void getServers() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try (StandaloneServiceRegistryClient client = new StandaloneServiceRegistryClient("localhost",
        testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
      List<ServerDetail> servers =
          client.getServers("dc1", "cluster1", 0, Collections.emptyList());
      Assert.assertEquals(servers.size(), 0);

      servers = client.getServers("dc1", "cluster1", 10, Collections.emptyList());
      Assert.assertEquals(servers.size(), 0);

      client.registerServer("dc1", "cluster1", "server1", "host1:1");
      servers = client.getServers("dc1", "cluster1", 10, Collections.emptyList());
      Assert.assertEquals(servers.size(), 1);
      Assert.assertEquals(servers, Arrays.asList(new ServerDetail("server1", "host1:1")));

      servers = client.getServers("dc1", "cluster2", 10, Collections.emptyList());
      Assert.assertEquals(servers.size(), 0);

      client.registerServer("dc1", "cluster1", "server2", "host2:2");
      client.registerServer("dc1", "cluster1", "server3", "host3:3");

      servers = client.lookupServers("dc1", "cluster1", Arrays.asList("server2", "server3"));
      Assert.assertEquals(servers.size(), 2);
      servers.sort(Comparator.comparing(ServerDetail::getServerId));
      Assert.assertEquals(servers,
          Arrays.asList(
              new ServerDetail("server2", "host2:2"),
              new ServerDetail("server3", "host3:3")));
    } finally {
      testServer.shutdown();
    }
  }

}
