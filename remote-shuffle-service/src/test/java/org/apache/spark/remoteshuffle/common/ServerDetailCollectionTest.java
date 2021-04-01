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

package org.apache.spark.remoteshuffle.common;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ServerDetailCollectionTest {
  @Test
  public void addServer() {
    ServerDetailCollection serverDetailCollection = new ServerDetailCollection();

    Assert.assertEquals(serverDetailCollection.getServers("dc1", "cluster1").size(), 0);

    serverDetailCollection.addServer("dc1", "cluster1", new ServerDetail("server1", "node1:1"));
    serverDetailCollection.addServer("dc1", "cluster1", new ServerDetail("server1", "node2:2"));

    Assert.assertEquals(serverDetailCollection.getServers("dc1", "cluster1"),
        Arrays.asList(new ServerDetail("server1", "node2:2")));

    serverDetailCollection.addServer("dc1", "cluster1", new ServerDetail("server2", "node2:2"));

    List<ServerDetail> servers = serverDetailCollection.getServers("dc1", "cluster1");
    servers.sort(Comparator.comparing(ServerDetail::getServerId));
    Assert.assertEquals(servers,
        Arrays.asList(new ServerDetail("server1", "node2:2"),
            new ServerDetail("server2", "node2:2")));
  }
}
