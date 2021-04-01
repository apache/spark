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
import org.apache.spark.remoteshuffle.exceptions.RssException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class InMemoryServiceRegistryTest {
  @Test
  public void registerServer() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();

    Assert.assertEquals(
        serviceRegistry.getServers("dc1", "cluster1", 0, Collections.emptyList()).size(), 0);

    serviceRegistry.registerServer("dc1", "cluster1", "server1", "node1:1");
    serviceRegistry.registerServer("dc1", "cluster1", "server2", "node2:2");

    List<ServerDetail> result =
        serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
    result.sort(Comparator.comparing(ServerDetail::getServerId));
    Assert.assertEquals(result,
        Arrays.asList(new ServerDetail("server1", "node1:1"),
            new ServerDetail("server2", "node2:2")));

    result = serviceRegistry.lookupServers("dc1", "cluster1", Arrays.asList("server1", "server2"));
    Assert.assertEquals(result,
        Arrays.asList(new ServerDetail("server1", "node1:1"),
            new ServerDetail("server2", "node2:2")));
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_NullDataCenter() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer(null, "cluster1", "server1", "node1:1");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_EmptyDataCenter() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer("", "cluster1", "server1", "node1:1");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_NullCluster() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer("dc1", null, "server1", "node1:1");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_EmptyCluster() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer("dc1", "", "server1", "node1:1");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_NullHostPort() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer("dc1", "cluster1", "server1", null);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_EmptyHostPort() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer("dc1", "cluster1", "server1", "");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_NullServerId() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer("dc1", "cluster1", null, "node1:1");
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void registerServer_EmptyServerId() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    serviceRegistry.registerServer("dc1", "cluster1", "", "node1:1");
  }

  @Test
  public void lookupServers_emptyList() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    ServiceRegistryTestUtil.lookupServers_emptyList(serviceRegistry);
  }

  @Test(expectedExceptions = {RssException.class})
  public void lookupServers_dcWithoutServerRegistered() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    ServiceRegistryTestUtil.lookupServers_dcWithoutServerRegistered(serviceRegistry);
  }

  @Test(expectedExceptions = {RssException.class})
  public void lookupServers_clusterWithoutServerRegistered() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    ServiceRegistryTestUtil.lookupServers_clusterWithoutServerRegistered(serviceRegistry);
  }

  @Test(expectedExceptions = {RssException.class})
  public void lookupServers_badServerIds() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    ServiceRegistryTestUtil.lookupServers_badServerIds(serviceRegistry);
  }

  @Test(expectedExceptions = {RssException.class})
  public void lookupServers_oneGoodServerIdOneBad() {
    InMemoryServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    ServiceRegistryTestUtil.lookupServers_oneGoodServerIdOneBad(serviceRegistry);
  }
}
