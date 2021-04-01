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
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ServiceRegistryTestUtil {
  public final static int ZK_TIMEOUT_MILLIS = 1000;
  public final static int ZK_MAX_RETRIES = 0;

  public static void lookupServers_emptyList(ServiceRegistry serviceRegistry) {
    List<ServerDetail> result =
        serviceRegistry.lookupServers("dc1", "cluster1", new ArrayList<>());
    Assert.assertEquals(result.size(), 0);
  }

  public static void lookupServers_dcWithoutServerRegistered(ServiceRegistry serviceRegistry) {
    serviceRegistry.registerServer("dc1", "cluster1", "server1", "host1:1");
    serviceRegistry.lookupServers("dc2", "cluster1", Arrays.asList("server1"));
  }

  public static void lookupServers_clusterWithoutServerRegistered(
      ServiceRegistry serviceRegistry) {
    serviceRegistry.registerServer("dc1", "cluster1", "server1", "host1:1");
    serviceRegistry.lookupServers("dc1", "cluster2", Arrays.asList("server1"));
  }

  public static void lookupServers_badServerIds(ServiceRegistry serviceRegistry) {
    serviceRegistry.registerServer("dc1", "cluster1", "server1", "host1:1");
    serviceRegistry.lookupServers("dc1", "cluster1", Arrays.asList("server3"));
  }

  public static void lookupServers_oneGoodServerIdOneBad(ServiceRegistry serviceRegistry) {
    serviceRegistry.registerServer("dc1", "cluster1", "server1", "host1:1");
    serviceRegistry.lookupServers("dc1", "cluster1", Arrays.asList("server1", "server3"));
  }
}
