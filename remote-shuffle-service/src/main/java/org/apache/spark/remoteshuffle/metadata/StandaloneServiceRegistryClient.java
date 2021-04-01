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

import org.apache.spark.remoteshuffle.clients.RegistryClient;
import org.apache.spark.remoteshuffle.common.ServerDetail;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/***
 * This is a service registry client connecting to a standalone registry server, which could be used in
 * a test environment without zookeeper, e.g. open source.
 * TODO this class currently create a new connection for each method call. Will improve that in future
 * to reuse connection and also recreate connection on failures.
 */
public class StandaloneServiceRegistryClient implements ServiceRegistry {
  private final String host;
  private final int port;
  private final int timeoutMillis;
  private final String user;

  public StandaloneServiceRegistryClient(String host, int port, int timeoutMillis, String user) {
    this.host = host;
    this.port = port;
    this.timeoutMillis = timeoutMillis;
    this.user = user;
  }

  @Override
  public void registerServer(String dataCenter, String cluster, String serverId,
                             String hostAndPort) {
    try (RegistryClient registryClient = new RegistryClient(host, port, timeoutMillis, user)) {
      registryClient.connect();
      registryClient.registerServer(dataCenter, cluster, serverId, hostAndPort);
    }
  }

  @Override
  public List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount,
                                       Collection<String> excludeHosts) {
    try (RegistryClient registryClient = new RegistryClient(host, port, timeoutMillis, user)) {
      registryClient.connect();
      List<ServerDetail> list =
          registryClient.getServers(dataCenter, cluster, maxCount + excludeHosts.size());
      return ServiceRegistryUtils.excludeByHosts(list, maxCount, excludeHosts);
    }
  }

  @Override
  public List<ServerDetail> lookupServers(String dataCenter, String cluster,
                                          Collection<String> serverIds) {
    try (RegistryClient registryClient = new RegistryClient(host, port, timeoutMillis, user)) {
      registryClient.connect();
      List<ServerDetail> servers =
          registryClient.getServers(dataCenter, cluster, Integer.MAX_VALUE);
      return servers.stream().filter(t -> serverIds.contains(t.getServerId()))
          .collect(Collectors.toList());
    }
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    return "StandaloneServiceRegistryClient{" +
        "host='" + host + '\'' +
        ", port=" + port +
        ", timeoutMillis=" + timeoutMillis +
        ", user='" + user + '\'' +
        '}';
  }
}
