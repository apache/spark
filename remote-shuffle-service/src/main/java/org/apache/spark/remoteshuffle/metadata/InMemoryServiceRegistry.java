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
import org.apache.spark.remoteshuffle.common.ServerDetailCollection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/***
 * This is in memory service registry for testing purpose.
 */
public class InMemoryServiceRegistry implements ServiceRegistry {
  private static final String DEFAULT_SERVER_ROOT_URL = "http://localhost:58808";

  private static final Logger logger = LoggerFactory.getLogger(InMemoryServiceRegistry.class);

  private final ServerDetailCollection serverCollection = new ServerDetailCollection();

  public InMemoryServiceRegistry() {
    logger.info("Created " + this);
  }

  @Override
  public String toString() {
    return "InMemoryServiceRegistry{}";
  }

  @Override
  public synchronized void registerServer(String dataCenter, String cluster, String serverId,
                                          String hostAndPort) {
    if (StringUtils.isBlank(dataCenter)) {
      throw new IllegalArgumentException(
          String.format("Invalid input: dataCenter=%s", dataCenter));
    }

    if (StringUtils.isBlank(cluster)) {
      throw new IllegalArgumentException(String.format("Invalid input: cluster=%s", cluster));
    }

    if (StringUtils.isBlank(serverId)) {
      throw new IllegalArgumentException(String.format("Invalid input: serverId=%s", serverId));
    }

    if (StringUtils.isBlank(hostAndPort)) {
      throw new IllegalArgumentException(
          String.format("Invalid input: hostAndPort=%s", hostAndPort));
    }

    serverCollection.addServer(dataCenter, cluster, new ServerDetail(serverId, hostAndPort));
  }

  @Override
  public synchronized List<ServerDetail> getServers(String dataCenter, String cluster,
                                                    int maxCount,
                                                    Collection<String> excludeHosts) {
    if (StringUtils.isBlank(dataCenter)) {
      throw new IllegalArgumentException(
          String.format("Invalid input: dataCenter=%s", dataCenter));
    }

    if (StringUtils.isBlank(cluster)) {
      throw new IllegalArgumentException(String.format("Invalid input: cluster=%s", cluster));
    }

    List<ServerDetail> servers = serverCollection.getServers(dataCenter, cluster);

    if (servers.size() <= maxCount) {
      return new ArrayList<>(servers);
    } else {
      return servers.subList(0, maxCount);
    }
  }

  @Override
  public List<ServerDetail> lookupServers(String dataCenter, String cluster,
                                          Collection<String> serverIds) {
    return serverCollection.lookupServers(dataCenter, cluster, serverIds);
  }

  @Override
  public synchronized void close() {
  }
}
