/*
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/***
 * This is in service registry for continuous server sequence.
 */
public class ServerSequenceServiceRegistry implements ServiceRegistry {
  private static final String DEFAULT_CLUSTER = "default";

  private static final Logger logger =
      LoggerFactory.getLogger(ServerSequenceServiceRegistry.class);

  private final String serverIdFormat; // String format for server ID: e.g. "rss-%s"
  private final String connectionStringFormat;
      // String format for connection string: e.g. "rss-%s.rss.remote-shuffle-service.svc.cluster.local:9338"
  private final int sequenceStartIndex;
  private final int sequenceEndIndex;

  private final ServerDetailCollection serverCollection = new ServerDetailCollection();

  public ServerSequenceServiceRegistry(String serverIdFormat, String connectionStringFormat,
                                       int sequenceStartIndex, int sequenceEndIndex) {
    this.serverIdFormat = serverIdFormat;
    this.connectionStringFormat = connectionStringFormat;
    this.sequenceStartIndex = sequenceStartIndex;
    this.sequenceEndIndex = sequenceEndIndex;

    for (int i = sequenceStartIndex; i <= sequenceEndIndex; i++) {
      String serverId = String.format(serverIdFormat, i);
      ServerDetail serverDetail =
          new ServerDetail(serverId, String.format(connectionStringFormat, i));
      serverCollection
          .addServer(DEFAULT_DATA_CENTER, DEFAULT_CLUSTER, serverDetail);
    }

    logger.info("Created " + this);
  }

  @Override
  public synchronized void registerServer(String dataCenter, String cluster, String serverId,
                                          String hostAndPort) {
    return;
  }

  @Override
  public synchronized List<ServerDetail> getServers(String dataCenter, String cluster,
                                                    int maxCount,
                                                    Collection<String> excludeHosts) {
    // we do not care about data center and cluster, thus set them to default values
    dataCenter = DEFAULT_DATA_CENTER;
    cluster = DEFAULT_CLUSTER;

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
    // we do not care about data center and cluster, thus set them to default values
    dataCenter = DEFAULT_DATA_CENTER;
    cluster = DEFAULT_CLUSTER;
    return serverCollection.lookupServers(dataCenter, cluster, serverIds);
  }

  @Override
  public synchronized void close() {
  }
}
