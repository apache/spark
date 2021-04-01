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

package org.apache.spark.remoteshuffle.metadata;

import org.apache.spark.remoteshuffle.common.ServerDetail;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/***
 * This is the interface to hold server instances.
 */
public interface ServiceRegistry extends AutoCloseable {

  String TYPE_INMEMORY = "inmemory";
  String TYPE_STANDALONE = "standalone";
  String TYPE_SERVER_SEQUENCE = "serverSequence";
  List<String> VALID_TYPES = Arrays.asList(
      TYPE_INMEMORY,
      TYPE_STANDALONE,
      TYPE_SERVER_SEQUENCE
  );

  String DEFAULT_DATA_CENTER = "default";

  // This is only used in tests and test tools. The actual default is calculated at runtime
  String DEFAULT_TEST_CLUSTER = "default";

  /***
   * Register a server.
   * @param dataCenter data center.
   * @param cluster cluster name.
   * @param serverId unique id for the server
   * @param hostAndPort the host and port for the server.
   */
  void registerServer(String dataCenter, String cluster, String serverId, String hostAndPort);

  /***
   * Get servers.
   * @param dataCenter data center.
   * @param cluster cluster name.
   * @param maxCount the max number of servers to return.
   * @param excludeHosts excluding server host names
   * @return the server list.
   */
  List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount,
                                Collection<String> excludeHosts);

  /***
   * Look up servers by a given list of server ids
   * @param dataCenter data center.
   * @param cluster cluster name.
   * @param serverIds the given list of server ids to loop up servers.
   * @return the server list.
   */
  List<ServerDetail> lookupServers(String dataCenter, String cluster,
                                   Collection<String> serverIds);

  void close();
}
