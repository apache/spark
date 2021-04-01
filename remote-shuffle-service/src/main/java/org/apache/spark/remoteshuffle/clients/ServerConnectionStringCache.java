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

import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerList;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class caches server connections by server ids. It also allow outside code to update
 * the cache.
 */
public class ServerConnectionStringCache {

  private final static ServerConnectionStringCache instance = new ServerConnectionStringCache();

  public static ServerConnectionStringCache getInstance() {
    return instance;
  }

  private ConcurrentHashMap<String, ServerDetail> servers = new ConcurrentHashMap<>();

  public ServerConnectionStringCache() {
  }

  public ServerDetail getServer(ServerDetail serverDetail) {
    ServerDetail cachedServerDetail = servers.get(serverDetail.getServerId());
    if (cachedServerDetail != null) {
      return cachedServerDetail;
    } else {
      return serverDetail;
    }
  }

  public ServerList getServerList(ServerList serverList) {
    return new ServerList(
        serverList.getSevers().stream().map(this::getServer).collect(Collectors.toList()));
  }

  public void updateServer(String serverId, ServerDetail serverDetail) {
    servers.put(serverId, serverDetail);
  }
}
