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

import org.apache.spark.remoteshuffle.exceptions.RssException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/***
 * This class holds all server information. Methods are thread safe.
 */
public class ServerDetailCollection {
  private final ConcurrentHashMap<DataCenterAndCluster, ConcurrentHashMap<String, ServerDetail>>
      container = new ConcurrentHashMap<>();

  public void addServer(String dataCenter, String cluster, ServerDetail serverDetail) {
    container.computeIfAbsent(new DataCenterAndCluster(dataCenter, cluster),
        t -> new ConcurrentHashMap<>())
        .put(serverDetail.getServerId(), serverDetail);
  }

  public List<ServerDetail> getServers(String dataCenter, String cluster) {
    return new ArrayList<>(container.computeIfAbsent(new DataCenterAndCluster(dataCenter, cluster),
        t -> new ConcurrentHashMap<>())
        .values());
  }

  public List<ServerDetail> lookupServers(String dataCenter, String cluster,
                                          Collection<String> serverIds) {
    ConcurrentHashMap<String, ServerDetail> map = container
        .computeIfAbsent(new DataCenterAndCluster(dataCenter, cluster),
            t -> new ConcurrentHashMap<>());
    return serverIds.stream().map(t -> {
      ServerDetail serverDetail = map.get(t);
      if (serverDetail == null) {
        throw new RssException(String.format("Server %s not exist", t));
      }
      return serverDetail;
    }).collect(Collectors.toList());
  }

  private class DataCenterAndCluster {
    private String dataCenter;
    private String cluster;

    public DataCenterAndCluster(String dataCenter, String cluster) {
      this.dataCenter = dataCenter;
      this.cluster = cluster;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataCenterAndCluster that = (DataCenterAndCluster) o;
      return Objects.equals(dataCenter, that.dataCenter) &&
          Objects.equals(cluster, that.cluster);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataCenter, cluster);
    }

    @Override
    public String toString() {
      return "DataCenterAndCluster{" +
          "dataCenter='" + dataCenter + '\'' +
          ", cluster='" + cluster + '\'' +
          '}';
    }
  }
}
