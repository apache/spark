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

package org.apache.spark.remoteshuffle.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class ServerList {
  final private List<ServerDetail> serverList;

  @JsonCreator
  public ServerList(@JsonProperty("servers") Collection<ServerDetail> servers) {
    this.serverList = new ArrayList<>(servers);
  }

  @JsonCreator
  public ServerList(@JsonProperty("servers") ServerDetail[] servers) {
    this.serverList = Arrays.asList(servers);
  }

  @JsonProperty("servers")
  public List<ServerDetail> getSevers() {
    return new ArrayList<>(serverList);
  }

  @JsonIgnore
  public int getSeverCount() {
    return serverList.size();
  }

  @JsonIgnore
  public ServerDetail getSeverDetail(String serverId) {
    for (ServerDetail entry : serverList) {
      if (entry.getServerId().equals(serverId)) {
        return entry;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerList that = (ServerList) o;
    return Objects.equals(serverList, that.serverList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serverList);
  }

  @Override
  public String toString() {
    return "ServerList{" +
        StringUtils.join(serverList, ',') +
        '}';
  }
}
