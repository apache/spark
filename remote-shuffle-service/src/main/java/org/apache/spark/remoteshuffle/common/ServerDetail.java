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

import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

import java.util.Objects;

public class ServerDetail {
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, serverId);
    ByteBufUtils.writeLengthAndString(buf, connectionString);
  }

  public static ServerDetail deserialize(ByteBuf buf) {
    String serverId = ByteBufUtils.readLengthAndString(buf);
    String connectionString = ByteBufUtils.readLengthAndString(buf);
    return new ServerDetail(serverId, connectionString);
  }

  private String serverId;
  private String connectionString;

  public ServerDetail(String serverId, String connectionString) {
    this.serverId = serverId;
    this.connectionString = connectionString;
  }

  public String getServerId() {
    return serverId;
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerDetail that = (ServerDetail) o;
    return Objects.equals(serverId, that.serverId) &&
        Objects.equals(connectionString, that.connectionString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serverId);
  }

  @Override
  public String toString() {
    return "Server{" +
        connectionString +
        ", " + serverId +
        '}';
  }
}
