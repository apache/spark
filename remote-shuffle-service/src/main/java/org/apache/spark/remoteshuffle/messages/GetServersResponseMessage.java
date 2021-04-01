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

package org.apache.spark.remoteshuffle.messages;

import org.apache.spark.remoteshuffle.common.ServerDetail;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class GetServersResponseMessage extends ServerResponseMessage {
  private List<ServerDetail> servers;

  public GetServersResponseMessage(List<ServerDetail> servers) {
    this.servers = servers;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_GetServersResponse;
  }

  @Override
  public void serialize(ByteBuf buf) {
    if (servers != null) {
      buf.writeInt(servers.size());
      for (ServerDetail s : servers) {
        s.serialize(buf);
      }
    } else {
      buf.writeInt(0);
    }
  }

  public static GetServersResponseMessage deserialize(ByteBuf buf) {
    int count = buf.readInt();
    List<ServerDetail> servers = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ServerDetail serverDetail = ServerDetail.deserialize(buf);
      servers.add(serverDetail);
    }
    return new GetServersResponseMessage(servers);
  }

  public List<ServerDetail> getServers() {
    return servers;
  }

  @Override
  public String toString() {
    return "GetServersResponseMessage{" +
        "servers=" + servers +
        '}';
  }
}
