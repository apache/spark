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

import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class GetServersRequestMessage extends ControlMessage {
  private String dataCenter;
  private String cluster;
  private int maxCount;

  public GetServersRequestMessage(String dataCenter, String cluster, int maxCount) {
    this.dataCenter = dataCenter;
    this.cluster = cluster;
    this.maxCount = maxCount;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_GetServersRequest;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, dataCenter);
    ByteBufUtils.writeLengthAndString(buf, cluster);
    buf.writeInt(maxCount);
  }

  public static GetServersRequestMessage deserialize(ByteBuf buf) {
    String dataCenter = ByteBufUtils.readLengthAndString(buf);
    String cluster = ByteBufUtils.readLengthAndString(buf);
    int maxCount = buf.readInt();
    return new GetServersRequestMessage(dataCenter, cluster, maxCount);
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public String getCluster() {
    return cluster;
  }

  public int getMaxCount() {
    return maxCount;
  }

  @Override
  public String toString() {
    return "GetServersRequestMessage{" +
        "dataCenter='" + dataCenter + '\'' +
        ", cluster='" + cluster + '\'' +
        ", maxCount='" + maxCount + '\'' +
        '}';
  }
}
