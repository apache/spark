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

import java.util.HashMap;
import java.util.Map;

public class GetBusyStatusResponse extends BaseMessage {
  private final Map<Long, Long> metrics;
  private final Map<String, String> metadata;

  public GetBusyStatusResponse(Map<Long, Long> metrics, Map<String, String> metadata) {
    this.metrics = new HashMap<>(metrics);
    this.metadata = new HashMap<>(metadata);
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_GetBusyStatusResponse;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(metrics.size());
    for (Map.Entry<Long, Long> entry : metrics.entrySet()) {
      buf.writeLong(entry.getKey());
      buf.writeLong(entry.getValue());
    }
    buf.writeInt(metadata.size());
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      ByteBufUtils.writeLengthAndString(buf, entry.getKey());
      ByteBufUtils.writeLengthAndString(buf, entry.getValue());
    }
  }

  public static GetBusyStatusResponse deserialize(ByteBuf buf) {
    int size = buf.readInt();
    Map<Long, Long> metricsMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      long key = buf.readLong();
      long value = buf.readLong();
      metricsMap.put(key, value);
    }
    size = buf.readInt();
    Map<String, String> metadataMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      String key = ByteBufUtils.readLengthAndString(buf);
      String value = ByteBufUtils.readLengthAndString(buf);
      metadataMap.put(key, value);
    }
    return new GetBusyStatusResponse(metricsMap, metadataMap);
  }

  public Map<Long, Long> getMetrics() {
    return metrics;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "GetBusyStatusResponse{" +
        "metrics=" + metrics +
        ", metadata=" + metadata +
        '}';
  }
}
