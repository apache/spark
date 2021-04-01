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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Base64;

/***
 * This class store track information for a map task, e.g. task attempt id.
 */
public class MapTaskRssInfo {
  private final int mapId;
  private final long taskAttemptId;
  private final int numRssServers;

  public void serialize(ByteBuf buf) {
    buf.writeInt(mapId);
    buf.writeLong(taskAttemptId);
    buf.writeInt(numRssServers);
  }

  public static MapTaskRssInfo deserialize(ByteBuf buf) {
    int mapId = buf.readInt();
    long taskAttemptId = buf.readLong();
    int numRssServers = buf.readInt();
    return new MapTaskRssInfo(mapId, taskAttemptId, numRssServers);
  }

  /***
   * This serialize method is faster than json serialization.
   * @return
   */
  public String serializeToString() {
    ByteBuf buf = Unpooled.buffer();
    try {
      serialize(buf);
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      return Base64.getEncoder().encodeToString(bytes);
    } finally {
      buf.release();
    }
  }

  public static MapTaskRssInfo deserializeFromString(String str) {
    byte[] bytes = Base64.getDecoder().decode(str);
    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      return deserialize(buf);
    } finally {
      buf.release();
    }
  }

  public MapTaskRssInfo(int mapId, long taskAttemptId, int numRssServers) {
    this.mapId = mapId;
    this.taskAttemptId = taskAttemptId;
    this.numRssServers = numRssServers;
  }

  public int getMapId() {
    return mapId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public int getNumRssServers() {
    return numRssServers;
  }

  @Override
  public String toString() {
    return "MapTaskRssInfo{" +
        "mapId=" + mapId +
        ", taskAttemptId=" + taskAttemptId +
        ", numRssServers=" + numRssServers +
        '}';
  }
}
