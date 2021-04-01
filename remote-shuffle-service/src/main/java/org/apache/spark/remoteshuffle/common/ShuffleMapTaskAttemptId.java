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

import java.util.Objects;

/***
 * Application shuffle map task attempt id without app id / app attempt id
 */
public class ShuffleMapTaskAttemptId {
  private final int shuffleId;
  private final int mapId;
  private final long taskAttemptId;

  public ShuffleMapTaskAttemptId(int shuffleId, int mapId, long taskAttemptId) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.taskAttemptId = taskAttemptId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getMapId() {
    return mapId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShuffleMapTaskAttemptId that = (ShuffleMapTaskAttemptId) o;
    return shuffleId == that.shuffleId &&
        mapId == that.mapId &&
        taskAttemptId == that.taskAttemptId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(shuffleId, mapId, taskAttemptId);
  }

  @Override
  public String toString() {
    return "ShuffleMapTaskAttemptId{" +
        "shuffleId=" + shuffleId +
        ", mapId=" + mapId +
        ", taskAttemptId=" + taskAttemptId +
        '}';
  }
}
