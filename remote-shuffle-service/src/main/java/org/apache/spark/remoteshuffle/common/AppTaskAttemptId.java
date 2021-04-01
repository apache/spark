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
 * Fully qualified ID for application map task attempt.
 */
public class AppTaskAttemptId {
  private final String appId;
  private final String appAttempt;
  private final int shuffleId;
  private final int mapId;
  private final long taskAttemptId;

  public AppTaskAttemptId(AppShuffleId appShuffleId, int mapId, long taskAttemptId) {
    this(appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(), mapId,
        taskAttemptId);
  }

  public AppTaskAttemptId(AppMapId appMapId, long taskAttemptId) {
    this(appMapId.getAppId(), appMapId.getAppAttempt(), appMapId.getShuffleId(),
        appMapId.getMapId(), taskAttemptId);
  }

  public AppTaskAttemptId(String appId, String appAttempt, int shuffleId, int mapId,
                          long taskAttemptId) {
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.taskAttemptId = taskAttemptId;
  }

  public String getAppId() {
    return appId;
  }

  public String getAppAttempt() {
    return appAttempt;
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

  public AppShuffleId getAppShuffleId() {
    return new AppShuffleId(appId, appAttempt, shuffleId);
  }

  public AppMapId getAppMapId() {
    return new AppMapId(appId, appAttempt, shuffleId, mapId);
  }

  public ShuffleMapTaskAttemptId getShuffleMapTaskAttemptId() {
    return new ShuffleMapTaskAttemptId(shuffleId, mapId, taskAttemptId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AppTaskAttemptId that = (AppTaskAttemptId) o;
    return shuffleId == that.shuffleId &&
        mapId == that.mapId &&
        taskAttemptId == that.taskAttemptId &&
        Objects.equals(appId, that.appId) &&
        Objects.equals(appAttempt, that.appAttempt);
  }

  @Override
  public int hashCode() {

    return Objects.hash(appId, appAttempt, shuffleId, mapId, taskAttemptId);
  }

  @Override
  public String toString() {
    return "AppTaskAttemptId{" +
        "appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", shuffleId=" + shuffleId +
        ", mapId=" + mapId +
        ", taskAttemptId=" + taskAttemptId +
        '}';
  }
}
