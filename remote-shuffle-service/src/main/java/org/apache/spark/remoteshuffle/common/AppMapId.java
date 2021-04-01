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

import java.util.Objects;

/***
 * Fully qualified ID for application map task.
 */
public class AppMapId {
  private final String appId;
  private final String appAttempt;
  private final int shuffleId;
  private final int mapId;

  public AppMapId(AppShuffleId appShuffleId, int mapId) {
    this(appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(),
        mapId);
  }

  public AppMapId(String appId, String appAttempt, int shuffleId, int mapId) {
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
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

  public AppShuffleId getAppShuffleId() {
    return new AppShuffleId(appId, appAttempt, shuffleId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AppMapId appMapId = (AppMapId) o;
    return shuffleId == appMapId.shuffleId &&
        mapId == appMapId.mapId &&
        Objects.equals(appId, appMapId.appId) &&
        Objects.equals(appAttempt, appMapId.appAttempt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, appAttempt, shuffleId, mapId);
  }

  @Override
  public String toString() {
    return "AppMapId{" +
        "appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", shuffleId=" + shuffleId +
        ", mapId=" + mapId +
        '}';
  }
}
