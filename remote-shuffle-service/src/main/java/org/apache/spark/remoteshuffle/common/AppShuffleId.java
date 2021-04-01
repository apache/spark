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
 * Fully qualified ID for application shuffle stage.
 */
public class AppShuffleId {
  private final String appId;
  private final String appAttempt;
  private final int shuffleId;

  public AppShuffleId(String appId, String appAttempt, int shuffleId) {
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.shuffleId = shuffleId;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AppShuffleId that = (AppShuffleId) o;
    return shuffleId == that.shuffleId &&
        Objects.equals(appId, that.appId) &&
        Objects.equals(appAttempt, that.appAttempt);
  }

  @Override
  public int hashCode() {

    return Objects.hash(appId, appAttempt, shuffleId);
  }

  @Override
  public String toString() {
    return "AppShuffleId{" +
        "appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", shuffleId=" + shuffleId +
        '}';
  }
}
