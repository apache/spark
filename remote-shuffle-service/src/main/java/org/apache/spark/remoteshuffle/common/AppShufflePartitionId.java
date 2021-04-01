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
 * Fully qualified ID for application shuffle partition.
 */
public class AppShufflePartitionId {
  private final String appId;
  private final String appAttempt;
  private final int shuffleId;
  private final int partitionId;

  public AppShufflePartitionId(AppShuffleId appShuffleId,
                               int partitionId) {
    this(appShuffleId.getAppId(),
        appShuffleId.getAppAttempt(),
        appShuffleId.getShuffleId(),
        partitionId);
  }

  public AppShufflePartitionId(String appId,
                               String appAttempt,
                               int shuffleId,
                               int partitionId) {
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
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

  public int getPartitionId() {
    return partitionId;
  }

  public AppShuffleId getAppShuffleId() {
    return new AppShuffleId(appId, appAttempt, shuffleId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AppShufflePartitionId that = (AppShufflePartitionId) o;
    return shuffleId == that.shuffleId &&
        partitionId == that.partitionId &&
        Objects.equals(appId, that.appId) &&
        Objects.equals(appAttempt, that.appAttempt);
  }

  @Override
  public int hashCode() {

    return Objects.hash(appId, appAttempt, shuffleId, partitionId);
  }

  @Override
  public String toString() {
    return "AppShufflePartitionId{" +
        "appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", shuffleId=" + shuffleId +
        ", partitionId=" + partitionId +
        '}';
  }
}
