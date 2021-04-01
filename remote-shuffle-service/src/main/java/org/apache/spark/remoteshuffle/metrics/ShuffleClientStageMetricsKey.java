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

package org.apache.spark.remoteshuffle.metrics;

import java.util.Objects;

public class ShuffleClientStageMetricsKey {
  private String user;
  private String queue;

  public ShuffleClientStageMetricsKey(String user, String queue) {
    this.user = user;
    this.queue = queue;
  }

  public String getUser() {
    return user;
  }

  public String getQueue() {
    return queue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShuffleClientStageMetricsKey that = (ShuffleClientStageMetricsKey) o;
    return Objects.equals(user, that.user) &&
        Objects.equals(queue, that.queue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, queue);
  }

  @Override
  public String toString() {
    return "ShuffleClientStageMetricsKey{" +
        "user='" + user + '\'' +
        ", queue='" + queue + '\'' +
        '}';
  }
}
