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

package org.apache.spark.remoteshuffle.execution;

import java.util.concurrent.atomic.AtomicLong;

/***
 * This class contains the state for each application.
 */
public class ExecutorAppState {
  private final String appId;

  private final AtomicLong numWriteBytes = new AtomicLong();

  // The timestamp (milliseconds) to indicate the liveness of the shuffle stage
  private final AtomicLong livenessTimestamp = new AtomicLong(System.currentTimeMillis());

  public ExecutorAppState(String appId) {
    this.appId = appId;
  }

  public final String getAppId() {
    return appId;
  }

  public final void updateLivenessTimestamp() {
    livenessTimestamp.set(System.currentTimeMillis());
  }

  public final long getLivenessTimestamp() {
    return livenessTimestamp.get();
  }

  public final long addNumWriteBytes(long delta) {
    return numWriteBytes.addAndGet(delta);
  }

  public final long getNumWriteBytes() {
    return numWriteBytes.get();
  }

  @Override
  public String toString() {
    return "ExecutorAppState{" +
        "appId='" + appId + '\'' +
        ", numWriteBytes=" + numWriteBytes.get() +
        ", livenessTimestamp=" + livenessTimestamp +
        '}';
  }
}
