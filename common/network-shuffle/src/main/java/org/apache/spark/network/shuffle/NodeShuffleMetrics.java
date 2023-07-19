/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Class to encapsulate shuffle metrics around total shuffle data on node of shuffle service
 */
class NodeShuffleMetrics {
    private final long totalShuffleDataBytes;
    private final int numAppsWithShuffleData;
    private final long lastRefreshEpochMillis;

    NodeShuffleMetrics(long totalShuffleDataBytes, int numAppsWithShuffleData, long lastRefreshEpochMillis) {
      this.totalShuffleDataBytes = totalShuffleDataBytes;
      this.numAppsWithShuffleData = numAppsWithShuffleData;
      this.lastRefreshEpochMillis = lastRefreshEpochMillis;
    }

    long getTotalShuffleDataBytes() {
      return this.totalShuffleDataBytes;
    }

    int getNumAppsWithShuffleData() {
      return this.numAppsWithShuffleData;
    }

    long getLastRefreshEpochMillis() {
      return this.lastRefreshEpochMillis;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      NodeShuffleMetrics other = (NodeShuffleMetrics) obj;
      return other.getTotalShuffleDataBytes() == this.getTotalShuffleDataBytes() &&
          other.getNumAppsWithShuffleData() == this.getNumAppsWithShuffleData() &&
          other.getLastRefreshEpochMillis() == this.getLastRefreshEpochMillis();
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("totalShuffleData", totalShuffleDataBytes)
          .append("numAppsWithShuffleData", numAppsWithShuffleData)
          .append("lastRefreshEpochMillis", lastRefreshEpochMillis)
          .toString();
    }
  }