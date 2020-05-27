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

package org.apache.spark.network.remoteshuffle;

import java.util.Objects;

/**
 * A shuffle stage fully qualified ID.
 */
public class ShuffleStageFqid {
  private final String appId;
  private final String execId;
  private final int shuffleId;
  private final int stageAttempt;

  public ShuffleStageFqid(String appId, String execId, int shuffleId, int stageAttempt) {
    this.appId = appId;
    this.execId = execId;
    this.shuffleId = shuffleId;
    this.stageAttempt = stageAttempt;
  }

  public String getAppId() {
    return appId;
  }

  public String getExecId() {
    return execId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStageAttempt() {
    return stageAttempt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShuffleStageFqid that = (ShuffleStageFqid) o;
    return shuffleId == that.shuffleId &&
        stageAttempt == that.stageAttempt &&
        Objects.equals(appId, that.appId) &&
        Objects.equals(execId, that.execId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, execId, shuffleId, stageAttempt);
  }

  @Override
  public String toString() {
    return "ShuffleStageFqid{" +
        "appId='" + appId + '\'' +
        ", execId='" + execId + '\'' +
        ", shuffleId=" + shuffleId +
        ", stageAttempt=" + stageAttempt +
        '}';
  }
}
