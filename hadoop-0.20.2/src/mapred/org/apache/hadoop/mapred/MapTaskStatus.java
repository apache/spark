/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;


class MapTaskStatus extends TaskStatus {

  public MapTaskStatus() {}

  public MapTaskStatus(TaskAttemptID taskid, float progress, int numSlots,
          State runState, String diagnosticInfo, String stateString,
          String taskTracker, Phase phase, Counters counters) {
    super(taskid, progress, numSlots, runState, diagnosticInfo, stateString,
          taskTracker, phase, counters);
  }

  @Override
  public boolean getIsMap() {
    return true;
  }

  @Override
  public long getShuffleFinishTime() {
    throw new UnsupportedOperationException("getShuffleFinishTime() not supported for MapTask");
  }

  @Override
  void setShuffleFinishTime(long shuffleFinishTime) {
    throw new UnsupportedOperationException("setShuffleFinishTime() not supported for MapTask");
  }

  @Override
  public long getSortFinishTime() {
    throw new UnsupportedOperationException("getSortFinishTime() not supported for MapTask");
  }

  @Override
  void setSortFinishTime(long sortFinishTime) {
    throw new UnsupportedOperationException("setSortFinishTime() not supported for MapTask");
  }
}
