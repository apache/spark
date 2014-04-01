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

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.mapreduce.TaskType;

/**
 * Dummy implementation of Schedulable for unit testing.
 */
public class FakeSchedulable extends Schedulable {
  private int demand;
  private int runningTasks;
  private int minShare;
  private double weight;
  private JobPriority priority;
  private long startTime;
  
  public FakeSchedulable() {
    this(0, 0, 1, 0, 0, JobPriority.NORMAL, 0);
  }
  
  public FakeSchedulable(int demand) {
    this(demand, 0, 1, 0, 0, JobPriority.NORMAL, 0);
  }
  
  public FakeSchedulable(int demand, int minShare) {
    this(demand, minShare, 1, 0, 0, JobPriority.NORMAL, 0);
  }
  
  public FakeSchedulable(int demand, int minShare, double weight) {
    this(demand, minShare, weight, 0, 0, JobPriority.NORMAL, 0);
  }
  
  public FakeSchedulable(int demand, int minShare, double weight, int fairShare,
      int runningTasks, JobPriority priority, long startTime) {
    this.demand = demand;
    this.minShare = minShare;
    this.weight = weight;
    setFairShare(fairShare);
    this.runningTasks = runningTasks;
    this.priority = priority;
    this.startTime = startTime;
  }
  
  @Override
  public Task assignTask(TaskTrackerStatus tts, long currentTime,
      Collection<JobInProgress> visited) throws IOException {
    return null;
  }

  @Override
  public int getDemand() {
    return demand;
  }

  @Override
  public String getName() {
    return "FakeSchedulable" + this.hashCode();
  }

  @Override
  public JobPriority getPriority() {
    return priority;
  }

  @Override
  public int getRunningTasks() {
    return runningTasks;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }
  
  @Override
  public double getWeight() {
    return weight;
  }
  
  @Override
  public int getMinShare() {
    return minShare;
  }

  @Override
  public void redistributeShare() {}

  @Override
  public void updateDemand() {}

  @Override
  public TaskType getTaskType() {
    return TaskType.MAP;
  }

  @Override
  protected String getMetricsContextName() {
    return "fake";
  }

  @Override
  void updateMetrics() {
  }
}
