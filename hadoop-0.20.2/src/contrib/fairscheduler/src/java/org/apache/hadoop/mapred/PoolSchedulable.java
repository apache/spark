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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapreduce.TaskType;

public class PoolSchedulable extends Schedulable {
  public static final Log LOG = LogFactory.getLog(
      PoolSchedulable.class.getName());
  
  private FairScheduler scheduler;
  private Pool pool;
  private TaskType taskType;
  private PoolManager poolMgr;
  private List<JobSchedulable> jobScheds = new LinkedList<JobSchedulable>();
  private int demand = 0;

  // Variables used for preemption
  long lastTimeAtMinShare;
  long lastTimeAtHalfFairShare;
  
  public PoolSchedulable(FairScheduler scheduler, Pool pool, TaskType type) {
    this.scheduler = scheduler;
    this.pool = pool;
    this.taskType = type;
    this.poolMgr = scheduler.getPoolManager();
    long currentTime = scheduler.getClock().getTime();
    this.lastTimeAtMinShare = currentTime;
    this.lastTimeAtHalfFairShare = currentTime;
    
    initMetrics();
  }

  public void addJob(JobInProgress job) {
    JobInfo info = scheduler.getJobInfo(job);
    jobScheds.add(taskType == TaskType.MAP ?
        info.mapSchedulable : info.reduceSchedulable);
  }
  
  public void removeJob(JobInProgress job) {
    for (Iterator<JobSchedulable> it = jobScheds.iterator(); it.hasNext();) {
      JobSchedulable jobSched = it.next();
      if (jobSched.getJob() == job) {
        it.remove();
        break;
      }
    }
  }

  /**
   * Update demand by asking jobs in the pool to update
   */
  @Override
  public void updateDemand() {
    demand = 0;
    for (JobSchedulable sched: jobScheds) {
      sched.updateDemand();
      demand += sched.getDemand();
    }
    // if demand exceeds the cap for this pool, limit to the max
    int maxTasks = poolMgr.getMaxSlots(pool.getName(), taskType);
    if(demand > maxTasks) {
      demand = maxTasks;
    }
  }
  
  /**
   * Distribute the pool's fair share among its jobs
   */
  @Override
  public void redistributeShare() {
    if (pool.getSchedulingMode() == SchedulingMode.FAIR) {
      SchedulingAlgorithms.computeFairShares(jobScheds, getFairShare());
    } else {
      for (JobSchedulable sched: jobScheds) {
        sched.setFairShare(0);
      }
    } 
  }

  @Override
  public int getDemand() {
    return demand;
  }

  @Override
  public int getMinShare() {
    return poolMgr.getAllocation(pool.getName(), taskType);
  }

  @Override
  public double getWeight() {
    return poolMgr.getPoolWeight(pool.getName());
  }

  @Override
  public JobPriority getPriority() {
    return JobPriority.NORMAL;
  }

  @Override
  public int getRunningTasks() {
    int ans = 0;
    for (JobSchedulable sched: jobScheds) {
      ans += sched.getRunningTasks();
    }
    return ans;
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Task assignTask(TaskTrackerStatus tts, long currentTime,
      Collection<JobInProgress> visited) throws IOException {
    int runningTasks = getRunningTasks();
    if (runningTasks >= poolMgr.getMaxSlots(pool.getName(), taskType)) {
      return null;
    }
    SchedulingMode mode = pool.getSchedulingMode();
    Comparator<Schedulable> comparator;
    if (mode == SchedulingMode.FIFO) {
      comparator = new SchedulingAlgorithms.FifoComparator();
    } else if (mode == SchedulingMode.FAIR) {
      comparator = new SchedulingAlgorithms.FairShareComparator();
    } else {
      throw new RuntimeException("Unsupported pool scheduling mode " + mode);
    }
    Collections.sort(jobScheds, comparator);
    for (JobSchedulable sched: jobScheds) {
      Task task = sched.assignTask(tts, currentTime, visited);
      if (task != null)
        return task;
    }
    return null;
  }
  
  @Override
  public String getName() {
    return pool.getName();
  }

  Pool getPool() {
    return pool;
  }

  @Override
  public TaskType getTaskType() {
    return taskType;
  }
  
  public Collection<JobSchedulable> getJobSchedulables() {
    return jobScheds;
  }
  
  public long getLastTimeAtMinShare() {
    return lastTimeAtMinShare;
  }
  
  public void setLastTimeAtMinShare(long lastTimeAtMinShare) {
    this.lastTimeAtMinShare = lastTimeAtMinShare;
  }
  
  public long getLastTimeAtHalfFairShare() {
    return lastTimeAtHalfFairShare;
  }
  
  public void setLastTimeAtHalfFairShare(long lastTimeAtHalfFairShare) {
    this.lastTimeAtHalfFairShare = lastTimeAtHalfFairShare;
  }

  protected String getMetricsContextName() {
    return "pools";
  }
  
  @Override
  public void updateMetrics() {
    super.setMetricValues(metrics);
    
    if (scheduler.isPreemptionEnabled()) {
      // These won't be set if preemption is off
      long lastCheck = scheduler.getLastPreemptionUpdateTime();
      metrics.setMetric("millisSinceAtMinShare", lastCheck - lastTimeAtMinShare);
      metrics.setMetric("millisSinceAtHalfFairShare", lastCheck - lastTimeAtHalfFairShare);
    }
    metrics.update();

    for (JobSchedulable job : jobScheds) {
      job.updateMetrics();
    }
  }
}
