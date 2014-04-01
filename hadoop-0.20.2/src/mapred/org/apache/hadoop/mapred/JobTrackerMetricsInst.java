/*
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

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

class JobTrackerMetricsInst extends JobTrackerInstrumentation implements Updater {
  private final MetricsRecord metricsRecord;

  private int numMapTasksLaunched = 0;
  private int numMapTasksCompleted = 0;
  private int numMapTasksFailed = 0;
  private int numReduceTasksLaunched = 0;
  private int numReduceTasksCompleted = 0;
  private int numReduceTasksFailed = 0;
  private int numJobsSubmitted = 0;
  private int numJobsCompleted = 0;
  private int numWaitingMaps = 0;
  private int numWaitingReduces = 0;

  //Cluster status fields.
  private volatile int numMapSlots = 0;
  private volatile int numReduceSlots = 0;
  private int numBlackListedMapSlots = 0;
  private int numBlackListedReduceSlots = 0;

  private int numReservedMapSlots = 0;
  private int numReservedReduceSlots = 0;
  private int numOccupiedMapSlots = 0;
  private int numOccupiedReduceSlots = 0;
  
  private int numJobsFailed = 0;
  private int numJobsKilled = 0;
  
  private int numJobsPreparing = 0;
  private int numJobsRunning = 0;
  
  private int numRunningMaps = 0;
  private int numRunningReduces = 0;
  
  private int numMapTasksKilled = 0;
  private int numReduceTasksKilled = 0;

  private int numTrackers = 0;
  private int numTrackersBlackListed = 0;
  private int numTrackersDecommissioned = 0;

  // long, because 2^31 could well be only about a month's worth of
  // heartbeats, with reasonable assumptions and JobTracker improvements.
  private long numHeartbeats = 0L;
  
  public JobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
    super(tracker, conf);
    String sessionId = conf.getSessionId();
    // Initiate JVM Metrics
    JvmMetrics.init("JobTracker", sessionId);
    // Create a record for map-reduce metrics
    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "jobtracker");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }
    
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      metricsRecord.setMetric("map_slots", numMapSlots);
      metricsRecord.setMetric("reduce_slots", numReduceSlots);
      metricsRecord.incrMetric("blacklisted_maps", numBlackListedMapSlots);
      metricsRecord.incrMetric("blacklisted_reduces",
          numBlackListedReduceSlots);
      metricsRecord.incrMetric("maps_launched", numMapTasksLaunched);
      metricsRecord.incrMetric("maps_completed", numMapTasksCompleted);
      metricsRecord.incrMetric("maps_failed", numMapTasksFailed);
      metricsRecord.incrMetric("reduces_launched", numReduceTasksLaunched);
      metricsRecord.incrMetric("reduces_completed", numReduceTasksCompleted);
      metricsRecord.incrMetric("reduces_failed", numReduceTasksFailed);
      metricsRecord.incrMetric("jobs_submitted", numJobsSubmitted);
      metricsRecord.incrMetric("jobs_completed", numJobsCompleted);
      metricsRecord.incrMetric("waiting_maps", numWaitingMaps);
      metricsRecord.incrMetric("waiting_reduces", numWaitingReduces);
      
      metricsRecord.incrMetric("reserved_map_slots", numReservedMapSlots);
      metricsRecord.incrMetric("reserved_reduce_slots", numReservedReduceSlots);
      metricsRecord.incrMetric("occupied_map_slots", numOccupiedMapSlots);
      metricsRecord.incrMetric("occupied_reduce_slots", numOccupiedReduceSlots);
      
      metricsRecord.incrMetric("jobs_failed", numJobsFailed);
      metricsRecord.incrMetric("jobs_killed", numJobsKilled);
      
      metricsRecord.incrMetric("jobs_preparing", numJobsPreparing);
      metricsRecord.incrMetric("jobs_running", numJobsRunning);
      
      metricsRecord.incrMetric("running_maps", numRunningMaps);
      metricsRecord.incrMetric("running_reduces", numRunningReduces);
      
      metricsRecord.incrMetric("maps_killed", numMapTasksKilled);
      metricsRecord.incrMetric("reduces_killed", numReduceTasksKilled);

      metricsRecord.incrMetric("trackers", numTrackers);
      metricsRecord.incrMetric("trackers_blacklisted", numTrackersBlackListed);
      metricsRecord.setMetric("trackers_decommissioned", 
          numTrackersDecommissioned);

      metricsRecord.incrMetric("heartbeats", numHeartbeats);

      numMapTasksLaunched = 0;
      numMapTasksCompleted = 0;
      numMapTasksFailed = 0;
      numReduceTasksLaunched = 0;
      numReduceTasksCompleted = 0;
      numReduceTasksFailed = 0;
      numJobsSubmitted = 0;
      numJobsCompleted = 0;
      numWaitingMaps = 0;
      numWaitingReduces = 0;
      numBlackListedMapSlots = 0;
      numBlackListedReduceSlots = 0;
      
      numReservedMapSlots = 0;
      numReservedReduceSlots = 0;
      numOccupiedMapSlots = 0;
      numOccupiedReduceSlots = 0;
      
      numJobsFailed = 0;
      numJobsKilled = 0;
      
      numJobsPreparing = 0;
      numJobsRunning = 0;
      
      numRunningMaps = 0;
      numRunningReduces = 0;
      
      numMapTasksKilled = 0;
      numReduceTasksKilled = 0;

      numTrackers = 0;
      numTrackersBlackListed = 0;

      numHeartbeats = 0L;
    }
    metricsRecord.update();
  }

  @Override
  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksLaunched;
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void completeMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksCompleted;
  }

  @Override
  public synchronized void failedMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksFailed;
    addWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksLaunched;
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksCompleted;
  }

  @Override
  public synchronized void failedReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksFailed;
    addWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void submitJob(JobConf conf, JobID id) {
    ++numJobsSubmitted;
  }

  @Override
  public synchronized void completeJob(JobConf conf, JobID id) {
    ++numJobsCompleted;
  }

  @Override
  public synchronized void addWaitingMaps(JobID id, int task) {
    numWaitingMaps  += task;
  }
  
  @Override
  public synchronized void decWaitingMaps(JobID id, int task) {
    numWaitingMaps -= task;
  }
  
  @Override
  public synchronized void addWaitingReduces(JobID id, int task) {
    numWaitingReduces += task;
  }
  
  @Override
  public synchronized void decWaitingReduces(JobID id, int task){
    numWaitingReduces -= task;
  }

  @Override
  public synchronized void setMapSlots(int slots) {
    numMapSlots = slots;
  }

  @Override
  public synchronized void setReduceSlots(int slots) {
    numReduceSlots = slots;
  }

  @Override
  public synchronized void addBlackListedMapSlots(int slots){
    numBlackListedMapSlots += slots;
  }

  @Override
  public synchronized void decBlackListedMapSlots(int slots){
    numBlackListedMapSlots -= slots;
  }

  @Override
  public synchronized void addBlackListedReduceSlots(int slots){
    numBlackListedReduceSlots += slots;
  }

  @Override
  public synchronized void decBlackListedReduceSlots(int slots){
    numBlackListedReduceSlots -= slots;
  }

  @Override
  public synchronized void addReservedMapSlots(int slots)
  { 
    numReservedMapSlots += slots;
  }

  @Override
  public synchronized void decReservedMapSlots(int slots)
  {
    numReservedMapSlots -= slots;
  }

  @Override
  public synchronized void addReservedReduceSlots(int slots)
  {
    numReservedReduceSlots += slots;
  }

  @Override
  public synchronized void decReservedReduceSlots(int slots)
  {
    numReservedReduceSlots -= slots;
  }

  @Override
  public synchronized void addOccupiedMapSlots(int slots)
  {
    numOccupiedMapSlots += slots;
  }

  @Override
  public synchronized void decOccupiedMapSlots(int slots)
  {
    numOccupiedMapSlots -= slots;
  }

  @Override
  public synchronized void addOccupiedReduceSlots(int slots)
  {
    numOccupiedReduceSlots += slots;
  }

  @Override
  public synchronized void decOccupiedReduceSlots(int slots)
  {
    numOccupiedReduceSlots -= slots;
  }

  @Override
  public synchronized void failedJob(JobConf conf, JobID id) 
  {
    numJobsFailed++;
  }

  @Override
  public synchronized void killedJob(JobConf conf, JobID id) 
  {
    numJobsKilled++;
  }

  @Override
  public synchronized void addPrepJob(JobConf conf, JobID id) 
  {
    numJobsPreparing++;
  }

  @Override
  public synchronized void decPrepJob(JobConf conf, JobID id) 
  {
    numJobsPreparing--;
  }

  @Override
  public synchronized void addRunningJob(JobConf conf, JobID id) 
  {
    numJobsRunning++;
  }

  @Override
  public synchronized void decRunningJob(JobConf conf, JobID id) 
  {
    numJobsRunning--;
  }

  @Override
  public synchronized void addRunningMaps(int task)
  {
    numRunningMaps += task;
  }

  @Override
  public synchronized void decRunningMaps(int task) 
  {
    numRunningMaps -= task;
  }

  @Override
  public synchronized void addRunningReduces(int task)
  {
    numRunningReduces += task;
  }

  @Override
  public synchronized void decRunningReduces(int task)
  {
    numRunningReduces -= task;
  }

  @Override
  public synchronized void killedMap(TaskAttemptID taskAttemptID)
  {
    numMapTasksKilled++;
  }

  @Override
  public synchronized void killedReduce(TaskAttemptID taskAttemptID)
  {
    numReduceTasksKilled++;
  }

  @Override
  public synchronized void addTrackers(int trackers)
  {
    numTrackers += trackers;
  }

  @Override
  public synchronized void decTrackers(int trackers)
  {
    numTrackers -= trackers;
  }

  @Override
  public synchronized void addBlackListedTrackers(int trackers)
  {
    numTrackersBlackListed += trackers;
  }

  @Override
  public synchronized void decBlackListedTrackers(int trackers)
  {
    numTrackersBlackListed -= trackers;
  }

  @Override
  public synchronized void setDecommissionedTrackers(int trackers)
  {
    numTrackersDecommissioned = trackers;
  }  

  @Override
  public synchronized void heartbeat() {
    ++numHeartbeats;
  }
}
