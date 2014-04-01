/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.gridmix.Statistics.ClusterStats;
import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;

public class StressJobFactory extends JobFactory<Statistics.ClusterStats> {
  public static final Log LOG = LogFactory.getLog(StressJobFactory.class);

  private final LoadStatus loadStatus = new LoadStatus();
  private final Condition condUnderloaded = this.lock.newCondition();
  /**
   * The minimum ratio between pending+running map tasks (aka. incomplete map
   * tasks) and cluster map slot capacity for us to consider the cluster is
   * overloaded. For running maps, we only count them partially. Namely, a 40%
   * completed map is counted as 0.6 map tasks in our calculation.
   */
  private static final float OVERLOAD_MAPTASK_MAPSLOT_RATIO = 2.0f;
  public static final String CONF_OVERLOAD_MAPTASK_MAPSLOT_RATIO=
      "gridmix.throttle.maps.task-to-slot-ratio";
  final float overloadMapTaskMapSlotRatio;

  /**
   * The minimum ratio between pending+running reduce tasks (aka. incomplete
   * reduce tasks) and cluster reduce slot capacity for us to consider the
   * cluster is overloaded. For running reduces, we only count them partially.
   * Namely, a 40% completed reduce is counted as 0.6 reduce tasks in our
   * calculation.
   */
  private static final float OVERLOAD_REDUCETASK_REDUCESLOT_RATIO = 2.5f;
  public static final String CONF_OVERLOAD_REDUCETASK_REDUCESLOT_RATIO=
    "gridmix.throttle.reduces.task-to-slot-ratio";
  final float overloadReduceTaskReduceSlotRatio;

  /**
   * The maximum share of the cluster's mapslot capacity that can be counted
   * toward a job's incomplete map tasks in overload calculation.
   */
  private static final float MAX_MAPSLOT_SHARE_PER_JOB=0.1f;
  public static final String CONF_MAX_MAPSLOT_SHARE_PER_JOB=
    "gridmix.throttle.maps.max-slot-share-per-job";  
  final float maxMapSlotSharePerJob;
  
  /**
   * The maximum share of the cluster's reduceslot capacity that can be counted
   * toward a job's incomplete reduce tasks in overload calculation.
   */
  private static final float MAX_REDUCESLOT_SHARE_PER_JOB=0.1f;
  public static final String CONF_MAX_REDUCESLOT_SHARE_PER_JOB=
    "gridmix.throttle.reducess.max-slot-share-per-job";  
  final float maxReduceSlotSharePerJob;

  /**
   * The ratio of the maximum number of pending+running jobs over the number of
   * task trackers.
   */
  private static final float MAX_JOB_TRACKER_RATIO=1.0f;
  public static final String CONF_MAX_JOB_TRACKER_RATIO=
    "gridmix.throttle.jobs-to-tracker-ratio";  
  final float maxJobTrackerRatio;

  /**
   * Creating a new instance does not start the thread.
   *
   * @param submitter   Component to which deserialized jobs are passed
   * @param jobProducer Stream of job traces with which to construct a
   *                    {@link org.apache.hadoop.tools.rumen.ZombieJobProducer}
   * @param scratch     Directory into which to write output from simulated jobs
   * @param conf        Config passed to all jobs to be submitted
   * @param startFlag   Latch released from main to start pipeline
   * @throws java.io.IOException
   */
  public StressJobFactory(
    JobSubmitter submitter, JobStoryProducer jobProducer, Path scratch,
    Configuration conf, CountDownLatch startFlag, UserResolver resolver)
    throws IOException {
    super(
      submitter, jobProducer, scratch, conf, startFlag, resolver);
    overloadMapTaskMapSlotRatio = conf.getFloat(
        CONF_OVERLOAD_MAPTASK_MAPSLOT_RATIO, OVERLOAD_MAPTASK_MAPSLOT_RATIO);
    overloadReduceTaskReduceSlotRatio = conf.getFloat(
        CONF_OVERLOAD_REDUCETASK_REDUCESLOT_RATIO, 
        OVERLOAD_REDUCETASK_REDUCESLOT_RATIO);
    maxMapSlotSharePerJob = conf.getFloat(
        CONF_MAX_MAPSLOT_SHARE_PER_JOB, MAX_MAPSLOT_SHARE_PER_JOB);
    maxReduceSlotSharePerJob = conf.getFloat(
        CONF_MAX_REDUCESLOT_SHARE_PER_JOB, MAX_REDUCESLOT_SHARE_PER_JOB);
    maxJobTrackerRatio = conf.getFloat(
        CONF_MAX_JOB_TRACKER_RATIO, MAX_JOB_TRACKER_RATIO);
  }

  public Thread createReaderThread() {
    return new StressReaderThread("StressJobFactory");
  }

  /*
  * Worker thread responsible for reading descriptions, assigning sequence
  * numbers, and normalizing time.
  */
  private class StressReaderThread extends Thread {

    public StressReaderThread(String name) {
      super(name);
    }

    /**
     * STRESS: Submits the job in STRESS mode.
     * while(JT is overloaded) {
     * wait();
     * }
     * If not overloaded , get number of slots available.
     * Keep submitting the jobs till ,total jobs  is sufficient to
     * load the JT.
     * That is submit  (Sigma(no of maps/Job)) > (2 * no of slots available)
     */
    public void run() {
      try {
        startFlag.await();
        if (Thread.currentThread().isInterrupted()) {
          return;
        }
        LOG.info("START STRESS @ " + System.currentTimeMillis());
        while (!Thread.currentThread().isInterrupted()) {
          lock.lock();
          try {
            while (loadStatus.overloaded()) {
              //Wait while JT is overloaded.
              try {
                condUnderloaded.await();
              } catch (InterruptedException ie) {
                return;
              }
            }

            while (!loadStatus.overloaded()) {
              try {
                final JobStory job = getNextJobFiltered();
                if (null == job) {
                  return;
                }
                
                submitter.add(
                  jobCreator.createGridmixJob(
                    conf, 0L, job, scratch, userResolver.getTargetUgi(
                      UserGroupInformation.createRemoteUser(
                        job.getUser())), sequence.getAndIncrement()));
                // TODO: We need to take care of scenario when one map/reduce
                // takes more than 1 slot.
                loadStatus.mapSlotsBackfill -= 
                  calcEffectiveIncompleteMapTasks(
                    loadStatus.mapSlotCapacity, job.getNumberMaps(), 0.0f);
                loadStatus.reduceSlotsBackfill -= 
                  calcEffectiveIncompleteReduceTasks(
                    loadStatus.reduceSlotCapacity, job.getNumberReduces(), 
                    0.0f);
                --loadStatus.numJobsBackfill;
              } catch (IOException e) {
                LOG.error("Error while submitting the job ", e);
                error = e;
                return;
              }

            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException e) {
        return;
      } finally {
        IOUtils.cleanup(null, jobProducer);
      }
    }
  }

  /**
   * STRESS Once you get the notification from StatsCollector.Collect the
   * clustermetrics. Update current loadStatus with new load status of JT.
   *
   * @param item
   */
  @Override
  public void update(Statistics.ClusterStats item) {
    lock.lock();
    try {
      ClusterStatus clusterMetrics = item.getStatus();
      try {
        checkLoadAndGetSlotsToBackfill(item,clusterMetrics);
      } catch (IOException e) {
        LOG.error("Couldn't get the new Status",e);
      }
      if (!loadStatus.overloaded()) {
        condUnderloaded.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  float calcEffectiveIncompleteMapTasks(int mapSlotCapacity,
      int numMaps, float mapProgress) {
    float maxEffIncompleteMapTasks = Math.max(1.0f, mapSlotCapacity
        * maxMapSlotSharePerJob);
    float mapProgressAdjusted = Math.max(Math.min(mapProgress, 1.0f), 0.0f);
    return Math.min(maxEffIncompleteMapTasks, numMaps
        * (1.0f - mapProgressAdjusted));
  }

  float calcEffectiveIncompleteReduceTasks(int reduceSlotCapacity,
      int numReduces, float reduceProgress) {
    float maxEffIncompleteReduceTasks = Math.max(1.0f, reduceSlotCapacity
        * maxReduceSlotSharePerJob);
    float reduceProgressAdjusted = Math.max(Math.min(reduceProgress, 1.0f),
        0.0f);
    return Math.min(maxEffIncompleteReduceTasks, numReduces
        * (1.0f - reduceProgressAdjusted));
  }

  /**
   * We try to use some light-weight mechanism to determine cluster load.
   *
   * @param stats
   * @param clusterStatus Cluster status
   * @throws java.io.IOException
   */
  private void checkLoadAndGetSlotsToBackfill(
    ClusterStats stats, ClusterStatus clusterStatus) throws IOException {
    loadStatus.mapSlotCapacity = clusterStatus.getMaxMapTasks();
    loadStatus.reduceSlotCapacity = clusterStatus.getMaxReduceTasks();
    
    
    loadStatus.numJobsBackfill = 
      (int)(maxJobTrackerRatio*clusterStatus.getTaskTrackers())
        - stats.getNumRunningJob();
    if (loadStatus.numJobsBackfill <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(System.currentTimeMillis() + " Overloaded is "
            + Boolean.TRUE.toString() + " NumJobsBackfill is "
            + loadStatus.numJobsBackfill);
      }
      return; // stop calculation because we know it is overloaded.
    }

    float incompleteMapTasks = 0; // include pending & running map tasks.
    for (JobStats job : ClusterStats.getRunningJobStats()) {
      float mapProgress = job.getJob().mapProgress();
      int noOfMaps = job.getNoOfMaps();
      incompleteMapTasks += calcEffectiveIncompleteMapTasks(clusterStatus
          .getMaxMapTasks(), noOfMaps, mapProgress);
    }
    loadStatus.mapSlotsBackfill = (int) (overloadMapTaskMapSlotRatio
        * clusterStatus.getMaxMapTasks() - incompleteMapTasks);
    if (loadStatus.mapSlotsBackfill <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(System.currentTimeMillis() + " Overloaded is "
            + Boolean.TRUE.toString() + " MapSlotsBackfill is "
            + loadStatus.mapSlotsBackfill);
      }
      return; // stop calculation because we know it is overloaded.
    }

    float incompleteReduceTasks = 0; // include pending & running reduce tasks.
    for (JobStats job : ClusterStats.getRunningJobStats()) {
      int noOfReduces = job.getJob().getNumReduceTasks();
      if (noOfReduces > 0) {
        float reduceProgress = job.getJob().reduceProgress();
        incompleteReduceTasks += calcEffectiveIncompleteReduceTasks(
            clusterStatus.getMaxReduceTasks(), noOfReduces, reduceProgress);
      }
    }
    loadStatus.reduceSlotsBackfill = (int) (overloadReduceTaskReduceSlotRatio
        * clusterStatus.getMaxReduceTasks() - incompleteReduceTasks);
    if (loadStatus.reduceSlotsBackfill <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(System.currentTimeMillis() + " Overloaded is "
            + Boolean.TRUE.toString() + " ReduceSlotsBackfill is "
            + loadStatus.reduceSlotsBackfill);
      }
      return; // stop calculation because we know it is overloaded.
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(System.currentTimeMillis() + " Overloaded is "
          + Boolean.FALSE.toString() + "Current load Status is " + loadStatus);
    }
  }

  static class LoadStatus {
    int mapSlotsBackfill;
    int mapSlotCapacity;
    int reduceSlotsBackfill;
    int reduceSlotCapacity;
    int numJobsBackfill;

    /**
     * Construct the LoadStatus in an unknown state - assuming the cluster is
     * overloaded by setting numSlotsBackfill=0.
     */
    LoadStatus() {
      mapSlotsBackfill = 0;
      reduceSlotsBackfill = 0;
      numJobsBackfill = 0;
      
      mapSlotCapacity = -1;
      reduceSlotCapacity = -1;
    }
    
    public boolean overloaded() {
      return (mapSlotsBackfill <= 0) || (reduceSlotsBackfill <= 0)
          || (numJobsBackfill <= 0);
    }
    
    public String toString() {
      return " Overloaded = " + overloaded()
          + ", MapSlotBackfill = " + mapSlotsBackfill 
          + ", MapSlotCapacity = " + mapSlotCapacity
          + ", ReduceSlotBackfill = " + reduceSlotsBackfill 
          + ", ReduceSlotCapacity = " + reduceSlotCapacity
          + ", NumJobsBackfill = " + numJobsBackfill
          ;
    }
  }

  /**
   * Start the reader thread, wait for latch if necessary.
   */
  @Override
  public void start() {
    LOG.info(" Starting Stress submission ");
    this.rThread.start();
  }

}
