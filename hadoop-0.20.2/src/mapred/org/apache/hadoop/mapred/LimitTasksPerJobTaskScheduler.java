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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that limits the maximum number of tasks
 * running for a job. The limit is set by means of the
 * <code>mapred.jobtracker.scheduler.maxRunningTasksPerJob</code>
 * property.
 */
class LimitTasksPerJobTaskScheduler extends JobQueueTaskScheduler {
  
  private static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.mapred.TaskLimitedJobQueueTaskScheduler");
  
  public static final String MAX_TASKS_PER_JOB_PROPERTY = 
    "mapred.jobtracker.taskScheduler.maxRunningTasksPerJob";
  
  private long maxTasksPerJob;
  
  public LimitTasksPerJobTaskScheduler() {
    super();
  }
  
  @Override
  public synchronized void start() throws IOException {
    super.start();
    QueueManager queueManager = taskTrackerManager.getQueueManager();
    String queueName = queueManager.getJobQueueInfos()[0].getQueueName();
    queueManager.setSchedulerInfo(queueName
        ,"Maximum Tasks Per Job :: " + String.valueOf(maxTasksPerJob));
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    maxTasksPerJob = conf.getLong(MAX_TASKS_PER_JOB_PROPERTY ,Long.MAX_VALUE);
    if (maxTasksPerJob <= 0) {
      String msg = MAX_TASKS_PER_JOB_PROPERTY +
        " is set to zero or a negative value. Aborting.";
      LOG.fatal(msg);
      throw new RuntimeException (msg);
    }
  }

  @Override
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus();
    final int numTaskTrackers =
        taskTrackerManager.getClusterStatus().getTaskTrackers();
    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();
    Task task;

    /* Stats about the current taskTracker */
    final int mapTasksNumber = taskTrackerStatus.countMapTasks();
    final int reduceTasksNumber = taskTrackerStatus.countReduceTasks();
    final int maximumMapTasksNumber = taskTrackerStatus.getMaxMapSlots();
    final int maximumReduceTasksNumber = taskTrackerStatus.getMaxReduceSlots();

    /*
     * Statistics about the whole cluster. Most are approximate because of
     * concurrency
     */
    final int[] maxMapAndReduceLoad = getMaxMapAndReduceLoad(
        maximumMapTasksNumber, maximumReduceTasksNumber);
    final int maximumMapLoad = maxMapAndReduceLoad[0];
    final int maximumReduceLoad = maxMapAndReduceLoad[1];

    
    final int beginAtStep;
    /*
     * When step == 0, this loop starts as many map tasks it can wrt
     * maxTasksPerJob
     * When step == 1, this loop starts as many reduce tasks it can wrt
     * maxTasksPerJob
     * When step == 2, this loop starts as many map tasks it can
     * When step == 3, this loop starts as many reduce tasks it can
     *
     * It may seem that we would improve this loop by queuing jobs we cannot
     * start in steps 0 and 1 because of maxTasksPerJob, and using that queue
     * in step 2 and 3.
     * A first thing to notice is that the time with the current algorithm is
     * logarithmic, because it is the sum of (p^k) for k from 1 to N, were
     * N is the number of jobs and p is the probability for a job to not exceed
     * limits The probability for the cache to be useful would be similar to
     * p^N, that is 1/(e^N), whereas its size and the time spent to manage it
     * would be in ln(N).
     * So it is not a good idea.
     */
    if (maxTasksPerJob != Long.MAX_VALUE) {
      beginAtStep = 0;
    }
    else {
      beginAtStep = 2;
    }
    List<Task> assignedTasks = new ArrayList<Task>();
    scheduleTasks:
    for (int step = beginAtStep; step <= 3; ++step) {
      /* If we reached the maximum load for this step, go to the next */
      if ((step == 0 || step == 2) && mapTasksNumber >= maximumMapLoad ||
          (step == 1 || step == 3) && reduceTasksNumber >= maximumReduceLoad) {
        continue;
      }
      /* For each job, start its tasks */
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          /* Ignore non running jobs */
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }
          /* Check that we're not exceeding the global limits */
          if ((step == 0 || step == 1)
              && (job.runningMaps() + job.runningReduces() >= maxTasksPerJob)) {
            continue;
          }
          if (step == 0 || step == 2) {
            task = job.obtainNewMapTask(taskTrackerStatus, numTaskTrackers,
                taskTrackerManager.getNumberOfUniqueHosts());
          }
          else {
            task = job.obtainNewReduceTask(taskTrackerStatus, numTaskTrackers,
                taskTrackerManager.getNumberOfUniqueHosts());
          }
          if (task != null) {
            assignedTasks.add(task);
            break scheduleTasks;
          }
        }
      }
    }
    return assignedTasks;
  }

  /**
   * Determine the maximum number of maps or reduces that we are willing to run
   * on a taskTracker which accept a maximum of localMaxMapLoad maps and
   * localMaxReduceLoad reduces
   * @param localMaxMapLoad The local maximum number of map tasks for a host
   * @param localMaxReduceLoad The local maximum number of reduce tasks for a
   * host
   * @return An array of the two maximums: map then reduce.
   */
  protected synchronized int[] getMaxMapAndReduceLoad(int localMaxMapLoad,
      int localMaxReduceLoad) {
    // Approximate because of concurrency
    final int numTaskTrackers =
      taskTrackerManager.getClusterStatus().getTaskTrackers();
    /* Hold the result */
    int maxMapLoad = 0;
    int maxReduceLoad = 0;
    int neededMaps = 0;
    int neededReduces = 0;
    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();
    synchronized (jobQueue) {
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          neededMaps += job.desiredMaps() - job.finishedMaps();
          neededReduces += job.desiredReduces() - job.finishedReduces();
        }
      }
    }
    if (numTaskTrackers > 0) {
      maxMapLoad = Math.min(localMaxMapLoad, (int) Math
          .ceil((double) neededMaps / numTaskTrackers));
      maxReduceLoad = Math.min(localMaxReduceLoad, (int) Math
          .ceil((double) neededReduces / numTaskTrackers));
    }
    return new int[] { maxMapLoad, maxReduceLoad };
  }

}
