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
  
class TaskTrackerMetricsInst extends TaskTrackerInstrumentation 
                             implements Updater {
  private final MetricsRecord metricsRecord;
  private int numCompletedTasks = 0;
  private int timedoutTasks = 0;
  private int tasksFailedPing = 0;
    
  public TaskTrackerMetricsInst(TaskTracker t) {
    super(t);
    JobConf conf = tt.getJobConf();
    String sessionId = conf.getSessionId();
    // Initiate Java VM Metrics
    JvmMetrics.init("TaskTracker", sessionId);
    // Create a record for Task Tracker metrics
    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "tasktracker"); //guaranteed never null
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }

  @Override
  public synchronized void completeTask(TaskAttemptID t) {
    ++numCompletedTasks;
  }

  @Override
  public synchronized void timedoutTask(TaskAttemptID t) {
    ++timedoutTasks;
  }

  @Override
  public synchronized void taskFailedPing(TaskAttemptID t) {
    ++tasksFailedPing;
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  @Override
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      metricsRecord.setMetric("maps_running", tt.mapTotal);
      metricsRecord.setMetric("reduces_running", tt.reduceTotal);
      metricsRecord.setMetric("mapTaskSlots", (short)tt.getMaxCurrentMapTasks());
      metricsRecord.setMetric("reduceTaskSlots", 
                                   (short)tt.getMaxCurrentReduceTasks());
      metricsRecord.setMetric("failedDirs", tt.getNumDirFailures());
      metricsRecord.incrMetric("tasks_completed", numCompletedTasks);
      metricsRecord.incrMetric("tasks_failed_timeout", timedoutTasks);
      metricsRecord.incrMetric("tasks_failed_ping", tasksFailedPing);
      
      numCompletedTasks = 0;
      timedoutTasks = 0;
      tasksFailedPing = 0;
    }
      metricsRecord.update();
  }

  
}
