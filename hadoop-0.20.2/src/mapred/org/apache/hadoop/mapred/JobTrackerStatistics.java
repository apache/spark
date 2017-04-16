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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.StatisticsCollector.Stat;

/**
 * Collects the job tracker statistics.
 *
 */
class JobTrackerStatistics {

  final StatisticsCollector collector;
  final Map<String, TaskTrackerStat> ttStats = 
    new HashMap<String, TaskTrackerStat>();

  JobTrackerStatistics() {
    collector = new StatisticsCollector();
    collector.start();
  }

  synchronized void taskTrackerAdded(String name) {
    TaskTrackerStat stat = ttStats.get(name);
    if(stat == null) {
      stat =  new TaskTrackerStat(name);
      ttStats.put(name, stat);
    }
  }

  synchronized void taskTrackerRemoved(String name) {
    TaskTrackerStat stat = ttStats.remove(name);
    if(stat != null) {
      stat.remove();
    }
  }

  synchronized TaskTrackerStat getTaskTrackerStat(String name) {
    return ttStats.get(name);
  }

  class TaskTrackerStat {
    final String totalTasksKey;
    final Stat totalTasksStat;

    final String succeededTasksKey;
    final Stat succeededTasksStat;

    TaskTrackerStat(String trackerName) {
      totalTasksKey = trackerName+"-"+"totalTasks";
      totalTasksStat = collector.createStat(totalTasksKey);
      succeededTasksKey = trackerName+"-"+"succeededTasks";
      succeededTasksStat = collector.createStat(succeededTasksKey);
    }

    synchronized void incrTotalTasks() {
      totalTasksStat.inc();
    }

    synchronized void incrSucceededTasks() {
      succeededTasksStat.inc();
    }

    synchronized void remove() {
      collector.removeStat(totalTasksKey);
      collector.removeStat(succeededTasksKey);
    }

  }
}
