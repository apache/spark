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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Status information on the current state of the Map-Reduce cluster.
 * 
 * <p><code>ClusterStatus</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   Size of the cluster. 
 *   </li>
 *   <li>
 *   Name of the trackers. 
 *   </li>
 *   <li>
 *   Task capacity of the cluster. 
 *   </li>
 *   <li>
 *   The number of currently running map & reduce tasks.
 *   </li>
 *   <li>
 *   State of the <code>JobTracker</code>.
 *   </li>
 * </ol></p>
 * 
 * <p>Clients can query for the latest <code>ClusterStatus</code>, via 
 * {@link JobClient#getClusterStatus()}.</p>
 * 
 * @see JobClient
 */
public class ClusterStatus implements Writable {

  private int numActiveTrackers;
  private Collection<String> activeTrackers = new ArrayList<String>();
  private Collection<String> blacklistedTrackers = new ArrayList<String>();
  private int numBlacklistedTrackers;
  private int numExcludedNodes;
  private long ttExpiryInterval;
  private int map_tasks;
  private int reduce_tasks;
  private int max_map_tasks;
  private int max_reduce_tasks;
  private JobTracker.State state;

  public static final long UNINITIALIZED_MEMORY_VALUE = -1;
  private long used_memory = UNINITIALIZED_MEMORY_VALUE;
  private long max_memory = UNINITIALIZED_MEMORY_VALUE;

  ClusterStatus() {}
  
  /**
   * Construct a new cluster status.
   * 
   * @param trackers no. of tasktrackers in the cluster
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   * @deprecated 
   */
  @Deprecated
  ClusterStatus(int trackers, int maps, int reduces, int maxMaps,
                int maxReduces, JobTracker.State state) {
    this(trackers, 0, JobTracker.TASKTRACKER_EXPIRY_INTERVAL, maps, reduces,
        maxMaps, maxReduces, state);
  }
  
  /**
   * Construct a new cluster status.
   * 
   * @param trackers no. of tasktrackers in the cluster
   * @param blacklists no of blacklisted task trackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
                int maps, int reduces,
                int maxMaps, int maxReduces, JobTracker.State state) {
    this(trackers, blacklists, ttExpiryInterval, maps, reduces, maxMaps, 
         maxReduces, state, 0);
  }

  /**
   * Construct a new cluster status.
   * @param trackers no. of tasktrackers in the cluster
   * @param blacklists no of blacklisted task trackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   * @param numDecommissionedNodes number of decommission trackers
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
                int maps, int reduces, int maxMaps, int maxReduces, 
                JobTracker.State state, int numDecommissionedNodes) {
    this(trackers, blacklists, ttExpiryInterval, maps, reduces, 
         maxMaps, maxReduces, state, numDecommissionedNodes, 
         UNINITIALIZED_MEMORY_VALUE, UNINITIALIZED_MEMORY_VALUE);
  }

  /**
   * Construct a new cluster status.
   * 
   * @param activeTrackers active tasktrackers in the cluster
   * @param blacklistedTrackers blacklisted tasktrackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   */
  ClusterStatus(Collection<String> activeTrackers, 
      Collection<String> blacklistedTrackers,
      long ttExpiryInterval,
      int maps, int reduces, int maxMaps, int maxReduces, 
      JobTracker.State state) {
    this(activeTrackers, blacklistedTrackers, ttExpiryInterval, maps, reduces, 
         maxMaps, maxReduces, state, 0);
  }

  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
      int maps, int reduces, int maxMaps, int maxReduces, 
      JobTracker.State state, int numDecommissionedNodes,
      long used_memory, long max_memory) {
    numActiveTrackers = trackers;
    numBlacklistedTrackers = blacklists;
    this.numExcludedNodes = numDecommissionedNodes;
    this.ttExpiryInterval = ttExpiryInterval;
    map_tasks = maps;
    reduce_tasks = reduces;
    max_map_tasks = maxMaps;
    max_reduce_tasks = maxReduces;
    this.state = state;
    this.used_memory = used_memory;
    this.max_memory = max_memory;
  }
  
  /**
   * Construct a new cluster status. 
   * @param activeTrackers active tasktrackers in the cluster
   * @param blacklistedTrackers blacklisted tasktrackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   * @param numDecommissionNodes number of decommission trackers
   */
  ClusterStatus(Collection<String> activeTrackers, 
                Collection<String> blacklistedTrackers, long ttExpiryInterval,
                int maps, int reduces, int maxMaps, int maxReduces, 
                JobTracker.State state, int numDecommissionNodes) {
    this(activeTrackers.size(), blacklistedTrackers.size(), ttExpiryInterval, 
        maps, reduces, maxMaps, maxReduces, state, numDecommissionNodes, 
        Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory());
    this.activeTrackers = activeTrackers;
    this.blacklistedTrackers = blacklistedTrackers;
  }

  /**
   * Get the number of task trackers in the cluster.
   * 
   * @return the number of task trackers in the cluster.
   */
  public int getTaskTrackers() {
    return numActiveTrackers;
  }
  
  /**
   * Get the names of task trackers in the cluster.
   * 
   * @return the active task trackers in the cluster.
   */
  public Collection<String> getActiveTrackerNames() {
    return activeTrackers;
  }

  /**
   * Get the names of task trackers in the cluster.
   * 
   * @return the blacklisted task trackers in the cluster.
   */
  public Collection<String> getBlacklistedTrackerNames() {
    return blacklistedTrackers;
  }
  
  /**
   * Get the number of blacklisted task trackers in the cluster.
   * 
   * @return the number of blacklisted task trackers in the cluster.
   */
  public int getBlacklistedTrackers() {
    return numBlacklistedTrackers;
  }
  
  /**
   * Get the number of excluded hosts in the cluster.
   * @return the number of excluded hosts in the cluster.
   */
  public int getNumExcludedNodes() {
    return numExcludedNodes;
  }
  
  /**
   * Get the tasktracker expiry interval for the cluster
   * @return the expiry interval in msec
   */
  public long getTTExpiryInterval() {
    return ttExpiryInterval;
  }
  
  /**
   * Get the number of currently running map tasks in the cluster.
   * 
   * @return the number of currently running map tasks in the cluster.
   */
  public int getMapTasks() {
    return map_tasks;
  }
  
  /**
   * Get the number of currently running reduce tasks in the cluster.
   * 
   * @return the number of currently running reduce tasks in the cluster.
   */
  public int getReduceTasks() {
    return reduce_tasks;
  }
  
  /**
   * Get the maximum capacity for running map tasks in the cluster.
   * 
   * @return the maximum capacity for running map tasks in the cluster.
   */
  public int getMaxMapTasks() {
    return max_map_tasks;
  }

  /**
   * Get the maximum capacity for running reduce tasks in the cluster.
   * 
   * @return the maximum capacity for running reduce tasks in the cluster.
   */
  public int getMaxReduceTasks() {
    return max_reduce_tasks;
  }
  
  /**
   * Get the current state of the <code>JobTracker</code>, 
   * as {@link JobTracker.State}
   * 
   * @return the current state of the <code>JobTracker</code>.
   */
  public JobTracker.State getJobTrackerState() {
    return state;
  }

  /**
   * Get the total heap memory used by the <code>JobTracker</code>
   * 
   * @return the size of heap memory used by the <code>JobTracker</code>
   */
  public long getUsedMemory() {
    return used_memory;
  }

  /**
   * Get the maximum configured heap memory that can be used by the <code>JobTracker</code>
   * 
   * @return the configured size of max heap memory that can be used by the <code>JobTracker</code>
   */
  public long getMaxMemory() {
    return max_memory;
  }

  public void write(DataOutput out) throws IOException {
    if (activeTrackers.size() == 0) {
      out.writeInt(numActiveTrackers);
      out.writeInt(0);
    } else {
      out.writeInt(activeTrackers.size());
      out.writeInt(activeTrackers.size());
      for (String tracker : activeTrackers) {
        Text.writeString(out, tracker);
      }
    }
    if (blacklistedTrackers.size() == 0) {
      out.writeInt(numBlacklistedTrackers);
      out.writeInt(0);
    } else {
      out.writeInt(blacklistedTrackers.size());
      out.writeInt(blacklistedTrackers.size());
      for (String tracker : blacklistedTrackers) {
        Text.writeString(out, tracker);
      }
    }
    out.writeInt(numExcludedNodes);
    out.writeLong(ttExpiryInterval);
    out.writeInt(map_tasks);
    out.writeInt(reduce_tasks);
    out.writeInt(max_map_tasks);
    out.writeInt(max_reduce_tasks);
    out.writeLong(used_memory);
    out.writeLong(max_memory);
    WritableUtils.writeEnum(out, state);
  }

  public void readFields(DataInput in) throws IOException {
    numActiveTrackers = in.readInt();
    int numTrackerNames = in.readInt();
    if (numTrackerNames > 0) {
      for (int i = 0; i < numTrackerNames; i++) {
        String name = Text.readString(in);
        activeTrackers.add(name);
      }
    }
    numBlacklistedTrackers = in.readInt();
    numTrackerNames = in.readInt();
    if (numTrackerNames > 0) {
      for (int i = 0; i < numTrackerNames; i++) {
        String name = Text.readString(in);
        blacklistedTrackers.add(name);
      }
    }
    numExcludedNodes = in.readInt();
    ttExpiryInterval = in.readLong();
    map_tasks = in.readInt();
    reduce_tasks = in.readInt();
    max_map_tasks = in.readInt();
    max_reduce_tasks = in.readInt();
    used_memory = in.readLong();
    max_memory = in.readLong();
    state = WritableUtils.readEnum(in, JobTracker.State.class);
  }
}
