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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TaskStatus.State;

import java.io.*;
import java.util.*;

/**************************************************
 * A TaskTrackerStatus is a MapReduce primitive.  Keeps
 * info on a TaskTracker.  The JobTracker maintains a set
 * of the most recent TaskTrackerStatus objects for each
 * unique TaskTracker it knows about.
 *
 * This is NOT a public interface!
 **************************************************/
public class TaskTrackerStatus implements Writable {
  public static final Log LOG = LogFactory.getLog(TaskTrackerStatus.class);
  
  static {                                        // register a ctor
    WritableFactories.setFactory
      (TaskTrackerStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new TaskTrackerStatus(); }
       });
  }

  String trackerName;
  String host;
  int httpPort;
  int taskFailures;
  int dirFailures;
  List<TaskStatus> taskReports;
    
  volatile long lastSeen;
  private int maxMapTasks;
  private int maxReduceTasks;
  private TaskTrackerHealthStatus healthStatus;
   
  public static final int UNAVAILABLE = -1;
  /**
   * Class representing a collection of resources on this tasktracker.
   */
  public static class ResourceStatus implements Writable {
    
    private long totalVirtualMemory;
    private long totalPhysicalMemory;
    private long mapSlotMemorySizeOnTT;
    private long reduceSlotMemorySizeOnTT;
    private long availableSpace;
    
    private long availableVirtualMemory = UNAVAILABLE; // in byte
    private long availablePhysicalMemory = UNAVAILABLE; // in byte
    private int numProcessors = UNAVAILABLE;
    private long cumulativeCpuTime = UNAVAILABLE; // in millisecond
    private long cpuFrequency = UNAVAILABLE; // in kHz
    private float cpuUsage = UNAVAILABLE; // in %

    ResourceStatus() {
      totalVirtualMemory = JobConf.DISABLED_MEMORY_LIMIT;
      totalPhysicalMemory = JobConf.DISABLED_MEMORY_LIMIT;
      mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      reduceSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      availableSpace = Long.MAX_VALUE;
    }

    /**
     * Set the maximum amount of virtual memory on the tasktracker.
     * 
     * @param vmem maximum amount of virtual memory on the tasktracker in bytes.
     */
    void setTotalVirtualMemory(long totalMem) {
      totalVirtualMemory = totalMem;
    }

    /**
     * Get the maximum amount of virtual memory on the tasktracker.
     * 
     * If this is {@link JobConf#DISABLED_MEMORY_LIMIT}, it should be ignored
     * and not used in any computation.
     * 
     * @return the maximum amount of virtual memory on the tasktracker in bytes.
     */
    public long getTotalVirtualMemory() {
      return totalVirtualMemory;
    }

    /**
     * Set the maximum amount of physical memory on the tasktracker.
     * 
     * @param totalRAM maximum amount of physical memory on the tasktracker in
     *          bytes.
     */
    void setTotalPhysicalMemory(long totalRAM) {
      totalPhysicalMemory = totalRAM;
    }

    /**
     * Get the maximum amount of physical memory on the tasktracker.
     * 
     * If this is {@link JobConf#DISABLED_MEMORY_LIMIT}, it should be ignored
     * and not used in any computation.
     * 
     * @return maximum amount of physical memory on the tasktracker in bytes.
     */
    public long getTotalPhysicalMemory() {
      return totalPhysicalMemory;
    }

    /**
     * Set the memory size of each map slot on this TT. This will be used by JT
     * for accounting more slots for jobs that use more memory.
     * 
     * @param mem
     */
    void setMapSlotMemorySizeOnTT(long mem) {
      mapSlotMemorySizeOnTT = mem;
    }

    /**
     * Get the memory size of each map slot on this TT. See
     * {@link #setMapSlotMemorySizeOnTT(long)}
     * 
     * @return
     */
    long getMapSlotMemorySizeOnTT() {
      return mapSlotMemorySizeOnTT;
    }

    /**
     * Set the memory size of each reduce slot on this TT. This will be used by
     * JT for accounting more slots for jobs that use more memory.
     * 
     * @param mem
     */
    void setReduceSlotMemorySizeOnTT(long mem) {
      reduceSlotMemorySizeOnTT = mem;
    }

    /**
     * Get the memory size of each reduce slot on this TT. See
     * {@link #setReduceSlotMemorySizeOnTT(long)}
     * 
     * @return
     */
    long getReduceSlotMemorySizeOnTT() {
      return reduceSlotMemorySizeOnTT;
    }

    /**
     * Set the available disk space on the TT
     * @param availSpace
     */
    void setAvailableSpace(long availSpace) {
      availableSpace = availSpace;
    }
    
    /**
     * Will return LONG_MAX if space hasn't been measured yet.
     * @return bytes of available local disk space on this tasktracker.
     */    
    public long getAvailableSpace() {
      return availableSpace;
    }

    /**
     * Set the amount of available virtual memory on the tasktracker.
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param vmem amount of available virtual memory on the tasktracker
     *                    in bytes.
     */
    void setAvailableVirtualMemory(long availableMem) {
      availableVirtualMemory = availableMem > 0 ?
                               availableMem : UNAVAILABLE;
    }

    /**
     * Get the amount of available virtual memory on the tasktracker.
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return the amount of available virtual memory on the tasktracker
     *             in bytes.
     */
    long getAvailableVirtualMemory() {
      return availableVirtualMemory;
    }

    /**
     * Set the amount of available physical memory on the tasktracker.
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param availableRAM amount of available physical memory on the
     *                     tasktracker in bytes.
     */
    void setAvailablePhysicalMemory(long availableRAM) {
      availablePhysicalMemory = availableRAM > 0 ?
                                availableRAM : UNAVAILABLE;
    }

    /**
     * Get the amount of available physical memory on the tasktracker.
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return amount of available physical memory on the tasktracker in bytes.
     */
    long getAvailablePhysicalMemory() {
      return availablePhysicalMemory;
    }

    /**
     * Set the CPU frequency of this TaskTracker
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param cpuFrequency CPU frequency in kHz
     */
    public void setCpuFrequency(long cpuFrequency) {
      this.cpuFrequency = cpuFrequency > 0 ?
                          cpuFrequency : UNAVAILABLE;
    }

    /**
     * Get the CPU frequency of this TaskTracker
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return CPU frequency in kHz
     */
    public long getCpuFrequency() {
      return cpuFrequency;
    }

    /**
     * Set the number of processors on this TaskTracker
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param numProcessors number of processors
     */
    public void setNumProcessors(int numProcessors) {
      this.numProcessors = numProcessors > 0 ?
                           numProcessors : UNAVAILABLE;
    }

    /**
     * Get the number of processors on this TaskTracker
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return number of processors
     */
    public int getNumProcessors() {
      return numProcessors;
    }

    /**
     * Set the cumulative CPU time on this TaskTracker since it is up
     * It can be set to UNAVAILABLE if it is currently unavailable.
     *
     * @param cumulativeCpuTime Used CPU time in millisecond
     */
    public void setCumulativeCpuTime(long cumulativeCpuTime) {
      this.cumulativeCpuTime = cumulativeCpuTime > 0 ?
                               cumulativeCpuTime : UNAVAILABLE;
    }

    /**
     * Get the cumulative CPU time on this TaskTracker since it is up
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return used CPU time in milliseconds
     */
    public long getCumulativeCpuTime() {
      return cumulativeCpuTime;
    }
    
    /**
     * Set the CPU usage on this TaskTracker
     * 
     * @param cpuUsage CPU usage in %
     */
    public void setCpuUsage(float cpuUsage) {
      this.cpuUsage = cpuUsage;
    }

    /**
     * Get the CPU usage on this TaskTracker
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return CPU usage in %
     */
    public float getCpuUsage() {
      return cpuUsage;
    }
    
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVLong(out, totalVirtualMemory);
      WritableUtils.writeVLong(out, totalPhysicalMemory);
      WritableUtils.writeVLong(out, availableVirtualMemory);
      WritableUtils.writeVLong(out, availablePhysicalMemory);
      WritableUtils.writeVLong(out, mapSlotMemorySizeOnTT);
      WritableUtils.writeVLong(out, reduceSlotMemorySizeOnTT);
      WritableUtils.writeVLong(out, availableSpace);
      WritableUtils.writeVLong(out, cumulativeCpuTime);
      WritableUtils.writeVLong(out, cpuFrequency);
      WritableUtils.writeVInt(out, numProcessors);
      out.writeFloat(getCpuUsage());
    }
    
    public void readFields(DataInput in) throws IOException {
      totalVirtualMemory = WritableUtils.readVLong(in);
      totalPhysicalMemory = WritableUtils.readVLong(in);
      availableVirtualMemory = WritableUtils.readVLong(in);
      availablePhysicalMemory = WritableUtils.readVLong(in);
      mapSlotMemorySizeOnTT = WritableUtils.readVLong(in);
      reduceSlotMemorySizeOnTT = WritableUtils.readVLong(in);
      availableSpace = WritableUtils.readVLong(in);
      cumulativeCpuTime = WritableUtils.readVLong(in);
      cpuFrequency = WritableUtils.readVLong(in);
      numProcessors = WritableUtils.readVInt(in);
      setCpuUsage(in.readFloat());
    }
  }
  
  private ResourceStatus resStatus;
  
  /**
   */
  public TaskTrackerStatus() {
    taskReports = new ArrayList<TaskStatus>();
    resStatus = new ResourceStatus();
    this.healthStatus = new TaskTrackerHealthStatus();
  }

  TaskTrackerStatus(String trackerName, String host) {
    this();
    this.trackerName = trackerName;
    this.host = host;
  }

  /**
   */
  public TaskTrackerStatus(String trackerName, String host, 
                           int httpPort, List<TaskStatus> taskReports, 
                           int taskFailures, int dirFailures,
                           int maxMapTasks, int maxReduceTasks) {
    this.trackerName = trackerName;
    this.host = host;
    this.httpPort = httpPort;

    this.taskReports = new ArrayList<TaskStatus>(taskReports);
    this.taskFailures = taskFailures;
    this.dirFailures = dirFailures;
    this.maxMapTasks = maxMapTasks;
    this.maxReduceTasks = maxReduceTasks;
    this.resStatus = new ResourceStatus();
    this.healthStatus = new TaskTrackerHealthStatus();
  }

  /**
   */
  public String getTrackerName() {
    return trackerName;
  }
  /**
   */
  public String getHost() {
    return host;
  }

  /**
   * Get the port that this task tracker is serving http requests on.
   * @return the http port
   */
  public int getHttpPort() {
    return httpPort;
  }

  /**
   * Get the number of tasks that have failed on this tracker.
   * @return The number of failed tasks
   */
  public int getFailures() {
    return taskFailures;
  }

  /**
   * Get the number of local directories that have failed on this tracker.
   * @return The number of failed local directories
   */
  public int getDirFailures() {
    return dirFailures;
  }
    
  /**
   * Get the current tasks at the TaskTracker.
   * Tasks are tracked by a {@link TaskStatus} object.
   * 
   * @return a list of {@link TaskStatus} representing 
   *         the current tasks at the TaskTracker.
   */
  public List<TaskStatus> getTaskReports() {
    return taskReports;
  }
   
  /**
   * Is the given task considered as 'running' ?
   * @param taskStatus
   * @return
   */
  private boolean isTaskRunning(TaskStatus taskStatus) {
    TaskStatus.State state = taskStatus.getRunState();
    return (state == State.RUNNING || state == State.UNASSIGNED || 
            taskStatus.inTaskCleanupPhase());
  }
  
  /**
   * Get the number of running map tasks.
   * @return the number of running map tasks
   */
  public int countMapTasks() {
    int mapCount = 0;
    for (TaskStatus ts : taskReports) {
      if (ts.getIsMap() && isTaskRunning(ts)) {
        mapCount++;
      }
    }
    return mapCount;
  }

  /**
   * Get the number of occupied map slots.
   * @return the number of occupied map slots
   */
  public int countOccupiedMapSlots() {
    int mapSlotsCount = 0;
    for (TaskStatus ts : taskReports) {
      if (ts.getIsMap() && isTaskRunning(ts)) {
        mapSlotsCount += ts.getNumSlots();
      }
    }
    return mapSlotsCount;
  }
  
  /**
   * Get available map slots.
   * @return available map slots
   */
  public int getAvailableMapSlots() {
    return getMaxMapSlots() - countOccupiedMapSlots();
  }
  
  /**
   * Get the number of running reduce tasks.
   * @return the number of running reduce tasks
   */
  public int countReduceTasks() {
    int reduceCount = 0;
    for (TaskStatus ts : taskReports) {
      if ((!ts.getIsMap()) && isTaskRunning(ts)) {
        reduceCount++;
      }
    }
    return reduceCount;
  }

  /**
   * Get the number of occupied reduce slots.
   * @return the number of occupied reduce slots
   */
  public int countOccupiedReduceSlots() {
    int reduceSlotsCount = 0;
    for (TaskStatus ts : taskReports) {
      if ((!ts.getIsMap()) && isTaskRunning(ts)) {
        reduceSlotsCount += ts.getNumSlots();
      }
    }
    return reduceSlotsCount;
  }
  
  /**
   * Get available reduce slots.
   * @return available reduce slots
   */
  public int getAvailableReduceSlots() {
    return getMaxReduceSlots() - countOccupiedReduceSlots();
  }
  

  /**
   */
  public long getLastSeen() {
    return lastSeen;
  }
  /**
   */
  public void setLastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  /**
   * Get the maximum map slots for this node.
   * @return the maximum map slots for this node
   */
  public int getMaxMapSlots() {
    return maxMapTasks;
  }
  
  /**
   * Get the maximum reduce slots for this node.
   * @return the maximum reduce slots for this node
   */
  public int getMaxReduceSlots() {
    return maxReduceTasks;
  }  
  
  /**
   * Return the {@link ResourceStatus} object configured with this
   * status.
   * 
   * @return the resource status
   */
  public ResourceStatus getResourceStatus() {
    return resStatus;
  }

  /**
   * Returns health status of the task tracker.
   * @return health status of Task Tracker
   */
  public TaskTrackerHealthStatus getHealthStatus() {
    return healthStatus;
  }

  /**
   * Static class which encapsulates the Node health
   * related fields.
   * 
   */
  /**
   * Static class which encapsulates the Node health
   * related fields.
   * 
   */
  static class TaskTrackerHealthStatus implements Writable {
    
    private boolean isNodeHealthy;
    
    private String healthReport;
    
    private long lastReported;
    
    public TaskTrackerHealthStatus(boolean isNodeHealthy, String healthReport,
        long lastReported) {
      this.isNodeHealthy = isNodeHealthy;
      this.healthReport = healthReport;
      this.lastReported = lastReported;
    }
    
    public TaskTrackerHealthStatus() {
      this.isNodeHealthy = true;
      this.healthReport = "";
      this.lastReported = System.currentTimeMillis();
    }

    /**
     * Sets whether or not a task tracker is healthy or not, based on the
     * output from the node health script.
     * 
     * @param isNodeHealthy
     */
    void setNodeHealthy(boolean isNodeHealthy) {
      this.isNodeHealthy = isNodeHealthy;
    }

    /**
     * Returns if node is healthy or not based on result from node health
     * script.
     * 
     * @return true if the node is healthy.
     */
    boolean isNodeHealthy() {
      return isNodeHealthy;
    }

    /**
     * Sets the health report based on the output from the health script.
     * 
     * @param healthReport
     *          String listing cause of failure.
     */
    void setHealthReport(String healthReport) {
      this.healthReport = healthReport;
    }

    /**
     * Returns the health report of the node if any, The health report is
     * only populated when the node is not healthy.
     * 
     * @return health report of the node if any
     */
    String getHealthReport() {
      return healthReport;
    }

    /**
     * Sets when the TT got its health information last 
     * from node health monitoring service.
     * 
     * @param lastReported last reported time by node 
     * health script
     */
    public void setLastReported(long lastReported) {
      this.lastReported = lastReported;
    }

    /**
     * Gets time of most recent node health update.
     * 
     * @return time stamp of most recent health update.
     */
    public long getLastReported() {
      return lastReported;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      isNodeHealthy = in.readBoolean();
      healthReport = Text.readString(in);
      lastReported = in.readLong();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(isNodeHealthy);
      Text.writeString(out, healthReport);
      out.writeLong(lastReported);
    }
    
  }
  
  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, trackerName);
    Text.writeString(out, host);
    out.writeInt(httpPort);
    out.writeInt(taskFailures);
    out.writeInt(dirFailures);
    out.writeInt(maxMapTasks);
    out.writeInt(maxReduceTasks);
    resStatus.write(out);
    out.writeInt(taskReports.size());

    for (TaskStatus taskStatus : taskReports) {
      TaskStatus.writeTaskStatus(out, taskStatus);
    }
    getHealthStatus().write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.trackerName = Text.readString(in);
    this.host = Text.readString(in);
    this.httpPort = in.readInt();
    this.taskFailures = in.readInt();
    this.dirFailures = in.readInt();
    this.maxMapTasks = in.readInt();
    this.maxReduceTasks = in.readInt();
    resStatus.readFields(in);
    taskReports.clear();
    int numTasks = in.readInt();

    for (int i = 0; i < numTasks; i++) {
      taskReports.add(TaskStatus.readTaskStatus(in));
    }
    getHealthStatus().readFields(in);
  }
}
