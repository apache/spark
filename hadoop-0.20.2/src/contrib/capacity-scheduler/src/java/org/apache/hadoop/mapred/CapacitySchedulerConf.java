/** Licensed to the Apache Software Foundation (ASF) under one
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Class providing access to resource manager configuration.
 * 
 * Resource manager configuration involves setting up queues, and defining
 * various properties for the queues. These are typically read from a file 
 * called capacity-scheduler.xml that must be in the classpath of the
 * application. The class provides APIs to get/set and reload the 
 * configuration for the queues.
 */
class CapacitySchedulerConf {
  
  /** Default file name from which the resource manager configuration is read. */ 
  public static final String SCHEDULER_CONF_FILE = "capacity-scheduler.xml";
  
  private int defaultUlimitMinimum;
  
  private float defaultUserLimitFactor;
  
  private boolean defaultSupportPriority;
  
  private static final String QUEUE_CONF_PROPERTY_NAME_PREFIX = 
    "mapred.capacity-scheduler.queue.";

  /**
   * If {@link JobConf#MAPRED_TASK_MAXPMEM_PROPERTY} is set to
   * {@link JobConf#DISABLED_MEMORY_LIMIT}, this configuration will be used to
   * calculate job's physical memory requirements as a percentage of the job's
   * virtual memory requirements set via
   * {@link JobConf#setMaxVirtualMemoryForTask()}. This property thus provides
   * default value of physical memory for job's that don't explicitly specify
   * physical memory requirements.
   * <p/>
   * It defaults to {@link JobConf#DISABLED_MEMORY_LIMIT} and if not explicitly
   * set to a valid value, scheduler will not consider physical memory for
   * scheduling even if virtual memory based scheduling is enabled.
   *
   * @deprecated
   */
  @Deprecated
  static String DEFAULT_PERCENTAGE_OF_PMEM_IN_VMEM_PROPERTY =
    "mapred.capacity-scheduler.task.default-pmem-percentage-in-vmem";

  /**
   * Configuration that provides an upper limit on the maximum physical memory
   * that can be specified by a job. The job configuration
   * {@link JobConf#MAPRED_TASK_MAXPMEM_PROPERTY} should,
   * by definition, be less than this value. If not, the job will be rejected
   * by the scheduler. If it is set to {@link JobConf#DISABLED_MEMORY_LIMIT},
   * scheduler will not consider physical memory for scheduling even if virtual
   * memory based scheduling is enabled.
   *
   * @deprecated
   */
  @Deprecated
  static final String UPPER_LIMIT_ON_TASK_PMEM_PROPERTY =
    "mapred.capacity-scheduler.task.limit.maxpmem";


  private static final String CAPACITY_PROPERTY = "capacity";

  /**
    * A maximum capacity defines a limit beyond which a queue
    * cannot expand .
   */
   static final String MAX_CAPACITY_PROPERTY ="maximum-capacity";

  /**
   * The constant which defines the default initialization thread
   * polling interval, denoted in milliseconds.
   */
  private static final int INITIALIZATION_THREAD_POLLING_INTERVAL = 3000;

  /**
   * The constant which defines the maximum number of worker threads to be
   * spawned off for job initialization
   */
  private static final int MAX_INITIALIZATION_WORKER_THREADS = 5;

  private Configuration rmConf;

  private int defaultInitToAcceptJobsFactor;
  private int defaultMaxActiveTasksPerUserToInitialize;
  private int defaultMaxActiveTasksPerQueueToInitialize;
  
  static final String MAX_SYSTEM_JOBS_KEY = 
    "mapred.capacity-scheduler.maximum-system-jobs";
  
  static final int DEFAULT_MAX_SYSTEM_JOBS = 5000;
  
  static final int DEFAULT_MAX_TASKS_TO_SCHEDULE_AFTER_OFFSWITCH = 0;
  
  /**
   * Create a new Capacity scheduler conf.
   * This method reads from the default configuration file mentioned in
   * {@link SCHEDULER_CONF_FILE}, that must be present in the classpath of the
   * application.
   */
  public CapacitySchedulerConf() {
    rmConf = new Configuration(false);
    rmConf.addResource(SCHEDULER_CONF_FILE);
    initializeDefaults();
  }

  /**
   * Create a new Cacpacity scheduler conf reading the specified configuration
   * file.
   * 
   * @param configFile {@link Path} to the configuration file containing
   * the resource manager configuration.
   */
  public CapacitySchedulerConf(Path configFile) {
    rmConf = new Configuration(false);
    rmConf.addResource(configFile);
    initializeDefaults();
  }
  
  /*
   * Method used to initialize the default values and the queue list
   * which is used by the Capacity Scheduler.
   */
  private void initializeDefaults() {
    defaultUlimitMinimum = 
      rmConf.getInt(
          "mapred.capacity-scheduler.default-minimum-user-limit-percent", 100);
    defaultUserLimitFactor = 
      rmConf.getFloat("mapred.capacity-scheduler.default-user-limit-factor", 
          1.0f);
    defaultSupportPriority = rmConf.getBoolean(
        "mapred.capacity-scheduler.default-supports-priority", false);
    defaultMaxActiveTasksPerQueueToInitialize = 
      rmConf.getInt(
          "mapred.capacity-scheduler.default-maximum-active-tasks-per-queue", 
          200000);
    defaultMaxActiveTasksPerUserToInitialize = 
      rmConf.getInt(
          "mapred.capacity-scheduler.default-maximum-active-tasks-per-user", 
          100000);
    defaultInitToAcceptJobsFactor =
      rmConf.getInt("mapred.capacity-scheduler.default-init-accept-jobs-factor", 
          10);
  }
  
  /**
   * Get the percentage of the cluster for the specified queue.
   * 
   * This method defaults to configured default Capacity if
   * no value is specified in the configuration for this queue. 
   * If the configured capacity is negative value or greater than 100 an
   * {@link IllegalArgumentException} is thrown.
   * 
   * If default capacity is not configured for a queue, then
   * system allocates capacity based on what is free at the time of 
   * capacity scheduler start
   * 
   * 
   * @param queue name of the queue
   * @return percent of the cluster for the queue.
   */
  public float getCapacity(String queue) {
    //Check done in order to return default capacity which can be negative
    //In case of both capacity and default capacity not configured.
    //Last check is if the configuration is specified and is marked as
    //negative we throw exception
    String raw = rmConf.getRaw(toFullPropertyName(queue, CAPACITY_PROPERTY));
    if(raw == null) {
      return -1;
    }
    float result = rmConf.getFloat(
      toFullPropertyName(queue, CAPACITY_PROPERTY), -1);
    if (result < 0.0 || result > 100.0) {
      throw new IllegalArgumentException(
        "Illegal capacity for queue " + queue +
                                         " of " + result);
    }
    return result;
  }
  
  /**
   * Sets the capacity of the given queue.
   * 
   * @param queue name of the queue
   * @param capacity percent of the cluster for the queue.
   */
  public void setCapacity(String queue,float capacity) {
    rmConf.setFloat(toFullPropertyName(queue, CAPACITY_PROPERTY),capacity);
  }

  /**
   * Return the maximum percentage of the cluster capacity that can be used by
   * the given queue.
   * This percentage defines a limit beyond which a
   * queue cannot use the capacity of cluster.
   * This provides a means to limit how much excess capacity a
   * queue can use. By default, there is no limit.
   *
   * The maximum-capacity of a queue can only be
   * greater than or equal to its minimum capacity.
   *
   * @param queue name of the queue.
   * @return maximum-capacity for the given queue
   */
  public float getMaxCapacity(String queue) {
    float result = rmConf.getFloat(
      toFullPropertyName(queue, MAX_CAPACITY_PROPERTY), -1);

    //if result is 0 or less than 0 set it to -1
    result = (result <= 0) ? -1 : result;

    if (result > 100.0) {
      throw new IllegalArgumentException(
        "Illegal " + MAX_CAPACITY_PROPERTY +
          " for queue " + queue + " of " + result);
    }

    if ((result != -1) && (result < getCapacity(queue))) {
      throw new IllegalArgumentException(
        MAX_CAPACITY_PROPERTY + " " + result +
          " for a queue should be greater than or equal to capacity ");
    }
    return result;
  }

    /**
   * Sets the maxCapacity of the given queue.
   *
   * @param queue name of the queue
   * @param maxCapacity percent of the cluster for the queue.
   */
  public void setMaxCapacity(String queue,float maxCapacity) {
      rmConf.setFloat(
        toFullPropertyName(queue, MAX_CAPACITY_PROPERTY), maxCapacity);
  }
  
  /**
   * Get whether priority is supported for this queue.
   * 
   * If this value is false, then job priorities will be ignored in 
   * scheduling decisions. This method defaults to <code>false</code> if 
   * the property is not configured for this queue. 
   * @param queue name of the queue
   * @return Whether this queue supports priority or not.
   */
  public boolean isPrioritySupported(String queue) {
    return rmConf.getBoolean(toFullPropertyName(queue, "supports-priority"),
        defaultSupportPriority);  
  }
  
  /**
   * Set whether priority is supported for this queue.
   * 
   * 
   * @param queue name of the queue
   * @param value true, if the queue must support priorities, false otherwise.
   */
  public void setPrioritySupported(String queue, boolean value) {
    rmConf.setBoolean(toFullPropertyName(queue, "supports-priority"), value);
  }
  
  /**
   * Get the minimum limit of resources for any user submitting jobs in 
   * this queue, in percentage.
   * 
   * This method defaults to default user limit configured if
   * no value is specified in the configuration for this queue.
   * 
   * Throws an {@link IllegalArgumentException} when invalid value is 
   * configured.
   * 
   * @param queue name of the queue
   * @return minimum limit of resources, in percentage, that will be 
   * available for a user.
   * 
   */
  public int getMinimumUserLimitPercent(String queue) {
    int userLimit = rmConf.getInt(toFullPropertyName(queue,
        "minimum-user-limit-percent"), defaultUlimitMinimum);
    if(userLimit <= 0 || userLimit > 100) {
      throw new IllegalArgumentException("Invalid user limit : "
          + userLimit + " for queue : " + queue);
    }
    return userLimit;
  }
  
  /**
   * Set the minimum limit of resources for any user submitting jobs in
   * this queue, in percentage.
   * 
   * @param queue name of the queue
   * @param value minimum limit of resources for any user submitting jobs
   * in this queue
   */
  public void setMinimumUserLimitPercent(String queue, int value) {
    rmConf.setInt(toFullPropertyName(queue, "minimum-user-limit-percent"), 
                    value);
  }
  
  /**
   * Get the factor of queue capacity above which a single user in a queue
   * can consume resources.
   * 
   * @param queue queue name
   * @return factor of queue capacity above which a single user in a queue
   *         can consume resources
   */
  public float getUserLimitFactor(String queue) {
    return rmConf.getFloat(toFullPropertyName(queue, "user-limit-factor"), 
        defaultUserLimitFactor);
  }
  
  /**
   * Set the factor of queue capacity above which a single user in a queue
   * can consume resources.
   * 
   * @param queue queue name
   * @param userLimitFactor factor of queue capacity above which a single user 
   *                        in a queue can consume resources
   */
  public void setUserLimitFactor(String queue, float userLimitFactor) {
    rmConf.setFloat(toFullPropertyName(queue, "user-limit-factor"), 
        userLimitFactor);
  }
  
  /**
   * Reload configuration by clearing the information read from the 
   * underlying configuration file.
   */
  public synchronized void reloadConfiguration() {
    rmConf.reloadConfiguration();
    initializeDefaults();
  }
  
  static final String toFullPropertyName(String queue, 
                                                  String property) {
      return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }

  public int getMaxSystemJobs() {
    int maxSystemJobs = 
      rmConf.getInt(MAX_SYSTEM_JOBS_KEY, DEFAULT_MAX_SYSTEM_JOBS);
    if (maxSystemJobs <= 0) {
      throw new IllegalArgumentException("Invalid maximum system jobs: " + 
          maxSystemJobs);
    }
    
    return maxSystemJobs;
  }

  public void setMaxSystemJobs(int maxSystemJobs) {
    rmConf.setInt(MAX_SYSTEM_JOBS_KEY, maxSystemJobs);
  }
  
  public int getInitToAcceptJobsFactor(String queue) {
    int initToAccepFactor = 
      rmConf.getInt(toFullPropertyName(queue, "init-accept-jobs-factor"), 
          defaultInitToAcceptJobsFactor);
    if(initToAccepFactor <= 0) {
      throw new IllegalArgumentException(
          "Invalid maximum jobs per user configuration " + initToAccepFactor);
    }
    return initToAccepFactor;
  }
  
  public void setInitToAcceptJobsFactor(String queue, int initToAcceptFactor) {
    rmConf.setInt(toFullPropertyName(queue, "init-accept-jobs-factor"), 
        initToAcceptFactor);
  }
  
  /**
   * Get the maximum active tasks per queue to be initialized.
   * 
   * @param queue queue name
   */
  public int getMaxInitializedActiveTasks(String queue) {
    return rmConf.getInt(toFullPropertyName(queue, 
                                            "maximum-initialized-active-tasks"), 
                         defaultMaxActiveTasksPerQueueToInitialize);
  }
  
  /**
   * Set the maximum active tasks per queue to be initialized.
   * 
   * @param queue queue name
   * @param value maximum active tasks
   */
  public void setMaxInitializedActiveTasks(String queue, int value) {
    rmConf.setInt(toFullPropertyName(queue, "maximum-initialized-active-tasks"), 
                  value);
  }
  
  /**
   * Get the maximum active tasks per-user, per-queue to be initialized.
   * 
   * @param queue queue name
   */
  public int getMaxInitializedActiveTasksPerUser(String queue) {
    return rmConf.getInt(toFullPropertyName(queue, 
                                            "maximum-initialized-active-tasks-per-user"), 
                         defaultMaxActiveTasksPerUserToInitialize);
  }
  
  /**
   * Set the maximum active tasks per-user, per-queue to be initialized.
   * 
   * @param queue queue name
   * @param value maximum active tasks
   */
  public void setMaxInitializedActiveTasksPerUser(String queue, int value) {
    rmConf.setInt(toFullPropertyName(queue, "maximum-initialized-active-tasks-per-user"), 
                  value);
  }
  
  /**
   * Amount of time in milliseconds which poller thread and initialization
   * thread would sleep before looking at the queued jobs.
   * 
   * The default value if no corresponding configuration is present is
   * 5000 Milliseconds.
   *  
   * @return time in milliseconds.
   * @throws IllegalArgumentException if time is negative or zero.
   */
  public long getSleepInterval() {
    long sleepInterval = rmConf.getLong(
        "mapred.capacity-scheduler.init-poll-interval", 
        INITIALIZATION_THREAD_POLLING_INTERVAL);
    
    if(sleepInterval <= 0) {
      throw new IllegalArgumentException(
          "Invalid initializater poller interval " + sleepInterval);
    }
    
    return sleepInterval;
  }

  /**
   * Gets maximum number of threads which are spawned to initialize jobs
   * in job queue in  parallel. The number of threads should be always less than
   * or equal to number of job queues present.
   * 
   * If number of threads is configured to be more than job queues present,
   * then number of job queues is used as number of threads used for initializing
   * jobs.
   * 
   * So a given thread can have responsibility of initializing jobs from more 
   * than one queue.
   * 
   * The default value is 5
   * 
   * @return maximum number of threads spawned to initialize jobs in job queue
   * in parallel.
   */
  public int getMaxWorkerThreads() {
    int maxWorkerThreads = rmConf.getInt(
        "mapred.capacity-scheduler.init-worker-threads", 
        MAX_INITIALIZATION_WORKER_THREADS);
    if(maxWorkerThreads <= 0) {
      throw new IllegalArgumentException(
          "Invalid initializater worker thread number " + maxWorkerThreads);
    }
    return maxWorkerThreads;
  }
  /**
   * Set the sleep interval which initialization poller would sleep before 
   * it looks at the jobs in the job queue.
   * 
   * @param interval sleep interval
   */
  public void setSleepInterval(long interval) {
    rmConf.setLong(
        "mapred.capacity-scheduler.init-poll-interval", interval);
  }
  
  /**
   * Sets number of threads which can be spawned to initialize jobs in
   * parallel.
   * 
   * @param poolSize number of threads to be spawned to initialize jobs
   * in parallel.
   */
  public void setMaxWorkerThreads(int poolSize) {
    rmConf.setInt(
        "mapred.capacity-scheduler.init-worker-threads", poolSize);
  }
  
  /**
   * Get the maximum number of tasks which can be scheduled in a heartbeat.
   * @return the maximum number of tasks which can be scheduled in a heartbeat
   */
  public int getMaxTasksPerHeartbeat() {
    return rmConf.getInt(
        "mapred.capacity-scheduler.maximum-tasks-per-heartbeat", 
        Short.MAX_VALUE);
  }

  /**
   * Set the maximum number of tasks which can be scheduled in a heartbeat
   * @param maxTasksPerHeartbeat the maximum number of tasks which can be 
   *                             scheduled in a heartbeat
   */
  public void setMaxTasksPerHeartbeat(int maxTasksPerHeartbeat) {
    rmConf.setInt("mapred.capacity-scheduler.maximum-tasks-per-heartbeat", 
        maxTasksPerHeartbeat);
  }
  
  /**
   * Get the maximum number of tasks to schedule, per heartbeat, after an
   * off-switch task has been assigned.
   * 
   * @return the maximum number of tasks to schedule, per heartbeat, after an
   *         off-switch task has been assigned
   */
  public int getMaxTasksToAssignAfterOffSwitch() {
    return rmConf.getInt(
        "mapred.capacity-scheduler.maximum-tasks-after-offswitch", 
        DEFAULT_MAX_TASKS_TO_SCHEDULE_AFTER_OFFSWITCH);
  }
  
  /**
   * Set the maximum number of tasks to schedule, per heartbeat, after an
   * off-switch task has been assigned.
   * 
   * @param maxTasksToAssignAfterOffSwitch the maximum number of tasks to 
   *                                       schedule, per heartbeat, after an
   *                                       off-switch task has been assigned
   */
  public void setMaxTasksToAssignAfterOffSwitch(
      int maxTasksToAssignAfterOffSwitch) {
    rmConf.setInt(
        "mapred.capacity-scheduler.maximum-tasks-after-offswitch", 
        maxTasksToAssignAfterOffSwitch);
  }
}
