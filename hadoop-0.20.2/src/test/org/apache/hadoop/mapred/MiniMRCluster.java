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

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class creates a single-process Map-Reduce cluster for junit testing.
 * One thread is created for each server.
 */
public class MiniMRCluster {
  private static final Log LOG = LogFactory.getLog(MiniMRCluster.class);
    
  private Thread jobTrackerThread;
  private JobTrackerRunner jobTracker;
    
  private int jobTrackerPort = 0;
  private int taskTrackerPort = 0;
  private int jobTrackerInfoPort = 0;
  private int numTaskTrackers;
    
  private List<TaskTrackerRunner> taskTrackerList = new ArrayList<TaskTrackerRunner>();
  private List<Thread> taskTrackerThreadList = new ArrayList<Thread>();
    
  private String namenode;
  private UserGroupInformation ugi = null;
  private JobConf conf;
  private int numTrackerToExclude;
    
  private JobConf job;
  
  /**
   * An inner class that runs a job tracker.
   */
  public class JobTrackerRunner implements Runnable {
    private JobTracker tracker = null;
    private volatile boolean isActive = true;
    
    JobConf jc = null;
        
    public JobTrackerRunner(JobConf conf) {
      jc = conf;
    }

    public boolean isUp() {
      return (tracker != null);
    }
        
    public boolean isActive() {
      return isActive;
    }

    public int getJobTrackerPort() {
      return tracker.getTrackerPort();
    }

    public int getJobTrackerInfoPort() {
      return tracker.getInfoPort();
    }
  
    public JobTracker getJobTracker() {
      return tracker;
    }
    
    /**
     * Create the job tracker and run it.
     */
    public void run() {
      try {
        jc = (jc == null) ? createJobConf() : createJobConf(jc);
        File f = new File("build/test/mapred/local").getAbsoluteFile();
        jc.set("mapred.local.dir",f.getAbsolutePath());
        jc.setClass("topology.node.switch.mapping.impl", 
            StaticMapping.class, DNSToSwitchMapping.class);
        final String id =
          new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
        if (ugi == null) {
          ugi = UserGroupInformation.getCurrentUser();
        }
        tracker = ugi.doAs(new PrivilegedExceptionAction<JobTracker>() {
          public JobTracker run() throws InterruptedException, IOException {
            return JobTracker.startTracker(jc, id);
          }
        });
        tracker.offerService();
      } catch (Throwable e) {
        LOG.error("Job tracker crashed", e);
        isActive = false;
      }
    }
        
    /**
     * Shutdown the job tracker and wait for it to finish.
     */
    public void shutdown() {
      try {
        if (tracker != null) {
          tracker.stopTracker();
        }
      } catch (Throwable e) {
        LOG.error("Problem shutting down job tracker", e);
      }
      isActive = false;
    }
  }
    
  /**
   * An inner class to run the task tracker.
   */
  class TaskTrackerRunner implements Runnable {
    volatile TaskTracker tt;
    int trackerId;
    // the localDirs for this taskTracker
    String[] localDirs;
    volatile boolean isInitialized = false;
    volatile boolean isDead = false;
    volatile boolean exited = false;
    int numDir;

    TaskTrackerRunner(int trackerId, int numDir, String hostname, 
                                    JobConf cfg) 
    throws IOException {
      this.trackerId = trackerId;
      this.numDir = numDir;
      localDirs = new String[numDir];
      final JobConf conf;
      if (cfg == null) {
        conf = createJobConf();
      } else {
        conf = createJobConf(cfg);
      }
      if (hostname != null) {
        conf.set("slave.host.name", hostname);
      }
      conf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
      conf.set("mapred.task.tracker.report.address", 
                "127.0.0.1:" + taskTrackerPort);
      File localDirBase = 
        new File(conf.get("mapred.local.dir")).getAbsoluteFile();
      localDirBase.mkdirs();
      StringBuffer localPath = new StringBuffer();
      for(int i=0; i < numDir; ++i) {
        File ttDir = new File(localDirBase, 
                              Integer.toString(trackerId) + "_" + i);
        if (!ttDir.mkdirs()) {
          if (!ttDir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + ttDir);
          }
        }
        localDirs[i] = ttDir.toString();
        if (i != 0) {
          localPath.append(",");
        }
        localPath.append(localDirs[i]);
      }
      conf.set("mapred.local.dir", localPath.toString());
      LOG.info("mapred.local.dir is " +  localPath);
      try {
        tt = ugi.doAs(new PrivilegedExceptionAction<TaskTracker>() {
          public TaskTracker run() throws InterruptedException, IOException {
            return createTaskTracker(conf);
          }
        });

        isInitialized = true;
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        LOG.error("task tracker " + trackerId + " crashed", e);
      }
    }
     
    /**
     * Creates a default {@link TaskTracker} using the conf passed. 
     */
    TaskTracker createTaskTracker(JobConf conf)
        throws IOException, InterruptedException {
      return new TaskTracker(conf);
    }
    
    /**
     * Create and run the task tracker.
     */
    public void run() {
      try {
        if (tt != null) {
          tt.run();
        }
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        LOG.error("task tracker " + trackerId + " crashed", e);
      }
      exited = true;
    }
 
    /**
     * Get the local dir for this TaskTracker.
     * This is there so that we do not break
     * previous tests. 
     * @return the absolute pathname
     */
    public String getLocalDir() {
      return localDirs[0];
    }
       
    public String[] getLocalDirs(){
      return localDirs;
    } 
    
    public TaskTracker getTaskTracker() {
      return tt;
    }
    
    /**
     * Shut down the server and wait for it to finish.
     */
    public void shutdown() {
      if (tt != null) {
        try {
          tt.shutdown();
        } catch (Throwable e) {
          LOG.error("task tracker " + trackerId + " could not shut down",
                    e);
        }
      }
    }
  }
    
  /**
   * Get the local directory for the Nth task tracker
   * @param taskTracker the index of the task tracker to check
   * @return the absolute pathname of the local dir
   */
  public String getTaskTrackerLocalDir(int taskTracker) {
    return (taskTrackerList.get(taskTracker)).getLocalDir();
  }

  /**
   * Get all the local directories for the Nth task tracker
   * @param taskTracker the index of the task tracker to check
   * @return array of local dirs
   */
  public String[] getTaskTrackerLocalDirs(int taskTracker) {
    return (taskTrackerList.get(taskTracker)).getLocalDirs();
  }

  public JobTrackerRunner getJobTrackerRunner() {
    return jobTracker;
  }
  
  TaskTrackerRunner getTaskTrackerRunner(int id) {
    return taskTrackerList.get(id);
  }
  /**
   * Get the number of task trackers in the cluster
   */
  public int getNumTaskTrackers() {
    return taskTrackerList.size();
  }
  
  /**
   * Sets inline cleanup threads to all task trackers sothat deletion of
   * temporary files/dirs happen inline
   */
  public void setInlineCleanupThreads() {
    for (int i = 0; i < getNumTaskTrackers(); i++) {
      getTaskTrackerRunner(i).getTaskTracker().setCleanupThread(
          new UtilsForTests.InlineCleanupQueue());
    }
  }

  /**
   * Wait until the system is idle.
   */
  public void waitUntilIdle() {
    waitTaskTrackers();
    
    JobClient client;
    try {
      client = new JobClient(job);
      ClusterStatus status = client.getClusterStatus();
      while(status.getTaskTrackers() + numTrackerToExclude 
            < taskTrackerList.size()) {
        for(TaskTrackerRunner runner : taskTrackerList) {
          if(runner.isDead) {
            throw new RuntimeException("TaskTracker is dead");
          }
        }
        Thread.sleep(1000);
        status = client.getClusterStatus();
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    
  }

  private void waitTaskTrackers() {
    for(Iterator<TaskTrackerRunner> itr= taskTrackerList.iterator(); itr.hasNext();) {
      TaskTrackerRunner runner = itr.next();
      while (!runner.isDead && (!runner.isInitialized || !runner.tt.isIdle())) {
        if (!runner.isInitialized) {
          LOG.info("Waiting for task tracker to start.");
        } else {
          LOG.info("Waiting for task tracker " + runner.tt.getName() +
                   " to be idle.");
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
  }
  
  /** 
   * Get the actual rpc port used.
   */
  public int getJobTrackerPort() {
    return jobTrackerPort;
  }

  public JobConf createJobConf() {
    return createJobConf(new JobConf());
  }

  public JobConf createJobConf(JobConf conf) {
    if(conf == null) {
      conf = new JobConf();
    }
    return configureJobConf(conf, namenode, jobTrackerPort, jobTrackerInfoPort, 
                            ugi);
  }
  
  static JobConf configureJobConf(JobConf conf, String namenode, 
                                  int jobTrackerPort, int jobTrackerInfoPort, 
                                  UserGroupInformation ugi) {
    JobConf result = new JobConf(conf);
    FileSystem.setDefaultUri(result, namenode);
    result.set("mapred.job.tracker", "localhost:"+jobTrackerPort);
    result.set("mapred.job.tracker.http.address", 
                        "127.0.0.1:" + jobTrackerInfoPort);
    // for debugging have all task output sent to the test output
    JobClient.setTaskOutputFilter(result, JobClient.TaskStatusFilter.ALL);
    return result;
  }

  /**
   * Create the config and the cluster.
   * @param numTaskTrackers no. of tasktrackers in the cluster
   * @param namenode the namenode
   * @param numDir no. of directories
   * @throws IOException
   */
  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir, 
      String[] racks, String[] hosts) throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts);
  }
  
  /**
   * Create the config and the cluster.
   * @param numTaskTrackers no. of tasktrackers in the cluster
   * @param namenode the namenode
   * @param numDir no. of directories
   * @param racks Array of racks
   * @param hosts Array of hosts in the corresponding racks
   * @param conf Default conf for the jobtracker
   * @throws IOException
   */
  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir, 
                       String[] racks, String[] hosts, JobConf conf) 
  throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts, null, conf);
  }

  /**
   * Create the config and the cluster.
   * @param numTaskTrackers no. of tasktrackers in the cluster
   * @param namenode the namenode
   * @param numDir no. of directories
   * @throws IOException
   */
  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir) 
    throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir);
  }
    
  public MiniMRCluster(int jobTrackerPort,
      int taskTrackerPort,
      int numTaskTrackers,
      String namenode,
      int numDir)
  throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, null);
  }
  
  public MiniMRCluster(int jobTrackerPort,
      int taskTrackerPort,
      int numTaskTrackers,
      String namenode,
      int numDir,
      String[] racks) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, racks, null);
  }
  
  public MiniMRCluster(int jobTrackerPort,
                       int taskTrackerPort,
                       int numTaskTrackers,
                       String namenode,
                       int numDir,
                       String[] racks, String[] hosts) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, racks, hosts, null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, 
      int numDir, String[] racks, String[] hosts, UserGroupInformation ugi
      ) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, racks, hosts, ugi, null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, 
      int numDir, String[] racks, String[] hosts, UserGroupInformation ugi,
      JobConf conf) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir, 
         racks, hosts, ugi, conf, 0);
  }
  
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, 
      int numDir, String[] racks, String[] hosts, UserGroupInformation ugi,
      JobConf conf, int numTrackerToExclude) throws IOException {
    if (racks != null && racks.length < numTaskTrackers) {
      LOG.error("Invalid number of racks specified. It should be at least " +
          "equal to the number of tasktrackers");
      shutdown();
    }
    if (hosts != null && numTaskTrackers > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + hosts.length
          + "] is less than the number of tasktrackers [" + numTaskTrackers + "].");
    }
     
     //Generate rack names if required
     if (racks == null) {
       System.out.println("Generating rack names for tasktrackers");
       racks = new String[numTaskTrackers];
       for (int i=0; i < racks.length; ++i) {
         racks[i] = NetworkTopology.DEFAULT_RACK;
       }
     }
     
    //Generate some hostnames if required
    if (hosts == null) {
      System.out.println("Generating host names for tasktrackers");
      hosts = new String[numTaskTrackers];
      for (int i = 0; i < numTaskTrackers; i++) {
        hosts[i] = "host" + i + ".foo.com";
      }
    }
    this.jobTrackerPort = jobTrackerPort;
    this.taskTrackerPort = taskTrackerPort;
    this.jobTrackerInfoPort = 0;
    this.numTaskTrackers = 0;
    this.namenode = namenode;
    this.ugi = ugi;
    this.conf = conf; // this is the conf the mr starts with
    this.numTrackerToExclude = numTrackerToExclude;

    // start the jobtracker
    startJobTracker();

    // Create the TaskTrackers
    for (int idx = 0; idx < numTaskTrackers; idx++) {
      String rack = null;
      String host = null;
      if (racks != null) {
        rack = racks[idx];
      }
      if (hosts != null) {
        host = hosts[idx];
      }
      
      startTaskTracker(host, rack, idx, numDir);
    }

    this.job = createJobConf(conf);
    waitUntilIdle();
  }
   
  public UserGroupInformation getUgi() {
    return ugi;
  }
    
  /**
   * Get the task completion events
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID id, int from, 
                                                          int max) 
  throws IOException {
    return jobTracker.getJobTracker().getTaskCompletionEvents(id, from, max);
  }

  /**
   * Change the job's priority
   * 
   * @throws IOException
   * @throws AccessControlException
   */
  public void setJobPriority(JobID jobId, JobPriority priority)
      throws AccessControlException, IOException {
    jobTracker.getJobTracker().setJobPriority(jobId, priority);
  }

  /**
   * Get the job's priority
   */
  public JobPriority getJobPriority(JobID jobId) {
    return jobTracker.getJobTracker().getJob(jobId).getPriority();
  }

  /**
   * Get the job finish time
   */
  public long getJobFinishTime(JobID jobId) {
    return jobTracker.getJobTracker().getJob(jobId).getFinishTime();
  }

  /**
   * Init the job
   */
  public void initializeJob(JobID jobId) throws IOException {
    JobInProgress job = jobTracker.getJobTracker().getJob(jobId);
    jobTracker.getJobTracker().initJob(job);
  }
  
  /**
   * Get the events list at the tasktracker
   */
  public MapTaskCompletionEventsUpdate 
         getMapTaskCompletionEventsUpdates(int index, JobID jobId, int max) 
  throws IOException {
    String jtId = jobTracker.getJobTracker().getTrackerIdentifier();
    TaskAttemptID dummy = 
      new TaskAttemptID(jtId, jobId.getId(), false, 0, 0);
    return taskTrackerList.get(index).getTaskTracker()
                                     .getMapCompletionEvents(jobId, 0, max, 
                                                             dummy, null);
  }
  
  /**
   * Get jobtracker conf
   */
  public JobConf getJobTrackerConf() {
    return this.conf;
  }
  
  /**
   * Get num events recovered
   */
  public int getNumEventsRecovered() {
    return jobTracker.getJobTracker().recoveryManager.totalEventsRecovered();
  }

  public int getFaultCount(String hostName) {
    return jobTracker.getJobTracker().getFaultCount(hostName);
  }
  
  /**
   * Start the jobtracker.
   */
  public void startJobTracker() {
    startJobTracker(true);
  }
  
  void startJobTracker(boolean wait) {
    //  Create the JobTracker
    jobTracker = new JobTrackerRunner(conf);
    jobTrackerThread = new Thread(jobTracker);
        
    jobTrackerThread.start();
    
    if (!wait) {
    	return;
    }
    
    while (jobTracker.isActive() && !jobTracker.isUp()) {
      try {                                     // let daemons get started
        Thread.sleep(1000);
      } catch(InterruptedException e) {
      }
    }
        
    // is the jobtracker has started then wait for it to init
    ClusterStatus status = null;
    if (jobTracker.isUp()) {
      status = jobTracker.getJobTracker().getClusterStatus(false);
      while (jobTracker.isActive() && status.getJobTrackerState() 
             == JobTracker.State.INITIALIZING) {
        try {
          LOG.info("JobTracker still initializing. Waiting.");
          Thread.sleep(1000);
        } catch(InterruptedException e) {}
        status = jobTracker.getJobTracker().getClusterStatus(false);
      }
    }

    if (!jobTracker.isActive()) {
      // return if jobtracker has crashed
      return;
    }
 
    // Set the configuration for the task-trackers
    this.jobTrackerPort = jobTracker.getJobTrackerPort();
    this.jobTrackerInfoPort = jobTracker.getJobTrackerInfoPort();
  }

  /**
   * Kill the jobtracker.
   */
  public void stopJobTracker() {
    //jobTracker.exit(-1);
    jobTracker.shutdown();

    jobTrackerThread.interrupt();
    try {
      jobTrackerThread.join();
    } catch (InterruptedException ex) {
      LOG.error("Problem waiting for job tracker to finish", ex);
    }
  }

  /**
   * Kill the tasktracker.
   */
  public void stopTaskTracker(int id) {
    TaskTrackerRunner tracker = taskTrackerList.remove(id);
    tracker.shutdown();

    Thread thread = taskTrackerThreadList.remove(id);
    thread.interrupt();
    
    try {
      thread.join();
      // This will break the wait until idle loop
      tracker.isDead = true;
      --numTaskTrackers;
    } catch (InterruptedException ex) {
      LOG.error("Problem waiting for task tracker to finish", ex);
    }
  }
  
  /**
   * Start the tasktracker.
   */
  public void startTaskTracker(String host, String rack, int idx, int numDir) 
  throws IOException {
    if (rack != null) {
      StaticMapping.addNodeToRack(host, rack);
    }
    if (host != null) {
      NetUtils.addStaticResolution(host, "localhost");
    }
    TaskTrackerRunner taskTracker;
    taskTracker = new TaskTrackerRunner(idx, numDir, host, conf);
    
    addTaskTracker(taskTracker);
  }
  
  /**
   * Add a tasktracker to the Mini-MR cluster.
   */
  void addTaskTracker(TaskTrackerRunner taskTracker) {
    Thread taskTrackerThread = new Thread(taskTracker);
    taskTrackerList.add(taskTracker);
    taskTrackerThreadList.add(taskTrackerThread);
    taskTrackerThread.start();
    ++numTaskTrackers;
  }
  
  /**
   * Get the tasktrackerID in MiniMRCluster with given trackerName.
   */
  int getTaskTrackerID(String trackerName) {
    for (int id=0; id < numTaskTrackers; id++) {
      if (taskTrackerList.get(id).getTaskTracker().getName().equals(
          trackerName)) {
        return id;
      }
    }
    return -1;
  }
  
  /**
   * Shut down the servers.
   */
  public void shutdown() {
    try {
      waitTaskTrackers();
      for (int idx = 0; idx < numTaskTrackers; idx++) {
        TaskTrackerRunner taskTracker = taskTrackerList.get(idx);
        Thread taskTrackerThread = taskTrackerThreadList.get(idx);
        taskTracker.shutdown();
        taskTrackerThread.interrupt();
        try {
          taskTrackerThread.join();
        } catch (InterruptedException ex) {
          LOG.error("Problem shutting down task tracker", ex);
        }
      }
      stopJobTracker();
    } finally {
      File configDir = new File("build", "minimr");
      File siteFile = new File(configDir, "mapred-site.xml");
      siteFile.delete();
    }
  }
    
  public static void main(String[] args) throws IOException {
    LOG.info("Bringing up Jobtracker and tasktrackers.");
    MiniMRCluster mr = new MiniMRCluster(4, "file:///", 1);
    LOG.info("JobTracker and TaskTrackers are up.");
    mr.shutdown();
    LOG.info("JobTracker and TaskTrackers brought down.");
  }
}

