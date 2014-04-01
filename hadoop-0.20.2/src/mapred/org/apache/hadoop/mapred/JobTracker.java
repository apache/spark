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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobSubmissionProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.mapred.AuditLogger.Constants;
import org.apache.hadoop.mapred.Counters.CountersExceededException;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.Listener;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapred.JobInProgress.KillInterruptedException;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.MRAsyncDiskService;
import org.apache.hadoop.util.PluginDispatcher;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.token.DelegationTokenRenewal;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.mortbay.util.ajax.JSON;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 *******************************************************/
public class JobTracker implements MRConstants, InterTrackerProtocol,
    JobSubmissionProtocol, TaskTrackerManager, RefreshUserMappingsProtocol,
    RefreshAuthorizationPolicyProtocol, AdminOperationsProtocol,
    JobTrackerMXBean, GetUserMappingsProtocol {

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  static long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  static long RETIRE_JOB_INTERVAL;
  static long RETIRE_JOB_CHECK_INTERVAL;
  
  private final long DELEGATION_TOKEN_GC_INTERVAL = 3600000; // 1 hour
  private final DelegationTokenSecretManager secretManager;

  
  // The interval after which one fault of a tracker will be discarded,
  // if there are no faults during this. 
  private static long UPDATE_FAULTY_TRACKER_INTERVAL = 24 * 60 * 60 * 1000;
  // The maximum percentage of trackers in cluster added 
  // to the 'blacklist' across all the jobs.
  private static double MAX_BLACKLIST_PERCENT = 0.50;
  // A tracker is blacklisted across jobs only if number of 
  // blacklists are X% above the average number of blacklists.
  // X is the blacklist threshold here.
  private double AVERAGE_BLACKLIST_THRESHOLD = 0.50;
  // The maximum number of blacklists for a tracker after which the 
  // tracker could be blacklisted across all jobs
  private int MAX_BLACKLISTS_PER_TRACKER = 4;
  

  /** the maximum allowed size of the jobconf **/
  long MAX_JOBCONF_SIZE = 5*1024*1024L;
  /** the config key for max user jobconf size **/
  public static final String MAX_USER_JOBCONF_SIZE_KEY = 
      "mapred.user.jobconf.limit";

  // Delegation token related keys
  public static final String  DELEGATION_KEY_UPDATE_INTERVAL_KEY =  
    "mapreduce.cluster.delegation.key.update-interval";
  public static final long    DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =  
    24*60*60*1000; // 1 day
  public static final String  DELEGATION_TOKEN_RENEW_INTERVAL_KEY =  
    "mapreduce.cluster.delegation.token.renew-interval";
  public static final long    DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =  
    24*60*60*1000;  // 1 day
  public static final String  DELEGATION_TOKEN_MAX_LIFETIME_KEY =  
    "mapreduce.cluster.delegation.token.max-lifetime";
  public static final long    DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =  
    7*24*60*60*1000; // 7 days
  
  // Approximate number of heartbeats that could arrive JobTracker
  // in a second
  static final String JT_HEARTBEATS_IN_SECOND = "mapred.heartbeats.in.second";
  private int NUM_HEARTBEATS_IN_SECOND;
  private static final int DEFAULT_NUM_HEARTBEATS_IN_SECOND = 100;
  // Minimum time between heartbeats
  static final String JT_HEARTBEAT_INTERVAL_MIN = "mapreduce.jobtracker.heartbeat.interval.min";
  private int HEARTBEAT_INTERVAL_MIN;

  private static final int MIN_NUM_HEARTBEATS_IN_SECOND = 1;
  
  // Scaling factor for heartbeats, used for testing only
  static final String JT_HEARTBEATS_SCALING_FACTOR = 
    "mapreduce.jobtracker.heartbeats.scaling.factor";
  private float HEARTBEATS_SCALING_FACTOR;
  private final float MIN_HEARTBEATS_SCALING_FACTOR = 0.01f;
  private final float DEFAULT_HEARTBEATS_SCALING_FACTOR = 1.0f;
  
  public static enum State { INITIALIZING, RUNNING }
  State state = State.INITIALIZING;
  private static final int FS_ACCESS_RETRY_PERIOD = 10000;
  static final String JOB_INFO_FILE = "job-info";
  private DNSToSwitchMapping dnsToSwitchMapping;
  private NetworkTopology clusterMap = new NetworkTopology();
  private int numTaskCacheLevels; // the max level to which we cache tasks
  /**
   * {@link #nodesAtMaxLevel} is using the keySet from {@link ConcurrentHashMap}
   * so that it can be safely written to and iterated on via 2 separate threads.
   * Note: It can only be iterated from a single thread which is feasible since
   *       the only iteration is done in {@link JobInProgress} under the 
   *       {@link JobTracker} lock.
   */
  private Set<Node> nodesAtMaxLevel = 
    Collections.newSetFromMap(new ConcurrentHashMap<Node, Boolean>());
  private final TaskScheduler taskScheduler;
  private final List<JobInProgressListener> jobInProgressListeners =
    new CopyOnWriteArrayList<JobInProgressListener>();

  private static final LocalDirAllocator lDirAlloc = 
                              new LocalDirAllocator("mapred.local.dir");
  //system directory is completely owned by the JobTracker
  final static FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------

  // system files should have 700 permission
  final static FsPermission SYSTEM_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------
  
  private Clock clock;

  private MRAsyncDiskService asyncDiskService;
  
  private final JobTokenSecretManager jobTokenSecretManager
    = new JobTokenSecretManager();

  JobTokenSecretManager getJobTokenSecretManager() {
    return jobTokenSecretManager;
  }

  /**
   * A client tried to submit a job before the Job Tracker was ready.
   */
  public static class IllegalStateException extends IOException {
 
    private static final long serialVersionUID = 1L;

    public IllegalStateException(String msg) {
      super(msg);
    }
  }

  /**
   * The maximum no. of 'completed' (successful/failed/killed)
   * jobs kept in memory per-user. 
   */
  final int MAX_COMPLETE_USER_JOBS_IN_MEMORY;

   /**
    * The minimum time (in ms) that a job's information has to remain
    * in the JobTracker's memory before it is retired.
    */
  static final int MIN_TIME_BEFORE_RETIRE = 0;


  private int nextJobId = 1;

  public static final Log LOG = LogFactory.getLog(JobTracker.class);
  
  static final String CONF_VERSION_KEY = "mapreduce.jobtracker.conf.version";
  static final String CONF_VERSION_DEFAULT = "default";

  private PluginDispatcher<JobTrackerPlugin> pluginDispatcher;

  public Clock getClock() {
    return clock;
  }
    
  /**
   * Start the JobTracker with given configuration.
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the JobTracker is up and running if the user passes the port as
   * <code>zero</code>.
   *   
   * @param conf configuration for the JobTracker.
   * @throws IOException
   */
  public static JobTracker startTracker(JobConf conf
                                        ) throws IOException,
                                                 InterruptedException {
    return startTracker(conf, generateNewIdentifier());
  }
  
  public static JobTracker startTracker(JobConf conf, String identifier) 
  throws IOException, InterruptedException {
    JobTracker result = null;
    while (true) {
      try {
        result = new JobTracker(conf, identifier);
        result.taskScheduler.setTaskTrackerManager(result);
        break;
      } catch (VersionMismatch e) {
        throw e;
      } catch (BindException e) {
        throw e;
      } catch (UnknownHostException e) {
        throw e;
      } catch (AccessControlException ace) {
        // in case of jobtracker not having right access
        // bail out
        throw ace;
      } catch (IOException e) {
        LOG.warn("Error starting tracker: " + 
                 StringUtils.stringifyException(e));
      }
      Thread.sleep(1000);
    }
    if (result != null) {
      JobEndNotifier.startNotifier();
      MBeanUtil.registerMBean("JobTracker", "JobTrackerInfo", result);
    }    
    return result;
  }

  public void stopTracker() throws IOException {
    JobEndNotifier.stopNotifier();
    close();
  }
    
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())){
      return JobSubmissionProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(AdminOperationsProtocol.class.getName())){
      return AdminOperationsProtocol.versionID;
    } else if (protocol.equals(RefreshUserMappingsProtocol.class.getName())){
      return RefreshUserMappingsProtocol.versionID;
    } else if (protocol.equals(GetUserMappingsProtocol.class.getName())) {
      return GetUserMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to job tracker: " + protocol);
    }
  }
  
  public DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return secretManager;
  }
  
  /**
   * A thread to timeout tasks that have been assigned to task trackers,
   * but that haven't reported back yet.
   * Note that I included a stop() method, even though there is no place
   * where JobTrackers are cleaned up.
   */
  private class ExpireLaunchingTasks implements Runnable {
    /**
     * This is a map of the tasks that have been assigned to task trackers,
     * but that have not yet been seen in a status report.
     * map: task-id -> time-assigned 
     */
    private Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();
      
    public void run() {
      while (true) {
        try {
          // Every 3 minutes check for any tasks that are overdue
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
          long now = clock.getTime();
          if(LOG.isDebugEnabled()) {
            LOG.debug("Starting launching task sweep");
          }
          synchronized (JobTracker.this) {
            synchronized (launchingTasks) {
              Iterator<Map.Entry<TaskAttemptID, Long>> itr =
                launchingTasks.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<TaskAttemptID, Long> pair = itr.next();
                TaskAttemptID taskId = pair.getKey();
                long age = now - (pair.getValue()).longValue();
                LOG.info(taskId + " is " + age + " ms debug.");
                if (age > TASKTRACKER_EXPIRY_INTERVAL) {
                  LOG.info("Launching task " + taskId + " timed out.");
                  TaskInProgress tip = null;
                  tip = taskidToTIPMap.get(taskId);
                  if (tip != null) {
                    JobInProgress job = tip.getJob();
                    String trackerName = getAssignedTracker(taskId);
                    TaskTrackerStatus trackerStatus = 
                      getTaskTrackerStatus(trackerName); 
                      
                    // This might happen when the tasktracker has already
                    // expired and this thread tries to call failedtask
                    // again. expire tasktracker should have called failed
                    // task!
                    if (trackerStatus != null)
                      job.failedTask(tip, taskId, "Error launching task", 
                                     tip.isMapTask()? TaskStatus.Phase.MAP:
                                     TaskStatus.Phase.STARTING,
                                     TaskStatus.State.FAILED,
                                     trackerName);
                  }
                  itr.remove();
                } else {
                  // the tasks are sorted by start time, so once we find
                  // one that we want to keep, we are done for this cycle.
                  break;
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          // all done
          break;
        } catch (Exception e) {
          LOG.error("Expire Launching Task Thread got exception: " +
                    StringUtils.stringifyException(e));
        }
      }
    }
      
    public void addNewTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.put(taskName, 
                           clock.getTime());
      }
    }
      
    public void removeTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.remove(taskName);
      }
    }
  }
    
  ///////////////////////////////////////////////////////
  // Used to expire TaskTrackers that have gone down
  ///////////////////////////////////////////////////////
  class ExpireTrackers implements Runnable {
    public ExpireTrackers() {
    }
    /**
     * The run method lives for the life of the JobTracker, and removes TaskTrackers
     * that have not checked in for some time.
     */
    public void run() {
      while (true) {
        try {
          //
          // Thread runs periodically to check whether trackers should be expired.
          // The sleep interval must be no more than half the maximum expiry time
          // for a task tracker.
          //
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);

          //
          // Loop through all expired items in the queue
          //
          // Need to lock the JobTracker here since we are
          // manipulating it's data-structures via
          // ExpireTrackers.run -> JobTracker.lostTaskTracker ->
          // JobInProgress.failedTask -> JobTracker.markCompleteTaskAttempt
          // Also need to lock JobTracker before locking 'taskTracker' &
          // 'trackerExpiryQueue' to prevent deadlock:
          // @see {@link JobTracker.processHeartbeat(TaskTrackerStatus, boolean)} 
          synchronized (JobTracker.this) {
            synchronized (taskTrackers) {
              synchronized (trackerExpiryQueue) {
                long now = clock.getTime();
                TaskTrackerStatus leastRecent = null;
                while ((trackerExpiryQueue.size() > 0) &&
                       (leastRecent = trackerExpiryQueue.first()) != null &&
                       ((now - leastRecent.getLastSeen()) > TASKTRACKER_EXPIRY_INTERVAL)) {

                        
                  // Remove profile from head of queue
                  trackerExpiryQueue.remove(leastRecent);
                  String trackerName = leastRecent.getTrackerName();
                        
                  // Figure out if last-seen time should be updated, or if tracker is dead
                  TaskTracker current = getTaskTracker(trackerName);
                  TaskTrackerStatus newProfile = 
                    (current == null ) ? null : current.getStatus();
                  // Items might leave the taskTracker set through other means; the
                  // status stored in 'taskTrackers' might be null, which means the
                  // tracker has already been destroyed.
                  if (newProfile != null) {
                    if ((now - newProfile.getLastSeen()) > TASKTRACKER_EXPIRY_INTERVAL) {
                      removeTracker(current);
                      // remove the mapping from the hosts list
                      String hostname = newProfile.getHost();
                      hostnameToTaskTracker.get(hostname).remove(trackerName);
                    } else {
                      // Update time by inserting latest profile
                      trackerExpiryQueue.add(newProfile);
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException iex) {
          break;
        } catch (Exception t) {
          LOG.error("Tracker Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
        
  }

  synchronized void historyFileCopied(JobID jobid, String historyFile) {
    JobInProgress job = getJob(jobid);
    if (job != null) { //found in main cache
      job.setHistoryFileCopied();
      if (historyFile != null) {
        job.setHistoryFile(historyFile);
      }
      return;
    }
    RetireJobInfo jobInfo = retireJobs.get(jobid);
    if (jobInfo != null) { //found in retired cache
      if (historyFile != null) {
        jobInfo.setHistoryFile(historyFile);
      }
    }
  }

  static class RetireJobInfo {
    final JobStatus status;
    final JobProfile profile;
    final long finishTime;
    final Counters counters;
    private String historyFile;
    RetireJobInfo(Counters counters, JobStatus status, JobProfile profile, 
        long finishTime, String historyFile) {
      this.counters = counters;
      this.status = status;
      this.profile = profile;
      this.finishTime = finishTime;
      this.historyFile = historyFile;
    }
    void setHistoryFile(String file) {
      this.historyFile = file;
    }
    String getHistoryFile() {
      return historyFile;
    }
  }
  ///////////////////////////////////////////////////////
  // Used to remove old finished Jobs that have been around for too long
  ///////////////////////////////////////////////////////
  class RetireJobs implements Runnable {
    private final Map<JobID, RetireJobInfo> jobIDStatusMap = 
      new HashMap<JobID, RetireJobInfo>();
    private final LinkedList<RetireJobInfo> jobRetireInfoQ = 
      new LinkedList<RetireJobInfo>();
    public RetireJobs() {
    }

    synchronized void addToCache(JobInProgress job) {
      Counters counters = new Counters();
      boolean isFine = job.getCounters(counters);
      counters = (isFine? counters: new Counters());
      RetireJobInfo info = new RetireJobInfo(counters, job.getStatus(),
          job.getProfile(), job.getFinishTime(), job.getHistoryFile());
      jobRetireInfoQ.add(info);
      jobIDStatusMap.put(info.status.getJobID(), info);
      if (jobRetireInfoQ.size() > retiredJobsCacheSize) {
        RetireJobInfo removed = jobRetireInfoQ.remove();
        jobIDStatusMap.remove(removed.status.getJobID());
        LOG.info("Retired job removed from cache " + removed.status.getJobID());
      }
    }

    synchronized RetireJobInfo get(JobID jobId) {
      return jobIDStatusMap.get(jobId);
    }

    @SuppressWarnings("unchecked")
    synchronized LinkedList<RetireJobInfo> getAll() {
      return (LinkedList<RetireJobInfo>) jobRetireInfoQ.clone();
    }

    synchronized LinkedList<JobStatus> getAllJobStatus() {
      LinkedList<JobStatus> list = new LinkedList<JobStatus>();
      for (RetireJobInfo info : jobRetireInfoQ) {
        list.add(info.status);
      }
      return list;
    }

    private boolean minConditionToRetire(JobInProgress job, long now) {
      return job.getStatus().getRunState() != JobStatus.RUNNING &&
          job.getStatus().getRunState() != JobStatus.PREP &&
          (job.getFinishTime() + MIN_TIME_BEFORE_RETIRE < now) &&
          job.isHistoryFileCopied();
    }
    /**
     * The run method lives for the life of the JobTracker,
     * and removes Jobs that are not still running, but which
     * finished a long time ago.
     */
    public void run() {
      while (true) {
        try {
          Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
          List<JobInProgress> retiredJobs = new ArrayList<JobInProgress>();
          long now = clock.getTime();
          long retireBefore = now - RETIRE_JOB_INTERVAL;

          synchronized (jobs) {
            for(JobInProgress job: jobs.values()) {
              if (minConditionToRetire(job, now) &&
                  (job.getFinishTime()  < retireBefore)) {
                retiredJobs.add(job);
              }
            }
          }
          synchronized (userToJobsMap) {
            Iterator<Map.Entry<String, ArrayList<JobInProgress>>> 
                userToJobsMapIt = userToJobsMap.entrySet().iterator();
            while (userToJobsMapIt.hasNext()) {
              Map.Entry<String, ArrayList<JobInProgress>> entry = 
                userToJobsMapIt.next();
              ArrayList<JobInProgress> userJobs = entry.getValue();
              Iterator<JobInProgress> it = userJobs.iterator();
              while (it.hasNext() && 
                  userJobs.size() > MAX_COMPLETE_USER_JOBS_IN_MEMORY) {
                JobInProgress jobUser = it.next();
                if (retiredJobs.contains(jobUser)) {
                  LOG.info("Removing from userToJobsMap: " + 
                      jobUser.getJobID());
                  it.remove();
                } else if (minConditionToRetire(jobUser, now)) {
                  LOG.info("User limit exceeded. Marking job: " + 
                      jobUser.getJobID() + " for retire.");
                  retiredJobs.add(jobUser);
                  it.remove();
                }
              }
              if (userJobs.isEmpty()) {
                userToJobsMapIt.remove();
              }
            }
          }
          if (!retiredJobs.isEmpty()) {
            synchronized (JobTracker.this) {
              synchronized (jobs) {
                synchronized (taskScheduler) {
                  for (JobInProgress job: retiredJobs) {
                    removeJobTasks(job);
                    jobs.remove(job.getProfile().getJobID());
                    for (JobInProgressListener l : jobInProgressListeners) {
                      l.jobRemoved(job);
                    }
                    String jobUser = job.getProfile().getUser();
                    LOG.info("Retired job with id: '" + 
                             job.getProfile().getJobID() + "' of user '" +
                             jobUser + "'");

                    // clean up job files from the local disk
                    JobHistory.JobInfo.cleanupJob(job.getProfile().getJobID());
                    addToCache(job);
                  }
                }
              }
            }
          }
        } catch (InterruptedException t) {
          break;
        } catch (Throwable t) {
          LOG.error("Error in retiring job:\n" +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }
  
  enum ReasonForBlackListing {
    EXCEEDING_FAILURES,
    NODE_UNHEALTHY
  }
  
  // The FaultInfo which indicates the number of faults of a tracker
  // and when the last fault occurred
  // and whether the tracker is blacklisted across all jobs or not
  private static class FaultInfo {
    static final String FAULT_FORMAT_STRING =  "%d failures on the tracker";
    int numFaults = 0;
    long lastUpdated;
    boolean blacklisted; 
    
    private boolean isHealthy;
    private HashMap<ReasonForBlackListing, String>rfbMap;
    
    FaultInfo(long time) {
      numFaults = 0;
      lastUpdated = time;
      blacklisted = false;
      rfbMap = new  HashMap<ReasonForBlackListing, String>();
    }

    void setFaultCount(int num) {
      numFaults = num;
    }

    void setLastUpdated(long timeStamp) {
      lastUpdated = timeStamp;
    }

    int getFaultCount() {
      return numFaults;
    }

    long getLastUpdated() {
      return lastUpdated;
    }
    
    boolean isBlacklisted() {
      return blacklisted;
    }
    
    void setBlacklist(ReasonForBlackListing rfb, 
        String trackerFaultReport) {
      blacklisted = true;
      this.rfbMap.put(rfb, trackerFaultReport);
    }

    public void setHealthy(boolean isHealthy) {
      this.isHealthy = isHealthy;
    }

    public boolean isHealthy() {
      return isHealthy;
    }
    
    public String getTrackerFaultReport() {
      StringBuffer sb = new StringBuffer();
      for(String reasons : rfbMap.values()) {
        sb.append(reasons);
        sb.append("\n");
      }
      return sb.toString();
    }
    
    Set<ReasonForBlackListing> getReasonforblacklisting() {
      return this.rfbMap.keySet();
    }
    
    public void unBlacklist() {
      this.blacklisted = false;
      this.rfbMap.clear();
    }

    public boolean removeBlackListedReason(ReasonForBlackListing rfb) {
      String str = rfbMap.remove(rfb);
      return str!=null;
    }

    public void addBlackListedReason(ReasonForBlackListing rfb, String reason) {
      this.rfbMap.put(rfb, reason);
    }
    
  }

  private class FaultyTrackersInfo {
    // A map from hostName to its faults
    private Map<String, FaultInfo> potentiallyFaultyTrackers = 
              new HashMap<String, FaultInfo>();
    // This count gives the number of blacklisted trackers in the cluster 
    // at any time. This is maintained to avoid iteration over 
    // the potentiallyFaultyTrackers to get blacklisted trackers. And also
    // this count doesn't include blacklisted trackers which are lost, 
    // although the fault info is maintained for lost trackers.  
    private volatile int numBlacklistedTrackers = 0;

    /**
     * Increments faults(blacklist by job) for the tracker by one.
     * 
     * Adds the tracker to the potentially faulty list. 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName 
     */
    void incrementFaults(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = getFaultInfo(hostName, true);
        int numFaults = fi.getFaultCount();
        ++numFaults;
        fi.setFaultCount(numFaults);
        fi.setLastUpdated(clock.getTime());
        if (exceedsFaults(fi)) {
          LOG.info("Adding " + hostName + " to the blacklist"
              + " across all jobs");
          String reason = String.format(FaultInfo.FAULT_FORMAT_STRING,
              numFaults);
          blackListTracker(hostName, reason,
              ReasonForBlackListing.EXCEEDING_FAILURES);
        }
      }        
    }

    private void incrBlackListedTrackers(int count) {
      numBlacklistedTrackers += count;
      getInstrumentation().addBlackListedTrackers(count);
    }

    private void decrBlackListedTrackers(int count) {
      numBlacklistedTrackers -= count;
      getInstrumentation().decBlackListedTrackers(count);
    }

    private void blackListTracker(String hostName, String reason, ReasonForBlackListing rfb) {
      FaultInfo fi = getFaultInfo(hostName, true);
      boolean blackListed = fi.isBlacklisted();
      if(blackListed) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding blacklisted reason for tracker : " + hostName 
              + " Reason for blacklisting is : " + rfb);
        }
        if (!fi.getReasonforblacklisting().contains(rfb)) {
          LOG.info("Adding blacklisted reason for tracker : " + hostName
              + " Reason for blacklisting is : " + rfb);
        }
        fi.addBlackListedReason(rfb, reason);
      } else {
        LOG.info("Blacklisting tracker : " + hostName 
            + " Reason for blacklisting is : " + rfb);
        Set<TaskTracker> trackers = 
          hostnameToTaskTracker.get(hostName);
        synchronized (trackers) {
          for (TaskTracker tracker : trackers) {
            tracker.cancelAllReservations();
          }
        }
        removeHostCapacity(hostName);
        fi.setBlacklist(rfb, reason);
      }
    }
    
    private boolean canUnBlackListTracker(String hostName,
        ReasonForBlackListing rfb) {
      FaultInfo fi = getFaultInfo(hostName, false);
      if(fi == null) {
        return false;
      }
      
      Set<ReasonForBlackListing> rfbSet = fi.getReasonforblacklisting();
      return fi.isBlacklisted() && rfbSet.contains(rfb);
    }

    private void unBlackListTracker(String hostName,
        ReasonForBlackListing rfb) {
      // check if you can black list the tracker then call this methods
      FaultInfo fi = getFaultInfo(hostName, false);
      if(fi.removeBlackListedReason(rfb)) {
        if(fi.getReasonforblacklisting().isEmpty()) {
          addHostCapacity(hostName);
          LOG.info("Unblacklisting tracker : " + hostName);
          fi.unBlacklist();
          //We have unBlackListed tracker, so tracker should
          //definitely be healthy. Check fault count if fault count
          //is zero don't keep it memory.
          if(fi.numFaults == 0) {
            potentiallyFaultyTrackers.remove(hostName);
          }
        }
      }
    }
    
    // Assumes JobTracker is locked on the entry
    private FaultInfo getFaultInfo(String hostName, 
        boolean createIfNeccessary) {
      FaultInfo fi = null;
      synchronized (potentiallyFaultyTrackers) {
        fi = potentiallyFaultyTrackers.get(hostName);
        if (fi == null && createIfNeccessary) {
          fi = new FaultInfo(clock.getTime());
          potentiallyFaultyTrackers.put(hostName, fi);
        }
      }
      return fi;
    }
    
    /**
     * Blacklists the tracker across all jobs if
     * <ol>
     * <li>#faults are more than 
     *     MAX_BLACKLISTS_PER_TRACKER (configurable) blacklists</li>
     * <li>#faults is 50% (configurable) above the average #faults</li>
     * <li>50% the cluster is not blacklisted yet </li>
     * </ol>
     */
    private boolean exceedsFaults(FaultInfo fi) {
      int faultCount = fi.getFaultCount();
      if (faultCount >= MAX_BLACKLISTS_PER_TRACKER) {
        // calculate avgBlackLists
        long clusterSize = getClusterStatus().getTaskTrackers();
        long sum = 0;
        for (FaultInfo f : potentiallyFaultyTrackers.values()) {
          sum += f.getFaultCount();
        }
        double avg = (double) sum / clusterSize;
            
        long totalCluster = clusterSize + numBlacklistedTrackers;
        if ((faultCount - avg) > (AVERAGE_BLACKLIST_THRESHOLD * avg) &&
            numBlacklistedTrackers < (totalCluster * MAX_BLACKLIST_PERCENT)) {
          return true;
        }
      }
      return false;
    }
    
    /**
     * Removes the tracker from blacklist and
     * from potentially faulty list, when it is restarted.
     * 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName
     */
    void markTrackerHealthy(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = potentiallyFaultyTrackers.remove(hostName);
        if (fi != null && fi.isBlacklisted()) {
          LOG.info("Removing " + hostName + " from blacklist");
          addHostCapacity(hostName);
        }
      }
    }

    /**
     * Check whether tasks can be assigned to the tracker.
     *
     * One fault of the tracker is discarded if there
     * are no faults during one day. So, the tracker will get a 
     * chance again to run tasks of a job.
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName The tracker name
     * @param now The current time
     * 
     * @return true if the tracker is blacklisted 
     *         false otherwise
     */
    boolean shouldAssignTasksToTracker(String hostName, long now) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = potentiallyFaultyTrackers.get(hostName);
        if (fi != null &&
            (now - fi.getLastUpdated()) > UPDATE_FAULTY_TRACKER_INTERVAL) {
          int numFaults = fi.getFaultCount() - 1;
          fi.setFaultCount(numFaults);
          fi.setLastUpdated(now);
          if (canUnBlackListTracker(hostName, 
              ReasonForBlackListing.EXCEEDING_FAILURES)) {
            unBlackListTracker(hostName,
                ReasonForBlackListing.EXCEEDING_FAILURES);
          }
        }
        return (fi != null && fi.isBlacklisted());
      }
    }

    private void removeHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        // remove the capacity of trackers on this host
        int numTrackersOnHost = 0;
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          int mapSlots = status.getMaxMapSlots();
          totalMapTaskCapacity -= mapSlots;
          int reduceSlots = status.getMaxReduceSlots();
          totalReduceTaskCapacity -= reduceSlots;
          ++numTrackersOnHost;
          getInstrumentation().addBlackListedMapSlots(
              mapSlots);
          getInstrumentation().addBlackListedReduceSlots(
              reduceSlots);
        }
        uniqueHostsMap.remove(hostName);
        incrBlackListedTrackers(numTrackersOnHost);
      }
    }
    
    // This is called on tracker's restart or after a day of blacklist.
    private void addHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        int numTrackersOnHost = 0;
        // add the capacity of trackers on the host
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          int mapSlots = status.getMaxMapSlots();
          totalMapTaskCapacity += mapSlots;
          int reduceSlots = status.getMaxReduceSlots();
          totalReduceTaskCapacity += reduceSlots;
          numTrackersOnHost++;
          getInstrumentation().decBlackListedMapSlots(mapSlots);
          getInstrumentation().decBlackListedReduceSlots(reduceSlots);
        }
        uniqueHostsMap.put(hostName,
                           numTrackersOnHost);
        decrBlackListedTrackers(numTrackersOnHost);
      }
    }

    /**
     * Whether a host is blacklisted across all the jobs. 
     * 
     * Assumes JobTracker is locked on the entry.
     * @param hostName
     * @return
     */
    boolean isBlacklisted(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.isBlacklisted();
        }
      }
      return false;
    }
    
    // Assumes JobTracker is locked on the entry.
    int getFaultCount(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.getFaultCount();
        }
      }
      return 0;
    }
    
    // Assumes JobTracker is locked on the entry.
    Set<ReasonForBlackListing> getReasonForBlackListing(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.getReasonforblacklisting();
        }
      }
      return null;
    }


    // Assumes JobTracker is locked on the entry.
    void setNodeHealthStatus(String hostName, boolean isHealthy, String reason) {
      FaultInfo fi = null;
      // If tracker is not healthy, create a fault info object
      // blacklist it.
      if (!isHealthy) {
        fi = getFaultInfo(hostName, true);
        fi.setHealthy(isHealthy);
        synchronized (potentiallyFaultyTrackers) {
          blackListTracker(hostName, reason,
              ReasonForBlackListing.NODE_UNHEALTHY);
        }
      } else {
        fi = getFaultInfo(hostName, false);
        if (fi == null) {
          return;
        } else {
          if (canUnBlackListTracker(hostName,
              ReasonForBlackListing.NODE_UNHEALTHY)) {
            unBlackListTracker(hostName, ReasonForBlackListing.NODE_UNHEALTHY);
          }
        }
      }
    }
  }
  
  /**
   * Get all task tracker statuses on given host
   * 
   * Assumes JobTracker is locked on the entry
   * @param hostName
   * @return {@link java.util.List} of {@link TaskTrackerStatus}
   */
  private List<TaskTrackerStatus> getStatusesOnHost(String hostName) {
    List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus(); 
        if (hostName.equals(status.getHost())) {
          statuses.add(status);
        }
      }
    }
    return statuses;
  }
  
  ///////////////////////////////////////////////////////
  // Used to recover the jobs upon restart
  ///////////////////////////////////////////////////////
  class RecoveryManager {
    Set<JobID> jobsToRecover; // set of jobs to be recovered
    
    private int totalEventsRecovered = 0;
    private int restartCount = 0;
    private boolean shouldRecover = false;

    Set<String> recoveredTrackers = 
      Collections.synchronizedSet(new HashSet<String>());
    
    /** A custom listener that replays the events in the order in which the 
     * events (task attempts) occurred. 
     */
    class JobRecoveryListener implements Listener {
      // The owner job
      private JobInProgress jip;
      
      private JobHistory.JobInfo job; // current job's info object
      
      // Maintain the count of the (attempt) events recovered
      private int numEventsRecovered = 0;
      
      // Maintains open transactions
      private Map<String, String> hangingAttempts = 
        new HashMap<String, String>();
      
      // Whether there are any updates for this job
      private boolean hasUpdates = false;
      
      public JobRecoveryListener(JobInProgress jip) {
        this.jip = jip;
        this.job = new JobHistory.JobInfo(jip.getJobID().toString());
      }

      /**
       * Process a task. Note that a task might commit a previously pending 
       * transaction.
       */
      private void processTask(String taskId, JobHistory.Task task) {
        // Any TASK info commits the previous transaction
        boolean hasHanging = hangingAttempts.remove(taskId) != null;
        if (hasHanging) {
          numEventsRecovered += 2;
        }
        
        TaskID id = TaskID.forName(taskId);
        TaskInProgress tip = getTip(id);

        updateTip(tip, task);
      }

      /**
       * Adds a task-attempt in the listener
       */
      private void processTaskAttempt(String taskAttemptId, 
                                      JobHistory.TaskAttempt attempt) {
        TaskAttemptID id = TaskAttemptID.forName(taskAttemptId);
        
        // Check if the transaction for this attempt can be committed
        String taskStatus = attempt.get(Keys.TASK_STATUS);
        TaskAttemptID taskID = TaskAttemptID.forName(taskAttemptId);
        JobInProgress jip = getJob(taskID.getJobID());
        JobStatus prevStatus = (JobStatus)jip.getStatus().clone();

        if (taskStatus.length() > 0) {
          // This means this is an update event
          if (taskStatus.equals(Values.SUCCESS.name())) {
            // Mark this attempt as hanging
            hangingAttempts.put(id.getTaskID().toString(), taskAttemptId);
            addSuccessfulAttempt(jip, id, attempt);
          } else {
            addUnsuccessfulAttempt(jip, id, attempt);
            numEventsRecovered += 2;
          }
        } else {
          createTaskAttempt(jip, id, attempt);
        }
        
        JobStatus newStatus = (JobStatus)jip.getStatus().clone();
        if (prevStatus.getRunState() != newStatus.getRunState()) {
          if(LOG.isDebugEnabled())
            LOG.debug("Status changed hence informing prevStatus" +  prevStatus + " currentStatus "+ newStatus);
          JobStatusChangeEvent event =
            new JobStatusChangeEvent(jip, EventType.RUN_STATE_CHANGED,
                                     prevStatus, newStatus);
          updateJobInProgressListeners(event);
        }
      }

      public void handle(JobHistory.RecordTypes recType, Map<Keys, 
                         String> values) throws IOException {
        if (recType == JobHistory.RecordTypes.Job) {
          // Update the meta-level job information
          job.handle(values);
          
          // Forcefully init the job as we have some updates for it
          checkAndInit();
        } else if (recType.equals(JobHistory.RecordTypes.Task)) {
          String taskId = values.get(Keys.TASKID);
          
          // Create a task
          JobHistory.Task task = new JobHistory.Task();
          task.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(task)) {
            return;
          }
            
          // Process the task i.e update the tip state
          processTask(taskId, task);
        } else if (recType.equals(JobHistory.RecordTypes.MapAttempt)) {
          String attemptId = values.get(Keys.TASK_ATTEMPT_ID);
          
          // Create a task attempt
          JobHistory.MapAttempt attempt = new JobHistory.MapAttempt();
          attempt.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(attempt)) {
            return;
          }
          
          // Process the attempt i.e update the attempt state via job
          processTaskAttempt(attemptId, attempt);
        } else if (recType.equals(JobHistory.RecordTypes.ReduceAttempt)) {
          String attemptId = values.get(Keys.TASK_ATTEMPT_ID);
          
          // Create a task attempt
          JobHistory.ReduceAttempt attempt = new JobHistory.ReduceAttempt();
          attempt.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(attempt)) {
            return;
          }
          
          // Process the attempt i.e update the job state via job
          processTaskAttempt(attemptId, attempt);
        }
      }

      // Check if the task is of type CLEANUP
      private boolean isCleanup(JobHistory.Task task) {
        String taskType = task.get(Keys.TASK_TYPE);
        return Values.CLEANUP.name().equals(taskType);
      }
      
      // Init the job if its ready for init. Also make sure that the scheduler
      // is updated
      private void checkAndInit() throws IOException {
        String jobStatus = this.job.get(Keys.JOB_STATUS);
        if (Values.PREP.name().equals(jobStatus)) {
          hasUpdates = true;
          LOG.info("Calling init from RM for job " + jip.getJobID().toString());
          try {
            initJob(jip);
          } catch (Throwable t) {
            LOG.error("Job initialization failed : \n" 
                      + StringUtils.stringifyException(t));
            failJob(jip);
            throw new IOException(t);
          }
        }
      }
      
      void close() {
        if (hasUpdates) {
          // Apply the final (job-level) updates
          JobStatusChangeEvent event = updateJob(jip, job);
          
          synchronized (JobTracker.this) {
            // Update the job listeners
            updateJobInProgressListeners(event);
          }
        }
      }
      
      public int getNumEventsRecovered() {
        return numEventsRecovered;
      }

    }
    
    public RecoveryManager() {
      jobsToRecover = new TreeSet<JobID>();
    }

    public boolean contains(JobID id) {
      return jobsToRecover.contains(id);
    }

    void addJobForRecovery(JobID id) {
      jobsToRecover.add(id);
    }

    public boolean shouldRecover() {
      return shouldRecover;
    }

    public boolean shouldSchedule() {
      return recoveredTrackers.isEmpty();
    }

    private void markTracker(String trackerName) {
      recoveredTrackers.add(trackerName);
    }

    void unMarkTracker(String trackerName) {
      recoveredTrackers.remove(trackerName);
    }

    Set<JobID> getJobsToRecover() {
      return jobsToRecover;
    }

    /** Check if the given string represents a job-id or not 
     */
    private boolean isJobNameValid(String str) {
      if(str == null) {
        return false;
      }
      String[] parts = str.split("_");
      if(parts.length == 3) {
        if(parts[0].equals("job")) {
            // other 2 parts should be parseable
            return JobTracker.validateIdentifier(parts[1])
                   && JobTracker.validateJobNumber(parts[2]);
        }
      }
      return false;
    }
    
    // checks if the job dir has the required files
    public void checkAndAddJob(FileStatus status) throws IOException {
      String fileName = status.getPath().getName();
      if (isJobNameValid(fileName)) {
        if (JobClient.isJobDirValid(status.getPath(), fs)) {
          recoveryManager.addJobForRecovery(JobID.forName(fileName));
          shouldRecover = true; // enable actual recovery if num-files > 1
        } else {
          LOG.info("Found an incomplete job directory " + fileName + "." 
                   + " Deleting it!!");
          fs.delete(status.getPath(), true);
        }
      }
    }
    
    private JobStatusChangeEvent updateJob(JobInProgress jip, 
        JobHistory.JobInfo job) {
      // Change the job priority
      String jobpriority = job.get(Keys.JOB_PRIORITY);
      JobPriority priority = JobPriority.valueOf(jobpriority);
      // It's important to update this via the jobtracker's api as it will 
      // take care of updating the event listeners too
      
      try {
        setJobPriority(jip.getJobID(), priority);
      } catch (IOException e) {
        // This will not happen. JobTracker can set jobPriority of any job
        // as mrOwner has the needed permissions.
        LOG.warn("Unexpected. JobTracker could not do SetJobPriority on "
                 + jip.getJobID() + ". " + e);
      }

      // Save the previous job status
      JobStatus oldStatus = (JobStatus)jip.getStatus().clone();
      
      // Set the start/launch time only if there are recovered tasks
      // Increment the job's restart count
      jip.updateJobInfo(job.getLong(JobHistory.Keys.SUBMIT_TIME), 
                        job.getLong(JobHistory.Keys.LAUNCH_TIME));

      // Save the new job status
      JobStatus newStatus = (JobStatus)jip.getStatus().clone();
      
      return new JobStatusChangeEvent(jip, EventType.START_TIME_CHANGED, oldStatus, 
                                      newStatus);
    }
    
    private void updateTip(TaskInProgress tip, JobHistory.Task task) {
      long startTime = task.getLong(Keys.START_TIME);
      if (startTime != 0) {
        tip.setExecStartTime(startTime);
      }
      
      long finishTime = task.getLong(Keys.FINISH_TIME);
      // For failed tasks finish-time will be missing
      if (finishTime != 0) {
        tip.setExecFinishTime(finishTime);
      }
      
      String cause = task.get(Keys.TASK_ATTEMPT_ID);
      if (cause.length() > 0) {
        // This means that the this is a FAILED events
        TaskAttemptID id = TaskAttemptID.forName(cause);
        TaskStatus status = tip.getTaskStatus(id);
        synchronized (JobTracker.this) {
          // This will add the tip failed event in the new log
          tip.getJob().failedTask(tip, id, status.getDiagnosticInfo(), 
                                  status.getPhase(), status.getRunState(), 
                                  status.getTaskTracker());
        }
      }
    }
    
    private void createTaskAttempt(JobInProgress job, 
                                   TaskAttemptID attemptId, 
                                   JobHistory.TaskAttempt attempt) {
      TaskID id = attemptId.getTaskID();
      String type = attempt.get(Keys.TASK_TYPE);
      TaskInProgress tip = job.getTaskInProgress(id);
      
      //    I. Get the required info
      TaskStatus taskStatus = null;
      String trackerName = attempt.get(Keys.TRACKER_NAME);
      String trackerHostName = 
        JobInProgress.convertTrackerNameToHostName(trackerName);
      // recover the port information.
      int port = 0; // default to 0
      String hport = attempt.get(Keys.HTTP_PORT);
      if (hport != null && hport.length() > 0) {
        port = attempt.getInt(Keys.HTTP_PORT);
      }
      
      long attemptStartTime = attempt.getLong(Keys.START_TIME);

      // II. Create the (appropriate) task status
      if (type.equals(Values.MAP.name())) {
        taskStatus = 
          new MapTaskStatus(attemptId, 0.0f, job.getNumSlotsPerTask(TaskType.MAP),
                            TaskStatus.State.RUNNING, "", "", trackerName, 
                            TaskStatus.Phase.MAP, new Counters());
      } else {
        taskStatus = 
          new ReduceTaskStatus(attemptId, 0.0f, job.getNumSlotsPerTask(TaskType.REDUCE), 
                               TaskStatus.State.RUNNING, "", "", trackerName, 
                               TaskStatus.Phase.REDUCE, new Counters());
      }

      // Set the start time
      taskStatus.setStartTime(attemptStartTime);

      List<TaskStatus> ttStatusList = new ArrayList<TaskStatus>();
      ttStatusList.add(taskStatus);
      
      // III. Create the dummy tasktracker status
      TaskTrackerStatus ttStatus = 
        new TaskTrackerStatus(trackerName, trackerHostName, port, ttStatusList, 
                              0 , 0, 0, 0);
      ttStatus.setLastSeen(clock.getTime());

      synchronized (JobTracker.this) {
        synchronized (taskTrackers) {
          synchronized (trackerExpiryQueue) {
            // IV. Register a new tracker
            TaskTracker taskTracker = getTaskTracker(trackerName);
            boolean isTrackerRegistered =  (taskTracker != null);
            if (!isTrackerRegistered) {
              markTracker(trackerName); // add the tracker to recovery-manager
              taskTracker = new TaskTracker(trackerName);
              taskTracker.setStatus(ttStatus);
              addNewTracker(taskTracker);
            }
      
            // V. Update the tracker status
            // This will update the meta info of the jobtracker and also add the
            // tracker status if missing i.e register it
            updateTaskTrackerStatus(trackerName, ttStatus);
          }
        }
        // Register the attempt with job and tip, under JobTracker lock. 
        // Since, as of today they are atomic through heartbeat.
        // VI. Register the attempt
        //   a) In the job
        job.addRunningTaskToTIP(tip, attemptId, ttStatus, false);
        //   b) In the tip
        tip.updateStatus(taskStatus);
      }
      
      // VII. Make an entry in the launched tasks
      expireLaunchingTasks.addNewTask(attemptId);
    }
    
    private void addSuccessfulAttempt(JobInProgress job, 
                                      TaskAttemptID attemptId, 
                                      JobHistory.TaskAttempt attempt) {
      // I. Get the required info
      TaskID taskId = attemptId.getTaskID();
      String type = attempt.get(Keys.TASK_TYPE);

      TaskInProgress tip = job.getTaskInProgress(taskId);
      long attemptFinishTime = attempt.getLong(Keys.FINISH_TIME);

      // Get the task status and the tracker name and make a copy of it
      TaskStatus taskStatus = (TaskStatus)tip.getTaskStatus(attemptId).clone();
      taskStatus.setFinishTime(attemptFinishTime);

      String stateString = attempt.get(Keys.STATE_STRING);

      // Update the basic values
      taskStatus.setStateString(stateString);
      taskStatus.setProgress(1.0f);
      taskStatus.setRunState(TaskStatus.State.SUCCEEDED);

      // Set the shuffle/sort finished times
      if (type.equals(Values.REDUCE.name())) {
        long shuffleTime = 
          Long.parseLong(attempt.get(Keys.SHUFFLE_FINISHED));
        long sortTime = 
          Long.parseLong(attempt.get(Keys.SORT_FINISHED));
        taskStatus.setShuffleFinishTime(shuffleTime);
        taskStatus.setSortFinishTime(sortTime);
      }

      // Add the counters
      String counterString = attempt.get(Keys.COUNTERS);
      Counters counter = null;
      //TODO Check if an exception should be thrown
      try {
        counter = Counters.fromEscapedCompactString(counterString);
      } catch (ParseException pe) { 
        counter = new Counters(); // Set it to empty counter
      }
      taskStatus.setCounters(counter);
      
      synchronized (JobTracker.this) {
        // II. Replay the status
        job.updateTaskStatus(tip, taskStatus);
      }
      
      // III. Prevent the task from expiry
      expireLaunchingTasks.removeTask(attemptId);
    }
    
    private void addUnsuccessfulAttempt(JobInProgress job,
                                        TaskAttemptID attemptId,
                                        JobHistory.TaskAttempt attempt) {
      // I. Get the required info
      TaskID taskId = attemptId.getTaskID();
      TaskInProgress tip = job.getTaskInProgress(taskId);
      long attemptFinishTime = attempt.getLong(Keys.FINISH_TIME);

      TaskStatus taskStatus = (TaskStatus)tip.getTaskStatus(attemptId).clone();
      taskStatus.setFinishTime(attemptFinishTime);

      // Reset the progress
      taskStatus.setProgress(0.0f);
      
      String stateString = attempt.get(Keys.STATE_STRING);
      taskStatus.setStateString(stateString);

      boolean hasFailed = 
        attempt.get(Keys.TASK_STATUS).equals(Values.FAILED.name());
      // Set the state failed/killed
      if (hasFailed) {
        taskStatus.setRunState(TaskStatus.State.FAILED);
      } else {
        taskStatus.setRunState(TaskStatus.State.KILLED);
      }

      // Get/Set the error msg
      String diagInfo = attempt.get(Keys.ERROR);
      taskStatus.setDiagnosticInfo(diagInfo); // diag info

      synchronized (JobTracker.this) {
        // II. Update the task status
        job.updateTaskStatus(tip, taskStatus);
      }

     // III. Prevent the task from expiry
     expireLaunchingTasks.removeTask(attemptId);
    }
  
    Path getRestartCountFile() {
      return new Path(getSystemDir(), "jobtracker.info");
    }

    Path getTempRestartCountFile() {
      return new Path(getSystemDir(), "jobtracker.info.recover");
    }

    /**
     * Initialize the recovery process. It simply creates a jobtracker.info file
     * in the jobtracker's system directory and writes its restart count in it.
     * For the first start, the jobtracker writes '0' in it. Upon subsequent 
     * restarts the jobtracker replaces the count with its current count which 
     * is (old count + 1). The whole purpose of this api is to obtain restart 
     * counts across restarts to avoid attempt-id clashes.
     * 
     * Note that in between if the jobtracker.info files goes missing then the
     * jobtracker will disable recovery and continue. 
     *  
     */
    void updateRestartCount() throws IOException {
      Path restartFile = getRestartCountFile();
      Path tmpRestartFile = getTempRestartCountFile();
      FsPermission filePerm = new FsPermission(SYSTEM_FILE_PERMISSION);

      // read the count from the jobtracker info file
      if (fs.exists(restartFile)) {
        fs.delete(tmpRestartFile, false); // delete the tmp file
      } else if (fs.exists(tmpRestartFile)) {
        // if .rec exists then delete the main file and rename the .rec to main
        fs.rename(tmpRestartFile, restartFile); // rename .rec to main file
      } else {
        // For the very first time the jobtracker will create a jobtracker.info
        // file. If the jobtracker has restarted then disable recovery as files'
        // needed for recovery are missing.

        // disable recovery if this is a restart
        shouldRecover = false;

        // write the jobtracker.info file
        try {
          FSDataOutputStream out = FileSystem.create(fs, restartFile, 
                                                     filePerm);
          out.writeInt(0);
          out.close();
        } catch (IOException ioe) {
          LOG.warn("Writing to file " + restartFile + " failed!");
          LOG.warn("FileSystem is not ready yet!");
          fs.delete(restartFile, false);
          throw ioe;
        }
        return;
      }

      FSDataInputStream in = fs.open(restartFile);
      try {
        // read the old count
        restartCount = in.readInt();
        ++restartCount; // increment the restart count
      } catch (IOException ioe) {
        LOG.warn("System directory is garbled. Failed to read file " 
                 + restartFile);
        LOG.warn("Jobtracker recovery is not possible with garbled"
                 + " system directory! Please delete the system directory and"
                 + " restart the jobtracker. Note that deleting the system" 
                 + " directory will result in loss of all the running jobs.");
        throw new RuntimeException(ioe);
      } finally {
        if (in != null) {
          in.close();
        }
      }

      // Write back the new restart count and rename the old info file
      //TODO This is similar to jobhistory recovery, maybe this common code
      //      can be factored out.
      
      // write to the tmp file
      FSDataOutputStream out = FileSystem.create(fs, tmpRestartFile, filePerm);
      out.writeInt(restartCount);
      out.close();

      // delete the main file
      fs.delete(restartFile, false);
      
      // rename the .rec to main file
      fs.rename(tmpRestartFile, restartFile);
    }

                                   // mapred.JobID::forName returns
    @SuppressWarnings("unchecked") // mapreduce.JobID
    public void recover() {
      if (!shouldRecover()) {
        // clean up jobs structure
        jobsToRecover.clear();
        return;
      }

      LOG.info("Restart count of the jobtracker : " + restartCount);

      // I. Init the jobs and cache the recovered job history filenames
      Map<JobID, Path> jobHistoryFilenameMap = new HashMap<JobID, Path>();
      Iterator<JobID> idIter = jobsToRecover.iterator();

      // 0. Cleanup
      try {
        JobHistory.JobInfo.deleteConfFiles();
      } catch (IOException ioe) {
        LOG.info("Error in cleaning up job history folder", ioe);
      }

      JobInProgress job = null;
      File jobIdFile = null;

      // 0. Cleanup
      try {
        JobHistory.JobInfo.deleteConfFiles();
      } catch (IOException ioe) {
        LOG.info("Error in cleaning up job history folder", ioe);
      }

      while (idIter.hasNext()) {
        JobID id = idIter.next();
        LOG.info("Trying to recover details of job " + id);
        try {
          // 1. Recover job owner and create JIP
          jobIdFile = 
            new File(lDirAlloc.getLocalPathToRead(SUBDIR + "/" + id, conf).toString());

          String user = null;
          if (jobIdFile != null && jobIdFile.exists()) {
            LOG.info("File " + jobIdFile + " exists for job " + id);
            FileInputStream in = new FileInputStream(jobIdFile);
            BufferedReader reader = null;
            try {
              reader = new BufferedReader(new InputStreamReader(in));
              user = reader.readLine();
              LOG.info("Recovered user " + user + " for job " + id);
            } finally {
              if (reader != null) {
                reader.close();
              }
              in.close();
            }
          }
          if (user == null) {
            throw new RuntimeException("Incomplete job " + id);
          }

          // Create the job
          /* THIS PART OF THE CODE IS USELESS. JOB RECOVERY SHOULD BE
           * BACKPORTED (MAPREDUCE-873)
           */
          job = new JobInProgress(JobTracker.this, conf,
              new JobInfo((org.apache.hadoop.mapreduce.JobID) id,
                new Text(user), new Path(getStagingAreaDirInternal(user))),
              restartCount, new Credentials() /*HACK*/);

          // 2. Check if the user has appropriate access
          // Get the user group info for the job's owner
          UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(job.getJobConf().getUser());
          LOG.info("Submitting job " + id + " on behalf of user "
                   + ugi.getShortUserName() + " in groups : "
                   + StringUtils.arrayToString(ugi.getGroupNames()));

          // check the access
          try {
            aclsManager.checkAccess(job, ugi, Operation.SUBMIT_JOB);
          } catch (Throwable t) {
            LOG.warn("Access denied for user " + ugi.getShortUserName() 
                     + " in groups : [" 
                     + StringUtils.arrayToString(ugi.getGroupNames()) + "]");
            throw t;
          }

          // 3. Get the log file and the file path
          String logFileName = 
            JobHistory.JobInfo.getJobHistoryFileName(job.getJobConf(), id);
          if (logFileName != null) {
            Path jobHistoryFilePath = 
              JobHistory.JobInfo.getJobHistoryLogLocation(logFileName);

            // 4. Recover the history file. This involved
            //     - deleting file.recover if file exists
            //     - renaming file.recover to file if file doesnt exist
            // This makes sure that the (master) file exists
            JobHistory.JobInfo.recoverJobHistoryFile(job.getJobConf(), 
                                                     jobHistoryFilePath);
          
            // 5. Cache the history file name as it costs one dfs access
            jobHistoryFilenameMap.put(job.getJobID(), jobHistoryFilePath);
          } else {
            LOG.info("No history file found for job " + id);
            idIter.remove(); // remove from recovery list
          }

          // 6. Sumbit the job to the jobtracker
          addJob(id, job);
        } catch (Throwable t) {
          LOG.warn("Failed to recover job " + id + " Ignoring the job.", t);
          idIter.remove();
          if (jobIdFile != null) {
            jobIdFile.delete();
            jobIdFile = null;
          }
          if (job != null) {
            job.fail();
            job = null;
          }
          continue;
        }
      }

      long recoveryStartTime = clock.getTime();

      // II. Recover each job
      idIter = jobsToRecover.iterator();
      while (idIter.hasNext()) {
        JobID id = idIter.next();
        JobInProgress pJob = getJob(id);

        // 1. Get the required info
        // Get the recovered history file
        Path jobHistoryFilePath = jobHistoryFilenameMap.get(pJob.getJobID());
        String logFileName = jobHistoryFilePath.getName();

        FileSystem fs;
        try {
          fs = jobHistoryFilePath.getFileSystem(conf);
        } catch (IOException ioe) {
          LOG.warn("Failed to get the filesystem for job " + id + ". Ignoring.",
                   ioe);
          continue;
        }

        // 2. Parse the history file
        // Note that this also involves job update
        JobRecoveryListener listener = new JobRecoveryListener(pJob);
        try {
          JobHistory.parseHistoryFromFS(jobHistoryFilePath.toString(), 
                                        listener, fs);
        } catch (Throwable t) {
          LOG.info("Error reading history file of job " + pJob.getJobID() 
                   + ". Ignoring the error and continuing.", t);
        }

        // 3. Close the listener
        listener.close();
        
        // 4. Update the recovery metric
        totalEventsRecovered += listener.getNumEventsRecovered();

        // 5. Cleanup history
        // Delete the master log file as an indication that the new file
        // should be used in future
        try {
          synchronized (pJob) {
            JobHistory.JobInfo.checkpointRecovery(logFileName, 
                                                  pJob.getJobConf());
          }
        } catch (Throwable t) {
          LOG.warn("Failed to delete log file (" + logFileName + ") for job " 
                   + id + ". Continuing.", t);
        }

        if (pJob.isComplete()) {
          idIter.remove(); // no need to keep this job info as its successful
        }
      }

      recoveryDuration = clock.getTime() - recoveryStartTime;
      hasRecovered = true;

      // III. Finalize the recovery
      synchronized (trackerExpiryQueue) {
        // Make sure that the tracker statuses in the expiry-tracker queue
        // are updated
        long now = clock.getTime();
        int size = trackerExpiryQueue.size();
        for (int i = 0; i < size ; ++i) {
          // Get the first tasktracker
          TaskTrackerStatus taskTracker = trackerExpiryQueue.first();

          // Remove it
          trackerExpiryQueue.remove(taskTracker);

          // Set the new time
          taskTracker.setLastSeen(now);

          // Add back to get the sorted list
          trackerExpiryQueue.add(taskTracker);
        }
      }

      LOG.info("Restoration complete");
    }
    
    int totalEventsRecovered() {
      return totalEventsRecovered;
    }
  }

  private final JobTrackerInstrumentation myInstrumentation;
    
  /////////////////////////////////////////////////////////////////
  // The real JobTracker
  ////////////////////////////////////////////////////////////////
  int port;
  String localMachine;
  private String trackerIdentifier;
  long startTime;
  int totalSubmissions = 0;
  private int totalMapTaskCapacity;
  private int totalReduceTaskCapacity;
  private HostsFileReader hostsReader;
  
  // JobTracker recovery variables
  private volatile boolean hasRestarted = false;
  private volatile boolean hasRecovered = false;
  private volatile long recoveryDuration;

  //
  // Properties to maintain while running Jobs and Tasks:
  //
  // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its 
  //     Tasks are complete.
  //
  // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
  //
  // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
  //
  // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
  //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
  //     reexecution if all downstream Tasks that require its output have already obtained
  //     the necessary files.
  //

  // All the known jobs.  (jobid->JobInProgress)
  Map<JobID, JobInProgress> jobs =  
    Collections.synchronizedMap(new TreeMap<JobID, JobInProgress>());

  // (user -> list of JobInProgress)
  TreeMap<String, ArrayList<JobInProgress>> userToJobsMap =
    new TreeMap<String, ArrayList<JobInProgress>>();
    
  // (trackerID --> list of jobs to cleanup)
  Map<String, Set<JobID>> trackerToJobsToCleanup = 
    new HashMap<String, Set<JobID>>();
  
  // (trackerID --> list of tasks to cleanup)
  Map<String, Set<TaskAttemptID>> trackerToTasksToCleanup = 
    new HashMap<String, Set<TaskAttemptID>>();
  
  // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
  Map<TaskAttemptID, TaskInProgress> taskidToTIPMap =
    new TreeMap<TaskAttemptID, TaskInProgress>();
  // This is used to keep track of all trackers running on one host. While
  // decommissioning the host, all the trackers on the host will be lost.
  Map<String, Set<TaskTracker>> hostnameToTaskTracker = 
    Collections.synchronizedMap(new TreeMap<String, Set<TaskTracker>>());
  

  // (taskid --> trackerID) 
  TreeMap<TaskAttemptID, String> taskidToTrackerMap = new TreeMap<TaskAttemptID, String>();

  // (trackerID->TreeSet of taskids running at that tracker)
  TreeMap<String, Set<TaskAttemptID>> trackerToTaskMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // (trackerID -> TreeSet of completed taskids running at that tracker)
  TreeMap<String, Set<TaskAttemptID>> trackerToMarkedTasksMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // (trackerID --> last sent HeartBeatResponse)
  Map<String, HeartbeatResponse> trackerToHeartbeatResponseMap = 
    new TreeMap<String, HeartbeatResponse>();

  // (hostname --> Node (NetworkTopology))
  Map<String, Node> hostnameToNodeMap = 
    Collections.synchronizedMap(new TreeMap<String, Node>());
  
  // Number of resolved entries
  int numResolved;
    
  private FaultyTrackersInfo faultyTrackers = new FaultyTrackersInfo();
  
  private JobTrackerStatistics statistics = 
    new JobTrackerStatistics();
  //
  // Watch and expire TaskTracker objects using these structures.
  // We can map from Name->TaskTrackerStatus, or we can expire by time.
  //
  int totalMaps = 0;
  int totalReduces = 0;
  private int occupiedMapSlots = 0;
  private int occupiedReduceSlots = 0;
  private int reservedMapSlots = 0;
  private int reservedReduceSlots = 0;
  private HashMap<String, TaskTracker> taskTrackers =
    new HashMap<String, TaskTracker>();
  Map<String,Integer>uniqueHostsMap = new ConcurrentHashMap<String, Integer>();
  ExpireTrackers expireTrackers = new ExpireTrackers();
  Thread expireTrackersThread = null;
  RetireJobs retireJobs = new RetireJobs();
  Thread retireJobsThread = null;
  final int retiredJobsCacheSize;
  ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
  Thread expireLaunchingTaskThread = new Thread(expireLaunchingTasks,
                                                "expireLaunchingTasks");

  CompletedJobStatusStore completedJobStatusStore = null;
  Thread completedJobsStoreThread = null;
  RecoveryManager recoveryManager;

  /**
   * It might seem like a bug to maintain a TreeSet of tasktracker objects,
   * which can be updated at any time.  But that's not what happens!  We
   * only update status objects in the taskTrackers table.  Status objects
   * are never updated once they enter the expiry queue.  Instead, we wait
   * for them to expire and remove them from the expiry queue.  If a status
   * object has been updated in the taskTracker table, the latest status is 
   * reinserted.  Otherwise, we assume the tracker has expired.
   */
  TreeSet<TaskTrackerStatus> trackerExpiryQueue =
    new TreeSet<TaskTrackerStatus>(
                                   new Comparator<TaskTrackerStatus>() {
                                     public int compare(TaskTrackerStatus p1, TaskTrackerStatus p2) {
                                       if (p1.getLastSeen() < p2.getLastSeen()) {
                                         return -1;
                                       } else if (p1.getLastSeen() > p2.getLastSeen()) {
                                         return 1;
                                       } else {
                                         return (p1.getTrackerName().compareTo(p2.getTrackerName()));
                                       }
                                     }
                                   }
                                   );

  // Used to provide an HTML view on Job, Task, and TaskTracker structures
  final HttpServer infoServer;
  int infoPort;

  Server interTrackerServer;

  // Some jobs are stored in a local system directory.  We can delete
  // the files when we're done with the job.
  static final String SUBDIR = "jobTracker";
  final LocalFileSystem localFs;
  FileSystem fs = null;
  Path systemDir = null;
  JobConf conf;

  private final ACLsManager aclsManager;

  long limitMaxMemForMapTasks;
  long limitMaxMemForReduceTasks;
  long memSizeForMapSlotOnJT;
  long memSizeForReduceSlotOnJT;

  private QueueManager queueManager;

  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf) throws IOException, InterruptedException {
    this(conf, generateNewIdentifier());
  }

  JobTracker(JobConf conf, Clock clock) 
  throws IOException, InterruptedException {
    this(conf, generateNewIdentifier(), clock);
  }
  
  public static final String JT_USER_NAME = "mapreduce.jobtracker.kerberos.principal";
  public static final String JT_KEYTAB_FILE =
    "mapreduce.jobtracker.keytab.file";
  
  JobTracker(final JobConf conf, String identifier) 
  throws IOException, InterruptedException {   
    this(conf, identifier, new Clock());
  }
  
  JobTracker(final JobConf conf, String identifier, Clock clock) 
  throws IOException, InterruptedException { 
    this.clock = clock;
    // Set ports, start RPC servers, setup security policy etc.
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();
    // find the owner of the process
    // get the desired principal to load
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, JT_KEYTAB_FILE, JT_USER_NAME, localMachine);

    long secretKeyInterval = 
    conf.getLong(DELEGATION_KEY_UPDATE_INTERVAL_KEY, 
                   DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
      conf.getLong(DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                   DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
      conf.getLong(DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 
                   DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    secretManager = 
      new DelegationTokenSecretManager(secretKeyInterval,
                                       tokenMaxLifetime,
                                       tokenRenewInterval,
                                       DELEGATION_TOKEN_GC_INTERVAL);
    secretManager.startThreads();
       
    MAX_JOBCONF_SIZE = conf.getLong(MAX_USER_JOBCONF_SIZE_KEY, MAX_JOBCONF_SIZE);
    //
    // Grab some static constants
    //
    TASKTRACKER_EXPIRY_INTERVAL = 
      conf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);
    RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
    RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
    retiredJobsCacheSize =
             conf.getInt("mapred.job.tracker.retiredjobs.cache.size", 1000);
    MAX_COMPLETE_USER_JOBS_IN_MEMORY = conf.getInt("mapred.jobtracker.completeuserjobs.maximum", 100);
    MAX_BLACKLISTS_PER_TRACKER = 
        conf.getInt("mapred.max.tracker.blacklists", 4);
    
    NUM_HEARTBEATS_IN_SECOND = 
      conf.getInt(JT_HEARTBEATS_IN_SECOND, DEFAULT_NUM_HEARTBEATS_IN_SECOND);
    if (NUM_HEARTBEATS_IN_SECOND < MIN_NUM_HEARTBEATS_IN_SECOND) {
      NUM_HEARTBEATS_IN_SECOND = DEFAULT_NUM_HEARTBEATS_IN_SECOND;
    }

    HEARTBEAT_INTERVAL_MIN =
      conf.getInt(JT_HEARTBEAT_INTERVAL_MIN, HEARTBEAT_INTERVAL_MIN_DEFAULT);
    
    HEARTBEATS_SCALING_FACTOR = 
      conf.getFloat(JT_HEARTBEATS_SCALING_FACTOR, 
                    DEFAULT_HEARTBEATS_SCALING_FACTOR);
    if (HEARTBEATS_SCALING_FACTOR < MIN_HEARTBEATS_SCALING_FACTOR) {
      HEARTBEATS_SCALING_FACTOR = DEFAULT_HEARTBEATS_SCALING_FACTOR;
    }

    //This configuration is there solely for tuning purposes and 
    //once this feature has been tested in real clusters and an appropriate
    //value for the threshold has been found, this config might be taken out.
    AVERAGE_BLACKLIST_THRESHOLD = 
      conf.getFloat("mapred.cluster.average.blacklist.threshold", 0.5f); 

    // This is a directory of temporary submission files.  We delete it
    // on startup, and can delete any files that we're done with
    this.conf = conf;
    JobConf jobConf = new JobConf(conf);

    initializeTaskMemoryRelatedConfig();

    // Read the hosts/exclude files to restrict access to the jobtracker.
    this.hostsReader = new HostsFileReader(conf.get("mapred.hosts", ""),
                                           conf.get("mapred.hosts.exclude", ""));

    Configuration queuesConf = new Configuration(this.conf);
    queueManager = new QueueManager(queuesConf);

    aclsManager = new ACLsManager(conf, new JobACLsManager(conf), queueManager);

    LOG.info("Starting jobtracker with owner as " +
        getMROwner().getShortUserName());

    // Create the scheduler
    Class<? extends TaskScheduler> schedulerClass
      = conf.getClass("mapred.jobtracker.taskScheduler",
          JobQueueTaskScheduler.class, TaskScheduler.class);
    taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(schedulerClass, conf);
    
    int handlerCount = conf.getInt("mapred.job.tracker.handler.count", 10);
    this.interTrackerServer = 
      RPC.getServer(this, addr.getHostName(), addr.getPort(), handlerCount, 
          false, conf, secretManager);

    // Set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      this.interTrackerServer.refreshServiceAcl(conf, new MapReducePolicyProvider());
    }

    if (LOG.isDebugEnabled()) {
      Properties p = System.getProperties();
      for (Iterator it = p.keySet().iterator(); it.hasNext();) {
        String key = (String) it.next();
        String val = p.getProperty(key);
        LOG.debug("Property '" + key + "' is " + val);
      }
    }

    String infoAddr = 
      NetUtils.getServerAddress(conf, "mapred.job.tracker.info.bindAddress",
                                "mapred.job.tracker.info.port",
                                "mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.startTime = clock.getTime();
    infoServer = new HttpServer("job", infoBindAddress, tmpInfoPort, 
        tmpInfoPort == 0, conf, aclsManager.getAdminsAcl());
    infoServer.setAttribute("job.tracker", this);
    // initialize history parameters.
    final JobTracker jtFinal = this;
    getMROwner().doAs(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        JobHistory.init(jtFinal, conf,jtFinal.localMachine, 
            jtFinal.startTime);
        return true;
      }
    });
    
    infoServer.addServlet("reducegraph", "/taskgraph", TaskGraphServlet.class);
    infoServer.start();
    
    this.trackerIdentifier = identifier;

    // Initialize instrumentation
    JobTrackerInstrumentation tmp;
    Class<? extends JobTrackerInstrumentation> metricsInst =
      getInstrumentationClass(jobConf);
    try {
      java.lang.reflect.Constructor<? extends JobTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class[] {JobTracker.class, JobConf.class} );
      tmp = c.newInstance(this, jobConf);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by 
      //falling back on the default.
      LOG.error("failed to initialize job tracker metrics", e);
      tmp = new JobTrackerMetricsInst(this, jobConf);
    }
    myInstrumentation = tmp;
    
    // The rpc/web-server ports can be ephemeral ports... 
    // ... ensure we have the correct info
    this.port = interTrackerServer.getListenerAddress().getPort();
    this.conf.set("mapred.job.tracker", (this.localMachine + ":" + this.port));
    this.localFs = FileSystem.getLocal(conf);
    LOG.info("JobTracker up at: " + this.port);
    this.infoPort = this.infoServer.getPort();
    this.conf.set("mapred.job.tracker.http.address", 
        infoBindAddress + ":" + this.infoPort); 
    LOG.info("JobTracker webserver: " + this.infoServer.getPort());
    
    // start the recovery manager
    recoveryManager = new RecoveryManager();
    
    // start async disk service for asynchronous deletion service
    asyncDiskService = new MRAsyncDiskService(FileSystem.getLocal(jobConf),
        jobConf.getLocalDirs());
    
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // if we haven't contacted the namenode go ahead and do it
        if (fs == null) {
          fs = getMROwner().doAs(new PrivilegedExceptionAction<FileSystem>() {
            public FileSystem run() throws IOException {
              return FileSystem.get(conf);
          }});
        }
        // clean up the system dir, which will only work if hdfs is out of 
        // safe mode
        if(systemDir == null) {
          systemDir = new Path(getSystemDir());    
        }
        try {
          FileStatus systemDirStatus = fs.getFileStatus(systemDir);
          if (!systemDirStatus.getOwner().equals(
              getMROwner().getShortUserName())) {
            throw new AccessControlException("The systemdir " + systemDir +
                " is not owned by " + getMROwner().getShortUserName());
          }
          if (!systemDirStatus.getPermission().equals(SYSTEM_DIR_PERMISSION)) {
            LOG.warn("Incorrect permissions on " + systemDir +
                ". Setting it to " + SYSTEM_DIR_PERMISSION);
            fs.setPermission(systemDir,new FsPermission(SYSTEM_DIR_PERMISSION));
          }
        } catch (FileNotFoundException fnf) {} //ignore
        // Make sure that the backup data is preserved
        FileStatus[] systemDirData = fs.listStatus(this.systemDir);
        // Check if the history is enabled .. as we cant have persistence with 
        // history disabled
        if (conf.getBoolean("mapred.jobtracker.restart.recover", false) 
            && systemDirData != null) {
          for (FileStatus status : systemDirData) {
            try {
              recoveryManager.checkAndAddJob(status);
            } catch (Throwable t) {
              LOG.warn("Failed to add the job " + status.getPath().getName(), 
                       t);
            }
          }
          
          // Check if there are jobs to be recovered
          hasRestarted = recoveryManager.shouldRecover();
          if (hasRestarted) {
            break; // if there is something to recover else clean the sys dir
          }
        }

        if (!fs.exists(systemDir)) {
          LOG.info("Creating the system directory");
          if (FileSystem.mkdirs(fs, systemDir, 
                                new FsPermission(SYSTEM_DIR_PERMISSION))) {
            // success
            break;
          } else {
            LOG.error("Mkdirs failed to create " + systemDir);
          }
        } else {
          LOG.info("Cleaning up the system directory");
          fs.setPermission(systemDir, new FsPermission(SYSTEM_DIR_PERMISSION));
          // It exists, just set permissions and clean the contents up
          deleteContents(fs, systemDir);
          break;
        }
      } catch (AccessControlException ace) {
        LOG.warn("Failed to operate on mapred.system.dir (" +
                 systemDir.makeQualified(fs) +
                 ") because of permissions.");
        LOG.warn("This directory should be owned by the user '" +
                 UserGroupInformation.getCurrentUser() + "'");
        LOG.warn("Bailing out ... ", ace);
        throw ace;
      } catch (IOException ie) {
        LOG.info("problem cleaning system directory: " +
                 systemDir.makeQualified(fs), ie);
      }
      Thread.sleep(FS_ACCESS_RETRY_PERIOD);
    }
    
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    
    // Same with 'localDir' except it's always on the local disk.
    asyncDiskService.moveAndDeleteFromEachVolume(SUBDIR);

    if (!hasRestarted) {
      jobConf.deleteLocalFiles(SUBDIR);
    }

    // Initialize history DONE folder
    FileSystem historyFS = getMROwner().doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws IOException {
        JobHistory.initDone(conf, fs);
        final String historyLogDir = 
          JobHistory.getCompletedJobHistoryLocation().toString();
        infoServer.setAttribute("historyLogDir", historyLogDir);
        
        return new Path(historyLogDir).getFileSystem(conf);
      }
    });
    infoServer.setAttribute("fileSys", historyFS);

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);
    this.numTaskCacheLevels = conf.getInt("mapred.task.cache.levels", 
        NetworkTopology.DEFAULT_HOST_LEVEL);

    //initializes the job status store
    completedJobStatusStore = new CompletedJobStatusStore(conf, aclsManager);
    pluginDispatcher = PluginDispatcher.createFromConfiguration(
            conf, "mapred.jobtracker.plugins", JobTrackerPlugin.class);
    pluginDispatcher.dispatchStart(this);
  }

  /**
   * Recursively delete the contents of a directory without deleting the
   * directory itself.
   */
  private void deleteContents(FileSystem fs, Path dir) throws IOException {
    for (FileStatus stat : fs.listStatus(dir)) {
      if (!fs.delete(stat.getPath(), true)) {
        throw new IOException("Unable to delete " + stat.getPath());
      }
    }
  }

  private static SimpleDateFormat getDateFormat() {
    return new SimpleDateFormat("yyyyMMddHHmm");
  }

  private static String generateNewIdentifier() {
    return getDateFormat().format(new Date());
  }
  
  static boolean validateIdentifier(String id) {
    try {
      // the jobtracker id should be 'date' parseable
      getDateFormat().parse(id);
      return true;
    } catch (ParseException pe) {}
    return false;
  }

  static boolean validateJobNumber(String id) {
    try {
      // the job number should be integer parseable
      Integer.parseInt(id);
      return true;
    } catch (IllegalArgumentException pe) {}
    return false;
  }

  /**
   * Whether the JT has restarted
   */
  public boolean hasRestarted() {
    return hasRestarted;
  }

  /**
   * Whether the JT has recovered upon restart
   */
  public boolean hasRecovered() {
    return hasRecovered;
  }

  /**
   * How long the jobtracker took to recover from restart.
   */
  public long getRecoveryDuration() {
    return hasRestarted() 
           ? recoveryDuration
           : 0;
  }

  /**
   * Get JobTracker's FileSystem. This is the filesystem for mapred.system.dir.
   */
  FileSystem getFileSystem() {
    return fs;
  }

  /**
   * Get JobTracker's LocalFileSystem handle. This is used by jobs for 
   * localizing job files to the local disk.
   */
  LocalFileSystem getLocalFileSystem() throws IOException {
    return localFs;
  }

  public static Class<? extends JobTrackerInstrumentation> getInstrumentationClass(Configuration conf) {
    return conf.getClass("mapred.jobtracker.instrumentation",
        JobTrackerMetricsInst.class, JobTrackerInstrumentation.class);
  }
  
  public static void setInstrumentationClass(Configuration conf, Class<? extends JobTrackerInstrumentation> t) {
    conf.setClass("mapred.jobtracker.instrumentation",
        t, JobTrackerInstrumentation.class);
  }

  JobTrackerInstrumentation getInstrumentation() {
    return myInstrumentation;
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr = 
      conf.get("mapred.job.tracker", "localhost:8012");
    return NetUtils.createSocketAddr(jobTrackerStr);
  }

  /**
   * Run forever
   */
  public void offerService() throws InterruptedException, IOException {
    // Prepare for recovery. This is done irrespective of the status of restart
    // flag.
    while (true) {
      try {
        recoveryManager.updateRestartCount();
        break;
      } catch (IOException ioe) {
        LOG.warn("Failed to initialize recovery manager. ", ioe);
        // wait for some time
        Thread.sleep(FS_ACCESS_RETRY_PERIOD);
        LOG.warn("Retrying...");
      }
    }

    taskScheduler.start();
    
    //  Start the recovery after starting the scheduler
    try {
      recoveryManager.recover();
    } catch (Throwable t) {
      LOG.warn("Recovery manager crashed! Ignoring.", t);
    }
    // refresh the node list as the recovery manager might have added 
    // disallowed trackers
    refreshHosts();
    
    this.expireTrackersThread = new Thread(this.expireTrackers,
                                          "expireTrackers");
    this.expireTrackersThread.start();
    this.retireJobsThread = new Thread(this.retireJobs, "retireJobs");
    this.retireJobsThread.start();
    expireLaunchingTaskThread.start();

    if (completedJobStatusStore.isActive()) {
      completedJobsStoreThread = new Thread(completedJobStatusStore,
                                            "completedjobsStore-housekeeper");
      completedJobsStoreThread.start();
    }

    // start the inter-tracker server once the jt is ready
    this.interTrackerServer.start();
    
    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");
    
    this.interTrackerServer.join();
    LOG.info("Stopped interTrackerServer");
  }

  void close() throws IOException {
    if (this.pluginDispatcher != null) {
        LOG.info("Stopping pluginDispatcher");
        pluginDispatcher.dispatchStop();
      }
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        LOG.warn("Exception shutting down JobTracker", ex);
      }
    }
    if (this.interTrackerServer != null) {
      LOG.info("Stopping interTrackerServer");
      this.interTrackerServer.stop();
    }
    if (this.expireTrackersThread != null && this.expireTrackersThread.isAlive()) {
      LOG.info("Stopping expireTrackers");
      this.expireTrackersThread.interrupt();
      try {
        this.expireTrackersThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.retireJobsThread != null && this.retireJobsThread.isAlive()) {
      LOG.info("Stopping retirer");
      this.retireJobsThread.interrupt();
      try {
        this.retireJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (taskScheduler != null) {
      taskScheduler.terminate();
    }
    if (this.expireLaunchingTaskThread != null && this.expireLaunchingTaskThread.isAlive()) {
      LOG.info("Stopping expireLaunchingTasks");
      this.expireLaunchingTaskThread.interrupt();
      try {
        this.expireLaunchingTaskThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.completedJobsStoreThread != null &&
        this.completedJobsStoreThread.isAlive()) {
      LOG.info("Stopping completedJobsStore thread");
      this.completedJobsStoreThread.interrupt();
      try {
        this.completedJobsStoreThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    DelegationTokenRenewal.close();
    LOG.info("stopped all jobtracker services");
    return;
  }
    
  ///////////////////////////////////////////////////////
  // Maintain lookup tables; called by JobInProgress
  // and TaskInProgress
  ///////////////////////////////////////////////////////
  void createTaskEntry(TaskAttemptID taskid, String taskTracker, TaskInProgress tip) {
    LOG.info("Adding task (" + tip.getAttemptType(taskid) + ") " + 
      "'"  + taskid + "' to tip " + 
      tip.getTIPId() + ", for tracker '" + taskTracker + "'");

    // taskid --> tracker
    taskidToTrackerMap.put(taskid, taskTracker);

    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToTaskMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToTaskMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);

    // taskid --> TIP
    taskidToTIPMap.put(taskid, tip);
    
  }
    
  void removeTaskEntry(TaskAttemptID taskid) {
    // taskid --> tracker
    String tracker = taskidToTrackerMap.remove(taskid);

    // tracker --> taskid
    if (tracker != null) {
      Set<TaskAttemptID> trackerSet = trackerToTaskMap.get(tracker);
      if (trackerSet != null) {
        trackerSet.remove(taskid);
      }
    }

    // taskid --> TIP
    if (taskidToTIPMap.remove(taskid) != null) {   
      LOG.info("Removing task '" + taskid + "'");
    }
  }
    
  /**
   * Mark a 'task' for removal later.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker the tasktracker at which the 'task' was running
   * @param taskid completed (success/failure/killed) task
   */
  void markCompletedTaskAttempt(String taskTracker, TaskAttemptID taskid) {
    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToMarkedTasksMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToMarkedTasksMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);
      
    if (LOG.isDebugEnabled()) {
      LOG.debug("Marked '" + taskid + "' from '" + taskTracker + "'");
    }
  }

  /**
   * Mark all 'non-running' jobs of the job for pruning.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param job the completed job
   */
  void markCompletedJob(JobInProgress job) {
    for (TaskInProgress tip : job.getTasks(TaskType.JOB_SETUP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.MAP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.REDUCE)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
  }
    
  /**
   * Remove all 'marked' tasks running on a given {@link TaskTracker}
   * from the {@link JobTracker}'s data-structures.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker tasktracker whose 'non-running' tasks are to be purged
   */
  private void removeMarkedTasks(String taskTracker) {
    // Purge all the 'marked' tasks which were running at taskTracker
    Set<TaskAttemptID> markedTaskSet = 
      trackerToMarkedTasksMap.get(taskTracker);
    if (markedTaskSet != null) {
      for (TaskAttemptID taskid : markedTaskSet) {
        removeTaskEntry(taskid);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removed marked completed task '" + taskid + "' from '" + 
                    taskTracker + "'");
        }
      }
      // Clear
      trackerToMarkedTasksMap.remove(taskTracker);
    }
  }
    
  /**
   * Call {@link #removeTaskEntry(String)} for each of the
   * job's tasks.
   * When the JobTracker is retiring the long-completed
   * job, either because it has outlived {@link #RETIRE_JOB_INTERVAL}
   * or the limit of {@link #MAX_COMPLETE_USER_JOBS_IN_MEMORY} jobs 
   * has been reached, we can afford to nuke all it's tasks; a little
   * unsafe, but practically feasible. 
   * 
   * @param job the job about to be 'retired'
   */
  synchronized void removeJobTasks(JobInProgress job) { 
    // iterate over all the task types
    for (TaskType type : TaskType.values()) {
      // iterate over all the tips of the type under consideration
      for (TaskInProgress tip : job.getTasks(type)) {
        // iterate over all the task-ids in the tip under consideration
        for (TaskAttemptID id : tip.getAllTaskAttemptIDs()) {
          // remove the task-id entry from the jobtracker
          removeTaskEntry(id);
        }
      }
    }
  }
    
  /**
   * Safe clean-up all data structures at the end of the 
   * job (success/failure/killed).
   * Here we also ensure that for a given user we maintain 
   * information for only MAX_COMPLETE_USER_JOBS_IN_MEMORY jobs 
   * on the JobTracker.
   *  
   * @param job completed job.
   */
  synchronized void finalizeJob(JobInProgress job) {
    // Mark the 'non-running' tasks for pruning
    markCompletedJob(job);
    
    JobEndNotifier.registerNotification(job.getJobConf(), job.getStatus());

    // start the merge of log files
    JobID id = job.getStatus().getJobID();
    if (job.hasRestarted()) {
      try {
        JobHistory.JobInfo.finalizeRecovery(id, job.getJobConf());
      } catch (IOException ioe) {
        LOG.info("Failed to finalize the log file recovery for job " + id, ioe);
      }
    }

    // mark the job as completed
    try {
      JobHistory.JobInfo.markCompleted(id);
    } catch (IOException ioe) {
      LOG.info("Failed to mark job " + id + " as completed!", ioe);
    }

    final JobTrackerInstrumentation metrics = getInstrumentation();
    metrics.finalizeJob(conf, id);
    
    long now = clock.getTime();
    
    // mark the job for cleanup at all the trackers
    addJobForCleanup(id);

    // add the blacklisted trackers to potentially faulty list
    if (job.getStatus().getRunState() == JobStatus.SUCCEEDED) {
      if (job.getNoOfBlackListedTrackers() > 0) {
        for (String hostName : job.getBlackListedTrackers()) {
          faultyTrackers.incrementFaults(hostName);
        }
      }
    }

    String jobUser = job.getProfile().getUser();
    //add to the user to jobs mapping
    synchronized (userToJobsMap) {
      ArrayList<JobInProgress> userJobs = userToJobsMap.get(jobUser);
      if (userJobs == null) {
        userJobs =  new ArrayList<JobInProgress>();
        userToJobsMap.put(jobUser, userJobs);
      }
      userJobs.add(job);
    }
  }

  ///////////////////////////////////////////////////////
  // Accessors for objects that want info on jobs, tasks,
  // trackers, etc.
  ///////////////////////////////////////////////////////
  public int getTotalSubmissions() {
    return totalSubmissions;
  }
  public String getJobTrackerMachine() {
    return localMachine;
  }
  
  /**
   * Get the unique identifier (ie. timestamp) of this job tracker start.
   * @return a string with a unique identifier
   */
  public String getTrackerIdentifier() {
    return trackerIdentifier;
  }

  public int getTrackerPort() {
    return port;
  }
  public int getInfoPort() {
    return infoPort;
  }
  public long getStartTime() {
    return startTime;
  }
  public Vector<JobInProgress> runningJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.RUNNING) {
        v.add(jip);
      }
    }
    return v;
  }
  /**
   * Version that is called from a timer thread, and therefore needs to be
   * careful to synchronize.
   */
  public synchronized List<JobInProgress> getRunningJobs() {
    synchronized (jobs) {
      return runningJobs();
    }
  }
  public Vector<JobInProgress> failedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if ((status.getRunState() == JobStatus.FAILED)
          || (status.getRunState() == JobStatus.KILLED)) {
        v.add(jip);
      }
    }
    return v;
  }

  public synchronized List<JobInProgress> getFailedJobs() {
    synchronized (jobs) {
      return failedJobs();
    }
  }

  public Vector<JobInProgress> completedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.SUCCEEDED) {
        v.add(jip);
      }
    }
    return v;
  }

  public synchronized List<JobInProgress> getCompletedJobs() {
    synchronized (jobs) {
      return completedJobs();
    }
  }

  /**
   * Get all the task trackers in the cluster
   * 
   * @return {@link Collection} of {@link TaskTrackerStatus} 
   */
  // lock to taskTrackers should hold JT lock first.
  public synchronized Collection<TaskTrackerStatus> taskTrackers() {
    Collection<TaskTrackerStatus> ttStatuses;
    synchronized (taskTrackers) {
      ttStatuses = 
        new ArrayList<TaskTrackerStatus>(taskTrackers.values().size());
      for (TaskTracker tt : taskTrackers.values()) {
        ttStatuses.add(tt.getStatus());
      }
    }
    return ttStatuses;
  }
  
  /**
   * Get the active task tracker statuses in the cluster
   *  
   * @return {@link Collection} of active {@link TaskTrackerStatus} 
   */
  // This method is synchronized to make sure that the locking order 
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers 
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public Collection<TaskTrackerStatus> activeTaskTrackers() {
    Collection<TaskTrackerStatus> activeTrackers = 
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for ( TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (!faultyTrackers.isBlacklisted(status.getHost())) {
          activeTrackers.add(status);
        }
      }
    }
    return activeTrackers;
  }
  
  /**
   * Get the active and blacklisted task tracker names in the cluster. The first
   * element in the returned list contains the list of active tracker names.
   * The second element in the returned list contains the list of blacklisted
   * tracker names. 
   */
  // This method is synchronized to make sure that the locking order 
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers 
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public List<List<String>> taskTrackerNames() {
    List<String> activeTrackers = 
      new ArrayList<String>();
    List<String> blacklistedTrackers = 
      new ArrayList<String>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (!faultyTrackers.isBlacklisted(status.getHost())) {
          activeTrackers.add(status.getTrackerName());
        } else {
          blacklistedTrackers.add(status.getTrackerName());
        }
      }
    }
    List<List<String>> result = new ArrayList<List<String>>(2);
    result.add(activeTrackers);
    result.add(blacklistedTrackers);
    return result;
  }
  
  /**
   * Get the blacklisted task tracker statuses in the cluster
   *  
   * @return {@link Collection} of blacklisted {@link TaskTrackerStatus} 
   */
  // This method is synchronized to make sure that the locking order 
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers 
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public Collection<TaskTrackerStatus> blacklistedTaskTrackers() {
    Collection<TaskTrackerStatus> blacklistedTrackers = 
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus(); 
        if (faultyTrackers.isBlacklisted(status.getHost())) {
          blacklistedTrackers.add(status);
        }
      }
    }    
    return blacklistedTrackers;
  }

  synchronized int getFaultCount(String hostName) {
    return faultyTrackers.getFaultCount(hostName);
  }
  
  /**
   * Get the number of blacklisted trackers across all the jobs
   * 
   * @return
   */
  int getBlacklistedTrackerCount() {
    return faultyTrackers.numBlacklistedTrackers;
  }

  /**
   * Whether the tracker is blacklisted or not
   * 
   * @param trackerID
   * 
   * @return true if blacklisted, false otherwise
   */
  synchronized public boolean isBlacklisted(String trackerID) {
    TaskTrackerStatus status = getTaskTrackerStatus(trackerID);
    if (status != null) {
      return faultyTrackers.isBlacklisted(status.getHost());
    }
    return false;
  }
  
  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTrackerStatus getTaskTrackerStatus(String trackerID) {
    TaskTracker taskTracker;
    synchronized (taskTrackers) {
      taskTracker = taskTrackers.get(trackerID);
    }
    return (taskTracker == null) ? null : taskTracker.getStatus();
  }

  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTracker getTaskTracker(String trackerID) {
    synchronized (taskTrackers) {
      return taskTrackers.get(trackerID);
    }
  }

  JobTrackerStatistics getStatistics() {
    return statistics;
  }
  /**
   * Adds a new node to the jobtracker. It involves adding it to the expiry
   * thread and adding it for resolution
   * 
   * Assumes JobTracker, taskTrackers and trackerExpiryQueue is locked on entry
   * 
   * @param status Task Tracker's status
   */
  private void addNewTracker(TaskTracker taskTracker) {
    TaskTrackerStatus status = taskTracker.getStatus();
    trackerExpiryQueue.add(status);

    //  Register the tracker if its not registered
    String hostname = status.getHost();
    if (getNode(status.getTrackerName()) == null) {
      // Making the network location resolution inline .. 
      resolveAndAddToTopology(hostname);
    }

    // add it to the set of tracker per host
    Set<TaskTracker> trackers = hostnameToTaskTracker.get(hostname);
    if (trackers == null) {
      trackers = Collections.synchronizedSet(new HashSet<TaskTracker>());
      hostnameToTaskTracker.put(hostname, trackers);
    }
    statistics.taskTrackerAdded(status.getTrackerName());
    getInstrumentation().addTrackers(1);
    LOG.info("Adding tracker " + status.getTrackerName() + " to host " 
             + hostname);
    trackers.add(taskTracker);
  }

  public Node resolveAndAddToTopology(String name) {
    List <String> tmpList = new ArrayList<String>(1);
    tmpList.add(name);
    List <String> rNameList = dnsToSwitchMapping.resolve(tmpList);
    String rName = rNameList.get(0);
    String networkLoc = NodeBase.normalize(rName);
    return addHostToNodeMapping(name, networkLoc);
  }
  
  private Node addHostToNodeMapping(String host, String networkLoc) {
    Node node = null;
    synchronized (nodesAtMaxLevel) {
      if ((node = clusterMap.getNode(networkLoc+"/"+host)) == null) {
        node = new NodeBase(host, networkLoc);
        clusterMap.add(node);
        if (node.getLevel() < getNumTaskCacheLevels()) {
          LOG.fatal("Got a host whose level is: " + node.getLevel() + "." 
              + " Should get at least a level of value: " 
              + getNumTaskCacheLevels());
          try {
            stopTracker();
          } catch (IOException ie) {
            LOG.warn("Exception encountered during shutdown: " 
                + StringUtils.stringifyException(ie));
            System.exit(-1);
          }
        }
        hostnameToNodeMap.put(host, node);
        // Make an entry for the node at the max level in the cache
        nodesAtMaxLevel.add(getParentNode(node, getNumTaskCacheLevels() - 1));
      }
    }
    return node;
  }

  /**
   * Returns a collection of nodes at the max level
   */
  public Collection<Node> getNodesAtMaxLevel() {
    return nodesAtMaxLevel;
  }

  public static Node getParentNode(Node node, int level) {
    for (int i = 0; i < level; ++i) {
      node = node.getParent();
    }
    return node;
  }

  /**
   * Return the Node in the network topology that corresponds to the hostname
   */
  public Node getNode(String name) {
    return hostnameToNodeMap.get(name);
  }
  public int getNumTaskCacheLevels() {
    return numTaskCacheLevels;
  }
  public int getNumResolvedTaskTrackers() {
    return numResolved;
  }
  
  public int getNumberOfUniqueHosts() {
    return uniqueHostsMap.size();
  }
  
  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }
  
  // Update the listeners about the job
  // Assuming JobTracker is locked on entry.
  private void updateJobInProgressListeners(JobChangeEvent event) {
    for (JobInProgressListener listener : jobInProgressListeners) {
      listener.jobUpdated(event);
    }
  }
  
  /**
   * Return the {@link QueueManager} associated with the JobTracker.
   */
  public QueueManager getQueueManager() {
    return queueManager;
  }
  
  ////////////////////////////////////////////////////
  // InterTrackerProtocol
  ////////////////////////////////////////////////////

  // Just returns the VersionInfo version (unlike MXBean#getVersion)
  public String getVIVersion() throws IOException {
    return VersionInfo.getVersion();
  }

  public String getBuildVersion() throws IOException {
    return VersionInfo.getBuildVersion();
  }

  /**
   * The periodic heartbeat mechanism between the {@link TaskTracker} and
   * the {@link JobTracker}.
   * 
   * The {@link JobTracker} processes the status information sent by the 
   * {@link TaskTracker} and responds with instructions to start/stop 
   * tasks or jobs, and also 'reset' instructions during contingencies. 
   */
  public synchronized HeartbeatResponse heartbeat(TaskTrackerStatus status, 
                                                  boolean restarted,
                                                  boolean initialContact,
                                                  boolean acceptNewTasks, 
                                                  short responseId) 
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got heartbeat from: " + status.getTrackerName() + 
                " (restarted: " + restarted + 
                " initialContact: " + initialContact + 
                " acceptNewTasks: " + acceptNewTasks + ")" +
                " with responseId: " + responseId);
    }

    // Make sure heartbeat is from a tasktracker allowed by the jobtracker.
    if (!acceptTaskTracker(status)) {
      throw new DisallowedTaskTrackerException(status);
    }

    // First check if the last heartbeat response got through
    String trackerName = status.getTrackerName();
    long now = clock.getTime();
    boolean isBlacklisted = false;
    if (restarted) {
      faultyTrackers.markTrackerHealthy(status.getHost());
    } else {
      isBlacklisted = 
        faultyTrackers.shouldAssignTasksToTracker(status.getHost(), now);
    }
    
    HeartbeatResponse prevHeartbeatResponse =
      trackerToHeartbeatResponseMap.get(trackerName);
    boolean addRestartInfo = false;

    if (initialContact != true) {
      // If this isn't the 'initial contact' from the tasktracker,
      // there is something seriously wrong if the JobTracker has
      // no record of the 'previous heartbeat'; if so, ask the 
      // tasktracker to re-initialize itself.
      if (prevHeartbeatResponse == null) {
        // This is the first heartbeat from the old tracker to the newly 
        // started JobTracker
        if (hasRestarted()) {
          addRestartInfo = true;
          // inform the recovery manager about this tracker joining back
          recoveryManager.unMarkTracker(trackerName);
        } else {
          // Jobtracker might have restarted but no recovery is needed
          // otherwise this code should not be reached
          LOG.warn("Serious problem, cannot find record of 'previous' " +
                   "heartbeat for '" + trackerName + 
                   "'; reinitializing the tasktracker");
          return new HeartbeatResponse(responseId, 
              new TaskTrackerAction[] {new ReinitTrackerAction()});
        }

      } else {
                
        // It is completely safe to not process a 'duplicate' heartbeat from a 
        // {@link TaskTracker} since it resends the heartbeat when rpcs are 
        // lost see {@link TaskTracker.transmitHeartbeat()};
        // acknowledge it by re-sending the previous response to let the 
        // {@link TaskTracker} go forward. 
        if (prevHeartbeatResponse.getResponseId() != responseId) {
          LOG.info("Ignoring 'duplicate' heartbeat from '" + 
              trackerName + "'; resending the previous 'lost' response");
          return prevHeartbeatResponse;
        }
      }
    }
      
    // Process this heartbeat 
    short newResponseId = (short)(responseId + 1);
    status.setLastSeen(now);
    if (!processHeartbeat(status, initialContact)) {
      if (prevHeartbeatResponse != null) {
        trackerToHeartbeatResponseMap.remove(trackerName);
      }
      return new HeartbeatResponse(newResponseId, 
                   new TaskTrackerAction[] {new ReinitTrackerAction()});
    }
      
    // Initialize the response to be sent for the heartbeat
    HeartbeatResponse response = new HeartbeatResponse(newResponseId, null);
    List<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();
    isBlacklisted = faultyTrackers.isBlacklisted(status.getHost());
    // Check for new tasks to be executed on the tasktracker
    if (recoveryManager.shouldSchedule() && acceptNewTasks && !isBlacklisted) {
      TaskTrackerStatus taskTrackerStatus = getTaskTrackerStatus(trackerName) ;
      if (taskTrackerStatus == null) {
        LOG.warn("Unknown task tracker polling; ignoring: " + trackerName);
      } else {
        List<Task> tasks = getSetupAndCleanupTasks(taskTrackerStatus);
        if (tasks == null ) {
          tasks = taskScheduler.assignTasks(taskTrackers.get(trackerName));
        }
        if (tasks != null) {
          for (Task task : tasks) {
            expireLaunchingTasks.addNewTask(task.getTaskID());
            if(LOG.isDebugEnabled()) {
              LOG.debug(trackerName + " -> LaunchTask: " + task.getTaskID());
            }
            actions.add(new LaunchTaskAction(task));
          }
        }
      }
    }
      
    // Check for tasks to be killed
    List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
    if (killTasksList != null) {
      actions.addAll(killTasksList);
    }
     
    // Check for jobs to be killed/cleanedup
    List<TaskTrackerAction> killJobsList = getJobsForCleanup(trackerName);
    if (killJobsList != null) {
      actions.addAll(killJobsList);
    }

    // Check for tasks whose outputs can be saved
    List<TaskTrackerAction> commitTasksList = getTasksToSave(status);
    if (commitTasksList != null) {
      actions.addAll(commitTasksList);
    }

    // calculate next heartbeat interval and put in heartbeat response
    int nextInterval = getNextHeartbeatInterval();
    response.setHeartbeatInterval(nextInterval);
    response.setActions(
                        actions.toArray(new TaskTrackerAction[actions.size()]));
    
    // check if the restart info is req
    if (addRestartInfo) {
      response.setRecoveredJobs(recoveryManager.getJobsToRecover());
    }
        
    // Update the trackerToHeartbeatResponseMap
    trackerToHeartbeatResponseMap.put(trackerName, response);

    // Done processing the hearbeat, now remove 'marked' tasks
    removeMarkedTasks(trackerName);
        
    return response;
  }
  
  /**
   * Calculates next heartbeat interval using cluster size.
   * Heartbeat interval is incremented by 1 second for every 100 nodes by default. 
   * @return next heartbeat interval.
   */
  public int getNextHeartbeatInterval() {
    // get the no of task trackers
    int clusterSize = getClusterStatus().getTaskTrackers();
    int heartbeatInterval =  Math.max(
                                (int)(1000 * HEARTBEATS_SCALING_FACTOR *
                                      ((double)clusterSize / 
                                                NUM_HEARTBEATS_IN_SECOND)),
                                HEARTBEAT_INTERVAL_MIN) ;
    return heartbeatInterval;
  }

  /**
   * Return if the specified tasktracker is in the hosts list, 
   * if one was configured.  If none was configured, then this 
   * returns true.
   */
  private boolean inHostsList(TaskTrackerStatus status) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || hostsList.contains(status.getHost()));
  }

  /**
   * Return if the specified tasktracker is in the exclude list.
   */
  private boolean inExcludedHostsList(TaskTrackerStatus status) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return excludeList.contains(status.getHost());
  }

  /**
   * Returns true if the tasktracker is in the hosts list and 
   * not in the exclude list. 
   */
  private boolean acceptTaskTracker(TaskTrackerStatus status) {
    return (inHostsList(status) && !inExcludedHostsList(status));
  }
    
  /**
   * Update the last recorded status for the given task tracker.
   * It assumes that the taskTrackers are locked on entry.
   * @param trackerName The name of the tracker
   * @param status The new status for the task tracker
   * @return Was an old status found?
   */
  private boolean updateTaskTrackerStatus(String trackerName,
                                          TaskTrackerStatus status) {
    TaskTracker tt = getTaskTracker(trackerName);
    TaskTrackerStatus oldStatus = (tt == null) ? null : tt.getStatus();
    if (oldStatus != null) {
      totalMaps -= oldStatus.countMapTasks();
      totalReduces -= oldStatus.countReduceTasks();
      occupiedMapSlots -= oldStatus.countOccupiedMapSlots();
      occupiedReduceSlots -= oldStatus.countOccupiedReduceSlots();
      getInstrumentation().decRunningMaps(oldStatus.countMapTasks());
      getInstrumentation().decRunningReduces(oldStatus.countReduceTasks());
      getInstrumentation().decOccupiedMapSlots(oldStatus.countOccupiedMapSlots());
      getInstrumentation().decOccupiedReduceSlots(oldStatus.countOccupiedReduceSlots());
      if (!faultyTrackers.isBlacklisted(oldStatus.getHost())) {
        int mapSlots = oldStatus.getMaxMapSlots();
        totalMapTaskCapacity -= mapSlots;
        int reduceSlots = oldStatus.getMaxReduceSlots();
        totalReduceTaskCapacity -= reduceSlots;
      }
      if (status == null) {
        taskTrackers.remove(trackerName);
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(oldStatus.getHost());
        if (numTaskTrackersInHost != null) {
          numTaskTrackersInHost --;
          if (numTaskTrackersInHost > 0)  {
            uniqueHostsMap.put(oldStatus.getHost(), numTaskTrackersInHost);
          }
          else {
            uniqueHostsMap.remove(oldStatus.getHost());
          }
        }
      }
    }
    if (status != null) {
      totalMaps += status.countMapTasks();
      totalReduces += status.countReduceTasks();
      occupiedMapSlots += status.countOccupiedMapSlots();
      occupiedReduceSlots += status.countOccupiedReduceSlots();
      getInstrumentation().addRunningMaps(status.countMapTasks());
      getInstrumentation().addRunningReduces(status.countReduceTasks());
      getInstrumentation().addOccupiedMapSlots(status.countOccupiedMapSlots());
      getInstrumentation().addOccupiedReduceSlots(status.countOccupiedReduceSlots());
      if (!faultyTrackers.isBlacklisted(status.getHost())) {
        int mapSlots = status.getMaxMapSlots();
        totalMapTaskCapacity += mapSlots;
        int reduceSlots = status.getMaxReduceSlots();
        totalReduceTaskCapacity += reduceSlots;
      }
      boolean alreadyPresent = false;
      TaskTracker taskTracker = taskTrackers.get(trackerName);
      if (taskTracker != null) {
        alreadyPresent = true;
      } else {
        taskTracker = new TaskTracker(trackerName);
      }
      
      taskTracker.setStatus(status);
      taskTrackers.put(trackerName, taskTracker);
      
      if (LOG.isDebugEnabled()) {
        int runningMaps = 0, runningReduces = 0;
        int commitPendingMaps = 0, commitPendingReduces = 0;
        int unassignedMaps = 0, unassignedReduces = 0;
        int miscMaps = 0, miscReduces = 0;
        List<TaskStatus> taskReports = status.getTaskReports();
        for (Iterator<TaskStatus> it = taskReports.iterator(); it.hasNext();) {
          TaskStatus ts = (TaskStatus) it.next();
          boolean isMap = ts.getIsMap();
          TaskStatus.State state = ts.getRunState();
          if (state == TaskStatus.State.RUNNING) {
            if (isMap) { ++runningMaps; }
            else { ++runningReduces; }
          } else if (state == TaskStatus.State.UNASSIGNED) {
            if (isMap) { ++unassignedMaps; }
            else { ++unassignedReduces; }
          } else if (state == TaskStatus.State.COMMIT_PENDING) {
            if (isMap) { ++commitPendingMaps; }
            else { ++commitPendingReduces; }
          } else {
            if (isMap) { ++miscMaps; } 
            else { ++miscReduces; } 
          }
        }
        LOG.debug(trackerName + ": Status -" +
                  " running(m) = " + runningMaps + 
                  " unassigned(m) = " + unassignedMaps + 
                  " commit_pending(m) = " + commitPendingMaps +
                  " misc(m) = " + miscMaps +
                  " running(r) = " + runningReduces + 
                  " unassigned(r) = " + unassignedReduces + 
                  " commit_pending(r) = " + commitPendingReduces +
                  " misc(r) = " + miscReduces); 
      }

      if (!alreadyPresent)  {
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(status.getHost());
        if (numTaskTrackersInHost == null) {
          numTaskTrackersInHost = 0;
        }
        numTaskTrackersInHost ++;
        uniqueHostsMap.put(status.getHost(), numTaskTrackersInHost);
      }
    }
    getInstrumentation().setMapSlots(totalMapTaskCapacity);
    getInstrumentation().setReduceSlots(totalReduceTaskCapacity);
    return oldStatus != null;
  }
  
  // Increment the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void incrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots += reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots += reservedSlots;
    }
  }

  // Decrement the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void decrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots -= reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots -= reservedSlots;
    }
  }

  private void updateNodeHealthStatus(TaskTrackerStatus trackerStatus) {
    TaskTrackerHealthStatus status = trackerStatus.getHealthStatus();
    synchronized (faultyTrackers) {
      faultyTrackers.setNodeHealthStatus(trackerStatus.getHost(), 
          status.isNodeHealthy(), status.getHealthReport());
    }
  }
    
  /**
   * Process incoming heartbeat messages from the task trackers.
   */
  private synchronized boolean processHeartbeat(
                                 TaskTrackerStatus trackerStatus, 
                                 boolean initialContact) {
    
    getInstrumentation().heartbeat();

    String trackerName = trackerStatus.getTrackerName();

    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                     trackerStatus);
        TaskTracker taskTracker = getTaskTracker(trackerName);
        if (initialContact) {
          // If it's first contact, then clear out 
          // any state hanging around
          if (seenBefore) {
            lostTaskTracker(taskTracker);
          }
        } else {
          // If not first contact, there should be some record of the tracker
          if (!seenBefore) {
            LOG.warn("Status from unknown Tracker : " + trackerName);
            updateTaskTrackerStatus(trackerName, null);
            return false;
          }
        }

        if (initialContact) {
          // if this is lost tracker that came back now, and if it blacklisted
          // increment the count of blacklisted trackers in the cluster
          if (isBlacklisted(trackerName)) {
            faultyTrackers.incrBlackListedTrackers(1);
          }
          addNewTracker(taskTracker);
        }
      }
    }

    updateTaskStatuses(trackerStatus);
    updateNodeHealthStatus(trackerStatus);
    
    return true;
  }

  /**
   * A tracker wants to know if any of its Tasks have been
   * closed (because the job completed, whether successfully or not)
   */
  private synchronized List<TaskTrackerAction> getTasksToKill(
                                                              String taskTracker) {
    
    Set<TaskAttemptID> taskIds = trackerToTaskMap.get(taskTracker);
    List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
    if (taskIds != null) {
      for (TaskAttemptID killTaskId : taskIds) {
        TaskInProgress tip = taskidToTIPMap.get(killTaskId);
        if (tip == null) {
          continue;
        }
        if (tip.shouldClose(killTaskId)) {
          // 
          // This is how the JobTracker ends a task at the TaskTracker.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (!tip.getJob().isComplete()) {
            killList.add(new KillTaskAction(killTaskId));
            if (LOG.isDebugEnabled()) {
              LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
            }
          }
        }
      }
    }
    
    // add the stray attempts for uninited jobs
    synchronized (trackerToTasksToCleanup) {
      Set<TaskAttemptID> set = trackerToTasksToCleanup.remove(taskTracker);
      if (set != null) {
        for (TaskAttemptID id : set) {
          killList.add(new KillTaskAction(id));
        }
      }
    }
    return killList;
  }

  /**
   * Add a job to cleanup for the tracker.
   */
  private void addJobForCleanup(JobID id) {
    for (String taskTracker : taskTrackers.keySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Marking job " + id + " for cleanup by tracker " + taskTracker);
      }
      synchronized (trackerToJobsToCleanup) {
        Set<JobID> jobsToKill = trackerToJobsToCleanup.get(taskTracker);
        if (jobsToKill == null) {
          jobsToKill = new HashSet<JobID>();
          trackerToJobsToCleanup.put(taskTracker, jobsToKill);
        }
        jobsToKill.add(id);
      }
    }
  }
  
  /**
   * A tracker wants to know if any job needs cleanup because the job completed.
   */
  private List<TaskTrackerAction> getJobsForCleanup(String taskTracker) {
    Set<JobID> jobs = null;
    synchronized (trackerToJobsToCleanup) {
      jobs = trackerToJobsToCleanup.remove(taskTracker);
    }
    if (jobs != null) {
      // prepare the actions list
      List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
      for (JobID killJobId : jobs) {
        killList.add(new KillJobAction(killJobId));
        if(LOG.isDebugEnabled()) {
          LOG.debug(taskTracker + " -> KillJobAction: " + killJobId);
        }
      }

      return killList;
    }
    return null;
  }

  /**
   * A tracker wants to know if any of its Tasks can be committed 
   */
  private synchronized List<TaskTrackerAction> getTasksToSave(
                                                 TaskTrackerStatus tts) {
    List<TaskStatus> taskStatuses = tts.getTaskReports();
    if (taskStatuses != null) {
      List<TaskTrackerAction> saveList = new ArrayList<TaskTrackerAction>();
      for (TaskStatus taskStatus : taskStatuses) {
        if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING) {
          TaskAttemptID taskId = taskStatus.getTaskID();
          TaskInProgress tip = taskidToTIPMap.get(taskId);
          if (tip == null) {
            continue;
          }
          if (tip.shouldCommit(taskId)) {
            saveList.add(new CommitTaskAction(taskId));
            if (LOG.isDebugEnabled()) {
              LOG.debug(tts.getTrackerName() + 
                      " -> CommitTaskAction: " + taskId);
            }
          }
        }
      }
      return saveList;
    }
    return null;
  }
  
  // returns cleanup tasks first, then setup tasks.
  synchronized List<Task> getSetupAndCleanupTasks(
    TaskTrackerStatus taskTracker) throws IOException {
    int maxMapTasks = taskTracker.getMaxMapSlots();
    int maxReduceTasks = taskTracker.getMaxReduceSlots();
    int numMaps = taskTracker.countOccupiedMapSlots();
    int numReduces = taskTracker.countOccupiedReduceSlots();
    int numTaskTrackers = getClusterStatus().getTaskTrackers();
    int numUniqueHosts = getNumberOfUniqueHosts();

    Task t = null;
    synchronized (jobs) {
      if (numMaps < maxMapTasks) {
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                  numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
      if (numReduces < maxReduceTasks) {
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
    }
    return null;
  }

  /**
   * Grab the local fs name
   */
  public synchronized String getFilesystemName() throws IOException {
    if (fs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return fs.getUri().toString();
  }

  /**
   * Returns a handle to the JobTracker's Configuration
   */
  public JobConf getConf() {
    return conf;
  }
  
  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) throws IOException {
    LOG.warn("Report from " + taskTracker + ": " + errorMessage);        
  }

  /**
   * Remove the job_ from jobids to get the unique string.
   */
  static String getJobUniqueString(String jobid) {
    return jobid.substring(4);
  }

  ////////////////////////////////////////////////////
  // JobSubmissionProtocol
  ////////////////////////////////////////////////////

  /**
   * Allocates a new JobId string.
   */
  public synchronized JobID getNewJobId() throws IOException {
    return new JobID(getTrackerIdentifier(), nextJobId++);
  }

  /**
   * JobTracker.submitJob() kicks off a new job.  
   *
   * Create a 'JobInProgress' object, which contains both JobProfile
   * and JobStatus.  Those two sub-objects are sometimes shipped outside
   * of the JobTracker.  But JobInProgress adds info that's useful for
   * the JobTracker alone.
   */
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
      throws IOException {
    JobInfo jobInfo = null;
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    synchronized (this) {
      if (jobs.containsKey(jobId)) {
        // job already running, don't start twice
        return jobs.get(jobId).getStatus();
      }
      jobInfo = new JobInfo(jobId, new Text(ugi.getShortUserName()),
          new Path(jobSubmitDir));
    }
    // Create the JobInProgress, do not lock the JobTracker since
    // we are about to copy job.xml from HDFS
    JobInProgress job = null;
    try {
      job = new JobInProgress(this, this.conf, jobInfo, 0, ts);
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    synchronized (this) {
      String queue = job.getProfile().getQueueName();
      if (!(queueManager.getQueues().contains(queue))) {
        job.fail();
        throw new IOException("Queue \"" + queue + "\" does not exist");
      }

      // check if queue is RUNNING
      if (!queueManager.isRunning(queue)) {
        throw new IOException("Queue \"" + queue + "\" is not running");
      }
      try {
        aclsManager.checkAccess(job, ugi, Operation.SUBMIT_JOB);
      } catch (IOException ioe) {
        LOG.warn("Access denied for user " + job.getJobConf().getUser()
            + ". Ignoring job " + jobId, ioe);
        job.fail();
        throw ioe;
      }

      try {
        this.taskScheduler.checkJobSubmission(job);
      } catch (IOException ioe){
        LOG.error("Problem in submitting job " + jobId, ioe);
        job.fail();
        throw ioe;
      }

      // Check the job if it cannot run in the cluster because of invalid memory
      // requirements.
      try {
        checkMemoryRequirements(job);
      } catch (IOException ioe) {
        throw ioe;
      }
      boolean recovered = true; // TODO: Once the Job recovery code is there,
      // (MAPREDUCE-873) we
      // must pass the "recovered" flag accurately.
      // This is handled in the trunk/0.22
      if (!recovered) {
        // Store the information in a file so that the job can be recovered
        // later (if at all)
        Path jobDir = getSystemDirectoryForJob(jobId);
        FileSystem.mkdirs(fs, jobDir, new FsPermission(SYSTEM_DIR_PERMISSION));
        FSDataOutputStream out = fs.create(getSystemFileForJob(jobId));
        jobInfo.write(out);
        out.close();
      }
      return addJob(jobId, job);
    }
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getStagingAreaDir()
   */
  public String getStagingAreaDir() throws IOException {
    try{
      final String user =
        UserGroupInformation.getCurrentUser().getShortUserName();
      return getMROwner().doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws Exception {
          return getStagingAreaDirInternal(user);
        }
      });
    } catch(InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  private String getStagingAreaDirInternal(String user) throws IOException {
    final Path stagingRootDir =
      new Path(conf.get("mapreduce.jobtracker.staging.root.dir",
            "/tmp/hadoop/mapred/staging"));
    final FileSystem fs = stagingRootDir.getFileSystem(conf);
    return fs.makeQualified(new Path(stagingRootDir,
                              user+"/.staging")).toString();
  }

  /**
   * Adds a job to the jobtracker. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * @param jobId The id for the job submitted which needs to be added
   */
  private synchronized JobStatus addJob(JobID jobId, JobInProgress job) {
    totalSubmissions++;

    synchronized (jobs) {
      synchronized (taskScheduler) {
        jobs.put(job.getProfile().getJobID(), job);
        for (JobInProgressListener listener : jobInProgressListeners) {
          try {
            listener.jobAdded(job);
          } catch (IOException ioe) {
            LOG.warn("Failed to add and so skipping the job : "
                + job.getJobID() + ". Exception : " + ioe);
          }
        }
      }
    }
    myInstrumentation.submitJob(job.getJobConf(), jobId);
    LOG.info("Job " + jobId + " added successfully for user '" 
             + job.getJobConf().getUser() + "' to queue '" 
             + job.getJobConf().getQueueName() + "'");
    AuditLogger.logSuccess(job.getUser(), 
        Operation.SUBMIT_JOB.name(), jobId.toString());
    return job.getStatus();
  }

  /**
   * Are ACLs for authorization checks enabled on the JT?
   * 
   * @return
   */
  boolean areACLsEnabled() {
    return conf.getBoolean(JobConf.MR_ACLS_ENABLED, false);
  }

  /**@deprecated use {@link #getClusterStatus(boolean)}*/
  @Deprecated
  public synchronized ClusterStatus getClusterStatus() {
    return getClusterStatus(false);
  }

  public synchronized ClusterStatus getClusterStatus(boolean detailed) {
    synchronized (taskTrackers) {
      if (detailed) {
        List<List<String>> trackerNames = taskTrackerNames();
        return new ClusterStatus(trackerNames.get(0),
            trackerNames.get(1),
            TASKTRACKER_EXPIRY_INTERVAL,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity, 
            state, getExcludedNodes().size()
            );
      } else {
        return new ClusterStatus(taskTrackers.size() - 
            getBlacklistedTrackerCount(),
            getBlacklistedTrackerCount(),
            TASKTRACKER_EXPIRY_INTERVAL,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity, 
            state, getExcludedNodes().size());          
      }
    }
  }

  public synchronized ClusterMetrics getClusterMetrics() {
    return new ClusterMetrics(totalMaps,
      totalReduces, occupiedMapSlots, occupiedReduceSlots,
      reservedMapSlots, reservedReduceSlots,
      totalMapTaskCapacity, totalReduceTaskCapacity,
      totalSubmissions,
      taskTrackers.size() - getBlacklistedTrackerCount(), 
      getBlacklistedTrackerCount(), getExcludedNodes().size()) ;
  }

  /**
   * @see JobSubmissionProtocol#killJob
   */
  public synchronized void killJob(JobID jobid) throws IOException {
    if (null == jobid) {
      LOG.info("Null jobid object sent to JobTracker.killJob()");
      return;
    }
    
    JobInProgress job = jobs.get(jobid);
    
    if (null == job) {
      LOG.info("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }
        
    // check both queue-level and job-level access
    aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
        Operation.KILL_JOB);

    killJob(job);
  }
  
  private synchronized void killJob(JobInProgress job) {
    LOG.info("Killing job " + job.getJobID());
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    job.kill();
    
    // Inform the listeners if the job is killed
    // Note : 
    //   If the job is killed in the PREP state then the listeners will be 
    //   invoked
    //   If the job is killed in the RUNNING state then cleanup tasks will be 
    //   launched and the updateTaskStatuses() will take care of it
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()
        && newStatus.getRunState() == JobStatus.KILLED) {
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
            newStatus);
      updateJobInProgressListeners(event);
    }
  }
  /**
   * Discard a current delegation token.
   */ 
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                       ) throws IOException,
                                                InterruptedException {
    String user = UserGroupInformation.getCurrentUser().getUserName();
    secretManager.cancelToken(token, user);
  }  
  /**
   * Get a new delegation token.
   */ 
  @Override
  public Token<DelegationTokenIdentifier> 
     getDelegationToken(Text renewer
                        )throws IOException, InterruptedException {
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be issued only with kerberos authentication");
    }
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Text owner = new Text(ugi.getUserName());
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }  
    DelegationTokenIdentifier ident =  
      new DelegationTokenIdentifier(owner, renewer, realUser);
    return new Token<DelegationTokenIdentifier>(ident, secretManager);
  }  
  /**
   * Renew a delegation token to extend its lifetime.
   */ 
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                      ) throws IOException,
                                               InterruptedException {
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be issued only with kerberos authentication");
    }
    String user = UserGroupInformation.getCurrentUser().getUserName();
    return secretManager.renewToken(token, user);
  }  

  public void initJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Init on null job is not valid");
      return;
    }
	        
    try {
      JobStatus prevStatus = (JobStatus)job.getStatus().clone();
      LOG.info("Initializing " + job.getJobID());
      job.initTasks();
      // Inform the listeners if the job state has changed
      // Note : that the job will be in PREP state.
      JobStatus newStatus = (JobStatus)job.getStatus().clone();
      if (prevStatus.getRunState() != newStatus.getRunState()) {
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
              newStatus);
        synchronized (JobTracker.this) {
          updateJobInProgressListeners(event);
        }
      }
    } catch (KillInterruptedException kie) {
      //   If job was killed during initialization, job state will be KILLED
      LOG.error("Job initialization interrupted:\n" +
          StringUtils.stringifyException(kie));
      killJob(job);
    } catch (Throwable t) {
      String failureInfo = 
        "Job initialization failed:\n" + StringUtils.stringifyException(t);
      // If the job initialization is failed, job state will be FAILED
      LOG.error(failureInfo);
      job.getStatus().setFailureInfo(failureInfo);
      failJob(job);
    }
	 }

  /**
   * Fail a job and inform the listeners. Other components in the framework 
   * should use this to fail a job.
   */
  public synchronized void failJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Fail on null job is not valid");
      return;
    }
         
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    LOG.info("Failing job " + job.getJobID());
    job.fail();
     
    // Inform the listeners if the job state has changed
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()) {
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
            newStatus);
      updateJobInProgressListeners(event);
    }
  }
  
  public synchronized void setJobPriority(JobID jobid, 
                                          String priority)
                                          throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (null == job) {
        LOG.info("setJobPriority(): JobId " + jobid.toString()
            + " is not a valid job");
        return;
    }

    JobPriority newPriority = JobPriority.valueOf(priority);
    setJobPriority(jobid, newPriority);
  }
                           
  void storeCompletedJob(JobInProgress job) {
    //persists the job info in DFS
    completedJobStatusStore.store(job);
  }

  /**
   * Check if the <code>job</code> has been initialized.
   * 
   * @param job {@link JobInProgress} to be checked
   * @return <code>true</code> if the job has been initialized,
   *         <code>false</code> otherwise
   */
  private boolean isJobInited(JobInProgress job) {
    return job.inited(); 
  }
  
  public JobProfile getJobProfile(JobID jobid) {
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getProfile while holding the lock
        // on the JobTracker since it isn't a synchronized method
        return job.getProfile();
      }  else {
        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          return info.profile;
        }
      }
    }
    return completedJobStatusStore.readJobProfile(jobid);
  }
  
  public JobStatus getJobStatus(JobID jobid) {
    if (null == jobid) {
      LOG.warn("JobTracker.getJobStatus() cannot get status for null jobid");
      return null;
    }
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getStatus while holding the lock
        // on the JobTracker since it isn't a synchronized method
        return job.getStatus();
      } else {
        
        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          return info.status;
        }
      }
    }
    return completedJobStatusStore.readJobStatus(jobid);
  }
  
  private static final Counters EMPTY_COUNTERS = new Counters();
  public Counters getJobCounters(JobID jobid) throws IOException {
    UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {

        // check the job-access
        aclsManager.checkAccess(job, callerUGI, Operation.VIEW_JOB_COUNTERS);
        Counters counters = new Counters();
        if (isJobInited(job)) {
          boolean isFine = job.getCounters(counters);
          if (!isFine) {
            throw new IOException("Counters Exceeded limit: " + 
                Counters.MAX_COUNTER_LIMIT);
          }
          return counters;
        }
        else {
          return EMPTY_COUNTERS;
        }
      } else {
        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          return info.counters;
        }
      }
    }

    return completedJobStatusStore.readCounters(jobid);
  }
  
  private static final TaskReport[] EMPTY_TASK_REPORTS = new TaskReport[0];
  
  public synchronized TaskReport[] getMapTaskReports(JobID jobid)
      throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeMapTasks =
        job.reportTasksInProgress(true, true);
      for (Iterator it = completeMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteMapTasks =
        job.reportTasksInProgress(true, false);
      for (Iterator it = incompleteMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  public synchronized TaskReport[] getReduceTaskReports(JobID jobid)
      throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector completeReduceTasks = job.reportTasksInProgress(false, true);
      for (Iterator it = completeReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector incompleteReduceTasks = job.reportTasksInProgress(false, false);
      for (Iterator it = incompleteReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  public synchronized TaskReport[] getCleanupTaskReports(JobID jobid)
      throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportCleanupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip = it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportCleanupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip = it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  
  }
  
  public synchronized TaskReport[] getSetupTaskReports(JobID jobid)
      throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportSetupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip =  it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportSetupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip =  it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }
  
  public static final String MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY =
      "mapred.cluster.map.memory.mb";
  public static final String MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY =
      "mapred.cluster.reduce.memory.mb";

  static final String MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY =
      "mapred.cluster.max.map.memory.mb";
  static final String MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY =
      "mapred.cluster.max.reduce.memory.mb";

  /* 
   * Returns a list of TaskCompletionEvent for the given job, 
   * starting from fromEventId.
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getTaskCompletionEvents(java.lang.String, int, int)
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(
      JobID jobid, int fromEventId, int maxEvents) throws IOException{
    JobInProgress job = this.jobs.get(jobid);
      
    if (null != job) {
      return isJobInited(job) ? 
          job.getTaskCompletionEvents(fromEventId, maxEvents) : 
          TaskCompletionEvent.EMPTY_ARRAY;
    }

    return completedJobStatusStore.readJobTaskCompletionEvents(jobid, 
                                                               fromEventId, 
                                                               maxEvents);
  }

  private static final String[] EMPTY_TASK_DIAGNOSTICS = new String[0];
  /**
   * Get the diagnostics for a given task
   * @param taskId the id of the task
   * @return an array of the diagnostic messages
   */
  public synchronized String[] getTaskDiagnostics(TaskAttemptID taskId)  
    throws IOException {
    List<String> taskDiagnosticInfo = null;
    JobID jobId = taskId.getJobID();
    TaskID tipId = taskId.getTaskID();
    JobInProgress job = jobs.get(jobId);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job != null && isJobInited(job)) {
      TaskInProgress tip = job.getTaskInProgress(tipId);
      if (tip != null) {
        taskDiagnosticInfo = tip.getDiagnosticInfo(taskId);
      }
      
    }
    
    return ((taskDiagnosticInfo == null) ? EMPTY_TASK_DIAGNOSTICS :
             taskDiagnosticInfo.toArray(new String[taskDiagnosticInfo.size()]));
  }
    
  /** Get all the TaskStatuses from the tipid. */
  TaskStatus[] getTaskStatuses(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? new TaskStatus[0] 
            : tip.getTaskStatuses());
  }

  /** Returns the TaskStatus for a particular taskid. */
  TaskStatus getTaskStatus(TaskAttemptID taskid) {
    TaskInProgress tip = getTip(taskid.getTaskID());
    return (tip == null ? null 
            : tip.getTaskStatus(taskid));
  }
    
  /**
   * Returns the counters for the specified task in progress.
   */
  Counters getTipCounters(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? null : tip.getCounters());
  }

  /**
   * Returns the configured task scheduler for this job tracker.
   * @return the configured task scheduler
   */
  TaskScheduler getTaskScheduler() {
    return taskScheduler;
  }
  
  /**
   * Returns specified TaskInProgress, or null.
   */
  public TaskInProgress getTip(TaskID tipid) {
    JobInProgress job = jobs.get(tipid.getJobID());
    return (job == null ? null : job.getTaskInProgress(tipid));
  }
    
  /**
   * @see JobSubmissionProtocol#killTask(TaskAttemptID, boolean)
   */
  public synchronized boolean killTask(TaskAttemptID taskid, boolean shouldFail)
      throws IOException {
    TaskInProgress tip = taskidToTIPMap.get(taskid);
    if(tip != null) {
      // check both queue-level and job-level access
      aclsManager.checkAccess(tip.getJob(),
          UserGroupInformation.getCurrentUser(),
          shouldFail ? Operation.FAIL_TASK : Operation.KILL_TASK);

      return tip.killTask(taskid, shouldFail);
    }
    else {
      LOG.info("Kill task attempt failed since task " + taskid + " was not found");
      return false;
    }
  }
  
  /**
   * Get tracker name for a given task id.
   * @param taskId the name of the task
   * @return The name of the task tracker
   */
  public synchronized String getAssignedTracker(TaskAttemptID taskId) {
    return taskidToTrackerMap.get(taskId);
  }
    
  public JobStatus[] jobsToComplete() {
    return getJobStatus(jobs.values(), true);
  } 
  /**
   * @see JobSubmissionProtocol#getAllJobs()
   */
  public JobStatus[] getAllJobs() {
    List<JobStatus> list = new ArrayList<JobStatus>();
    list.addAll(Arrays.asList(getJobStatus(jobs.values(),false)));
    list.addAll(retireJobs.getAllJobStatus());
    return list.toArray(new JobStatus[list.size()]);
  }
    
  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getQueueAdmins(String)
   */
  public AccessControlList getQueueAdmins(String queueName) throws IOException {
    AccessControlList acl =
        queueManager.getQueueACL(queueName, QueueACL.ADMINISTER_JOBS);
    if (acl == null) {
      acl = new AccessControlList(" ");
    }
    return acl;
  }

  ///////////////////////////////////////////////////////////////
  // JobTracker methods
  ///////////////////////////////////////////////////////////////
  public JobInProgress getJob(JobID jobid) {
    return jobs.get(jobid);
  }

  // Get the job directory in system directory
  Path getSystemDirectoryForJob(JobID id) {
    return new Path(getSystemDir(), id.toString());
  }
  
  //Get the job token file in system directory
  Path getSystemFileForJob(JobID id) {
    return new Path(getSystemDirectoryForJob(id)+"/" + JOB_INFO_FILE);
  }

  /**
   * Change the run-time priority of the given job.
   * 
   * @param jobId job id
   * @param priority new {@link JobPriority} for the job
   * @throws IOException
   * @throws AccessControlException
   */
  synchronized void setJobPriority(JobID jobId, JobPriority priority)
      throws AccessControlException, IOException {
    JobInProgress job = jobs.get(jobId);
    if (job != null) {

      // check both queue-level and job-level access
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.SET_JOB_PRIORITY);

      synchronized (taskScheduler) {
        JobStatus oldStatus = (JobStatus)job.getStatus().clone();
        job.setPriority(priority);
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.PRIORITY_CHANGED, oldStatus, 
                                   newStatus);
        updateJobInProgressListeners(event);
      }
    } else {
      LOG.warn("Trying to change the priority of an unknown job: " + jobId);
    }
  }
  
  ////////////////////////////////////////////////////
  // Methods to track all the TaskTrackers
  ////////////////////////////////////////////////////
  /**
   * Accept and process a new TaskTracker profile.  We might
   * have known about the TaskTracker previously, or it might
   * be brand-new.  All task-tracker structures have already
   * been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void updateTaskStatuses(TaskTrackerStatus status) {
    String trackerName = status.getTrackerName();
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(trackerName);
      TaskAttemptID taskId = report.getTaskID();
      
      // expire it
      expireLaunchingTasks.removeTask(taskId);
      
      JobInProgress job = getJob(taskId.getJobID());
      if (job == null) {
        // if job is not there in the cleanup list ... add it
        synchronized (trackerToJobsToCleanup) {
          Set<JobID> jobs = trackerToJobsToCleanup.get(trackerName);
          if (jobs == null) {
            jobs = new HashSet<JobID>();
            trackerToJobsToCleanup.put(trackerName, jobs);
          }
          jobs.add(taskId.getJobID());
        }
        continue;
      }
      
      if (!job.inited()) {
        // if job is not yet initialized ... kill the attempt
        synchronized (trackerToTasksToCleanup) {
          Set<TaskAttemptID> tasks = trackerToTasksToCleanup.get(trackerName);
          if (tasks == null) {
            tasks = new HashSet<TaskAttemptID>();
            trackerToTasksToCleanup.put(trackerName, tasks);
          }
          tasks.add(taskId);
        }
        continue;
      }

      TaskInProgress tip = taskidToTIPMap.get(taskId);
      // Check if the tip is known to the jobtracker. In case of a restarted
      // jt, some tasks might join in later
      if (tip != null || hasRestarted()) {
        if (tip == null) {
          tip = job.getTaskInProgress(taskId.getTaskID());
          job.addRunningTaskToTIP(tip, taskId, status, false);
        }
        
        // Update the job and inform the listeners if necessary
        JobStatus prevStatus = (JobStatus)job.getStatus().clone();
        // Clone TaskStatus object here, because JobInProgress
        // or TaskInProgress can modify this object and
        // the changes should not get reflected in TaskTrackerStatus.
        // An old TaskTrackerStatus is used later in countMapTasks, etc.
        job.updateTaskStatus(tip, (TaskStatus)report.clone());
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        
        // Update the listeners if an incomplete job completes
        if (prevStatus.getRunState() != newStatus.getRunState()) {
          JobStatusChangeEvent event = 
            new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, 
                                     prevStatus, newStatus);
          updateJobInProgressListeners(event);
        }
      } else {
        LOG.info("Serious problem.  While updating status, cannot find taskid " 
                 + report.getTaskID());
      }
      
      // Process 'failed fetch' notifications 
      List<TaskAttemptID> failedFetchMaps = report.getFetchFailedMaps();
      if (failedFetchMaps != null) {
        for (TaskAttemptID mapTaskId : failedFetchMaps) {
          TaskInProgress failedFetchMap = taskidToTIPMap.get(mapTaskId);
          
          if (failedFetchMap != null) {
            // Gather information about the map which has to be failed, if need be
            String failedFetchTrackerName = getAssignedTracker(mapTaskId);
            if (failedFetchTrackerName == null) {
              failedFetchTrackerName = "Lost task tracker";
            }
            failedFetchMap.getJob().fetchFailureNotification(failedFetchMap,
                                                             mapTaskId,
                                                             failedFetchTrackerName,
                                                             taskId,
                                                             trackerName);
          }
        }
      }
    }
  }

  /**
   * We lost the task tracker!  All task-tracker structures have 
   * already been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void lostTaskTracker(TaskTracker taskTracker) {
    String trackerName = taskTracker.getTrackerName();
    LOG.info("Lost tracker '" + trackerName + "'");
    
    // remove the tracker from the local structures
    synchronized (trackerToJobsToCleanup) {
      trackerToJobsToCleanup.remove(trackerName);
    }
    
    synchronized (trackerToTasksToCleanup) {
      trackerToTasksToCleanup.remove(trackerName);
    }
    
    // Inform the recovery manager
    recoveryManager.unMarkTracker(trackerName);
    
    Set<TaskAttemptID> lostTasks = trackerToTaskMap.get(trackerName);
    trackerToTaskMap.remove(trackerName);

    if (lostTasks != null) {
      // List of jobs which had any of their tasks fail on this tracker
      Set<JobInProgress> jobsWithFailures = new HashSet<JobInProgress>(); 
      for (TaskAttemptID taskId : lostTasks) {
        TaskInProgress tip = taskidToTIPMap.get(taskId);
        JobInProgress job = tip.getJob();

        // Completed reduce tasks never need to be failed, because 
        // their outputs go to dfs
        // And completed maps with zero reducers of the job 
        // never need to be failed. 
        if (!tip.isComplete() || 
            (tip.isMapTask() && !tip.isJobSetupTask() && 
             job.desiredReduces() != 0)) {
          // if the job is done, we don't want to change anything
          if (job.getStatus().getRunState() == JobStatus.RUNNING ||
              job.getStatus().getRunState() == JobStatus.PREP) {
            // the state will be KILLED_UNCLEAN, if the task(map or reduce) 
            // was RUNNING on the tracker
            TaskStatus.State killState = (tip.isRunningTask(taskId) && 
              !tip.isJobSetupTask() && !tip.isJobCleanupTask()) ? 
              TaskStatus.State.KILLED_UNCLEAN : TaskStatus.State.KILLED;
            job.failedTask(tip, taskId, ("Lost task tracker: " + trackerName), 
                           (tip.isMapTask() ? 
                               TaskStatus.Phase.MAP : 
                               TaskStatus.Phase.REDUCE), 
                            killState,
                            trackerName);
            jobsWithFailures.add(job);
          }
        } else {
          // Completed 'reduce' task and completed 'maps' with zero 
          // reducers of the job, not failed;
          // only removed from data-structures.
          markCompletedTaskAttempt(trackerName, taskId);
        }
      }
      
      // Penalize this tracker for each of the jobs which   
      // had any tasks running on it when it was 'lost' 
      // Also, remove any reserved slots on this tasktracker
      for (JobInProgress job : jobsWithFailures) {
        job.addTrackerTaskFailure(trackerName, taskTracker);
      }

      // Cleanup
      taskTracker.cancelAllReservations();

      // Purge 'marked' tasks, needs to be done  
      // here to prevent hanging references!
      removeMarkedTasks(trackerName);
    }
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.
   */
  public synchronized void refreshNodes() throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    // check access
    if (!aclsManager.isMRAdmin(UserGroupInformation.getCurrentUser())) {
      AuditLogger.logFailure(user, Constants.REFRESH_NODES, 
          aclsManager.getAdminsAcl().toString(), Constants.JOBTRACKER, 
          Constants.UNAUTHORIZED_USER);
      throw new AccessControlException(user + 
                                       " is not authorized to refresh nodes.");
    }
    
    AuditLogger.logSuccess(user, Constants.REFRESH_NODES, Constants.JOBTRACKER);
    // call the actual api
    refreshHosts();
  }

  UserGroupInformation getMROwner() {
    return aclsManager.getMROwner();
  }

  private synchronized void refreshHosts() throws IOException {
    // Reread the config to get mapred.hosts and mapred.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    LOG.info("Refreshing hosts information");
    Configuration conf = new Configuration();

    hostsReader.updateFileNames(conf.get("mapred.hosts",""), 
                                conf.get("mapred.hosts.exclude", ""));
    hostsReader.refresh();
    
    Set<String> excludeSet = new HashSet<String>();
    for(Map.Entry<String, TaskTracker> eSet : taskTrackers.entrySet()) {
      String trackerName = eSet.getKey();
      TaskTrackerStatus status = eSet.getValue().getStatus();
      // Check if not include i.e not in host list or in hosts list but excluded
      if (!inHostsList(status) || inExcludedHostsList(status)) {
          excludeSet.add(status.getHost()); // add to rejected trackers
      }
    }
    decommissionNodes(excludeSet);
  }

  // Assumes JobTracker, taskTrackers and trackerExpiryQueue is locked on entry
  // Remove a tracker from the system
  private void removeTracker(TaskTracker tracker) {
    String trackerName = tracker.getTrackerName();
    // Remove completely after marking the tasks as 'KILLED'
    lostTaskTracker(tracker);
    // tracker is lost, and if it is blacklisted, remove 
    // it from the count of blacklisted trackers in the cluster
    if (isBlacklisted(trackerName)) {
     faultyTrackers.decrBlackListedTrackers(1);
    }
    updateTaskTrackerStatus(trackerName, null);
    statistics.taskTrackerRemoved(trackerName);
    getInstrumentation().decTrackers(1);
  }
  
  // main decommission
  synchronized void decommissionNodes(Set<String> hosts) 
  throws IOException {  
    LOG.info("Decommissioning " + hosts.size() + " nodes");
    // create a list of tracker hostnames
    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        int trackersDecommissioned = 0;
        for (String host : hosts) {
          LOG.info("Decommissioning host " + host);
          Set<TaskTracker> trackers = hostnameToTaskTracker.remove(host);
          if (trackers != null) {
            for (TaskTracker tracker : trackers) {
              LOG.info("Decommission: Losing tracker " + tracker.getTrackerName() + 
                       " on host " + host);
              removeTracker(tracker);
            }
            trackersDecommissioned += trackers.size();
          }
          LOG.info("Host " + host + " is ready for decommissioning");
        }
        getInstrumentation().setDecommissionedTrackers(trackersDecommissioned);
      }
    }
  }

  /**
   * Returns a set of excluded nodes.
   */
  Collection<String> getExcludedNodes() {
    return hostsReader.getExcludedHosts();
  }

  /**
   * Get the localized job file path on the job trackers local file system
   * @param jobId id of the job
   * @return the path of the job conf file on the local file system
   */
  public static String getLocalJobFilePath(JobID jobId){
    return JobHistory.JobInfo.getLocalJobFilePath(jobId);
  }
  ////////////////////////////////////////////////////////////
  // main()
  ////////////////////////////////////////////////////////////

  /**
   * Start the JobTracker process.  This is used only for debugging.  As a rule,
   * JobTracker should be run as part of the DFS Namenode process.
   */
  public static void main(String argv[]
                          ) throws IOException, InterruptedException {
    StringUtils.startupShutdownMessage(JobTracker.class, argv, LOG);
    
    try {
      if(argv.length == 0) {
        JobTracker tracker = startTracker(new JobConf());
        tracker.offerService();
      }
      else {
        if ("-dumpConfiguration".equals(argv[0]) && argv.length == 1) {
          dumpConfiguration(new PrintWriter(System.out));
        }
        else {
          System.out.println("usage: JobTracker [-dumpConfiguration]");
          System.exit(-1);
        }
      }
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
  /**
   * Dumps the configuration properties in Json format
   * @param writer {@link}Writer object to which the output is written
   * @throws IOException
   */
  private static void dumpConfiguration(Writer writer) throws IOException {
    Configuration.dumpConfiguration(new JobConf(), writer);
    writer.write("\n");
    // get the QueueManager configuration properties
    QueueManager.dumpConfiguration(writer);
    writer.write("\n");
  }

  @Override
  public JobQueueInfo[] getQueues() throws IOException {
    return queueManager.getJobQueueInfos();
  }


  @Override
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return queueManager.getJobQueueInfo(queue);
  }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) throws IOException {
    Collection<JobInProgress> jips = taskScheduler.getJobs(queue);
    return getJobStatus(jips,false);
  }
  
  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException{
    return queueManager.getQueueAcls(
            UserGroupInformation.getCurrentUser());
  }
  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if(jips == null || jips.isEmpty()) {
      return new JobStatus[]{};
    }
    ArrayList<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for(JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();
      status.setStartTime(jip.getStartTime());
      status.setUsername(jip.getProfile().getUser());
      if(toComplete) {
        if(status.getRunState() == JobStatus.RUNNING || 
            status.getRunState() == JobStatus.PREP) {
          jobStatusList.add(status);
        }
      }else {
        jobStatusList.add(status);
      }
    }
    return  jobStatusList.toArray(
        new JobStatus[jobStatusList.size()]);
  }

  /**
   * Returns the confgiured maximum number of tasks for a single job
   */
  int getMaxTasksPerJob() {
    return conf.getInt("mapred.jobtracker.maxtasks.per.job", -1);
  }
  
  @Override
  public void refreshServiceAcl() throws IOException {
    if (!conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }
    this.interTrackerServer.refreshServiceAcl(conf, new MapReducePolicyProvider());
  }

  private void initializeTaskMemoryRelatedConfig() {
    memSizeForMapSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    memSizeForReduceSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));

    if (conf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY)+
          " instead use "+JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY+
          " and " + JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY
      );

      limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      if (limitMaxMemForMapTasks != JobConf.DISABLED_MEMORY_LIMIT &&
        limitMaxMemForMapTasks >= 0) {
        limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
          limitMaxMemForMapTasks /
            (1024 * 1024); //Converting old values in bytes to MB
      }
    } else {
      limitMaxMemForMapTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    }

    LOG.info(new StringBuilder().append("Scheduler configured with ").append(
        "(memSizeForMapSlotOnJT, memSizeForReduceSlotOnJT,").append(
        " limitMaxMemForMapTasks, limitMaxMemForReduceTasks) (").append(
        memSizeForMapSlotOnJT).append(", ").append(memSizeForReduceSlotOnJT)
        .append(", ").append(limitMaxMemForMapTasks).append(", ").append(
            limitMaxMemForReduceTasks).append(")"));
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() {
    LOG.info("Refreshing superuser proxy groups mapping ");
    
    ProxyUsers.refreshSuperUserGroupsConfiguration();
  }
  
  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting groups for user " + user);
    }
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }
    
  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    
    Groups.getUserToGroupsMappingService().refresh();
  }
  
  private boolean perTaskMemoryConfigurationSetOnJT() {
    if (limitMaxMemForMapTasks == JobConf.DISABLED_MEMORY_LIMIT
        || limitMaxMemForReduceTasks == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForMapSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForReduceSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
  }

  /**
   * Check the job if it has invalid requirements and throw and IOException if does have.
   * 
   * @param job
   * @throws IOException 
   */
  private void checkMemoryRequirements(JobInProgress job)
      throws IOException {
    if (!perTaskMemoryConfigurationSetOnJT()) {
      LOG.debug("Per-Task memory configuration is not set on JT. "
          + "Not checking the job for invalid memory requirements.");
      return;
    }

    boolean invalidJob = false;
    String msg = "";
    long maxMemForMapTask = job.getMemoryForMapTask();
    long maxMemForReduceTask = job.getMemoryForReduceTask();

    if (maxMemForMapTask == JobConf.DISABLED_MEMORY_LIMIT
        || maxMemForReduceTask == JobConf.DISABLED_MEMORY_LIMIT) {
      invalidJob = true;
      msg = "Invalid job requirements.";
    }

    if (maxMemForMapTask > limitMaxMemForMapTasks
        || maxMemForReduceTask > limitMaxMemForReduceTasks) {
      invalidJob = true;
      msg = "Exceeds the cluster's max-memory-limit.";
    }

    if (invalidJob) {
      StringBuilder jobStr =
          new StringBuilder().append(job.getJobID().toString()).append("(")
              .append(maxMemForMapTask).append(" memForMapTasks ").append(
                  maxMemForReduceTask).append(" memForReduceTasks): ");
      LOG.warn(jobStr.toString() + msg);

      throw new IOException(jobStr.toString() + msg);
    }
  }

  @Override
  public void refreshQueues() throws IOException {
    LOG.info("Refreshing queue information. requested by : " +
        UserGroupInformation.getCurrentUser().getShortUserName());
    this.queueManager.refreshQueues(new Configuration());
    
    synchronized (taskScheduler) {
      taskScheduler.refresh();
    }

  }
  
  synchronized String getReasonsForBlacklisting(String host) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, false);
    if (fi == null) {
      return "";
    }
    return fi.getTrackerFaultReport();
  }

  /** Test Methods */
  synchronized Set<ReasonForBlackListing> getReasonForBlackList(String host) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, false);
    if (fi == null) {
      return new HashSet<ReasonForBlackListing>();
    }
    return fi.getReasonforblacklisting();
  }

  /* 
   * This method is synchronized to make sure that the locking order 
   * "faultyTrackers.potentiallyFaultyTrackers lock followed by taskTrackers 
   * lock" is under JobTracker lock to avoid deadlocks.
   */
  synchronized void incrementFaults(String hostName) {
    faultyTrackers.incrementFaults(hostName);
  }
  
  /**
   * 
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }
  
  /**
   * Returns authentication method used to establish the connection
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  JobACLsManager getJobACLsManager() {
    return aclsManager.getJobACLsManager();
  }

  ACLsManager getACLsManager() {
    return aclsManager;
  }

  // Begin MXBean implementation
  @Override
  public String getHostname() {
    return StringUtils.simpleHostname(getJobTrackerMachine());
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() +", r"+ VersionInfo.getRevision();
  }

  @Override
  public String getConfigVersion() {
    return conf.get(CONF_VERSION_KEY, CONF_VERSION_DEFAULT);
  }

  @Override
  public int getThreadCount() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  @Override
  public String getSummaryJson() {
    return getSummary().toJson();
  }

  InfoMap getSummary() {
    final ClusterMetrics metrics = getClusterMetrics();
    InfoMap map = new InfoMap();
    map.put("nodes", metrics.getTaskTrackerCount()
            + getBlacklistedTrackerCount());
    map.put("alive", metrics.getTaskTrackerCount());
    map.put("blacklisted", getBlacklistedTrackerCount());
    map.put("slots", new InfoMap() {{
      put("map_slots", metrics.getMapSlotCapacity());
      put("map_slots_used", metrics.getOccupiedMapSlots());
      put("reduce_slots", metrics.getReduceSlotCapacity());
      put("reduce_slots_used", metrics.getOccupiedReduceSlots());
    }});
    map.put("jobs", metrics.getTotalJobSubmissions());
    return map;
  }

  @Override
  public String getAliveNodesInfoJson() {
    return JSON.toString(getAliveNodesInfo());
  }

  List<InfoMap> getAliveNodesInfo() {
    List<InfoMap> info = new ArrayList<InfoMap>();
    for (final TaskTrackerStatus  tts : activeTaskTrackers()) {
      final int mapSlots = tts.getMaxMapSlots();
      final int redSlots = tts.getMaxReduceSlots();
      info.add(new InfoMap() {{
        put("hostname", tts.getHost());
        put("last_seen", tts.getLastSeen());
        put("health", tts.getHealthStatus().isNodeHealthy() ? "OK" : "");
        put("slots", new InfoMap() {{
          put("map_slots", mapSlots);
          put("map_slots_used", mapSlots - tts.getAvailableMapSlots());
          put("reduce_slots", redSlots);
          put("reduce_slots_used", redSlots - tts.getAvailableReduceSlots());
        }});
        put("failures", tts.getFailures());
        put("dir_failures", tts.getDirFailures());
      }});
    }
    return info;
  }

  @Override
  public String getBlacklistedNodesInfoJson() {
    return JSON.toString(getUnhealthyNodesInfo(blacklistedTaskTrackers()));
  }

  List<InfoMap> getUnhealthyNodesInfo(Collection<TaskTrackerStatus> list) {
    List<InfoMap> info = new ArrayList<InfoMap>();
    for (final TaskTrackerStatus tts : list) {
      info.add(new InfoMap() {{
        put("hostname", tts.getHost());
        put("last_seen", tts.getLastSeen());
        put("reason", tts.getHealthStatus().getHealthReport());
      }});
    }
    return info;
  }
  
  @Override
  public String getQueueInfoJson() {
    return getQueueInfo().toJson();
  }

  InfoMap getQueueInfo() {
    InfoMap map = new InfoMap();
    try {
      for (final JobQueueInfo q : getQueues()) {
        map.put(q.getQueueName(), new InfoMap() {{
          put("info", q.getSchedulingInfo());
        }});
      }
    }
    catch (Exception e) {
      throw new RuntimeException("Getting queue info", e);
    }
    return map;
  }
  // End MXbean implementaiton
}
