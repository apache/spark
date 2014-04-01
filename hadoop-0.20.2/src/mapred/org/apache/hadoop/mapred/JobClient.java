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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * <code>JobClient</code> is the primary interface for the user-job to interact
 * with the {@link JobTracker}.
 * 
 * <code>JobClient</code> provides facilities to submit jobs, track their 
 * progress, access component-tasks' reports/logs, get the Map-Reduce cluster
 * status information etc.
 * 
 * <p>The job submission process involves:
 * <ol>
 *   <li>
 *   Checking the input and output specifications of the job.
 *   </li>
 *   <li>
 *   Computing the {@link InputSplit}s for the job.
 *   </li>
 *   <li>
 *   Setup the requisite accounting information for the {@link DistributedCache} 
 *   of the job, if necessary.
 *   </li>
 *   <li>
 *   Copying the job's jar and configuration to the map-reduce system directory 
 *   on the distributed file-system. 
 *   </li>
 *   <li>
 *   Submitting the job to the <code>JobTracker</code> and optionally monitoring
 *   it's status.
 *   </li>
 * </ol></p>
 *  
 * Normally the user creates the application, describes various facets of the
 * job via {@link JobConf} and then uses the <code>JobClient</code> to submit 
 * the job and monitor its progress.
 * 
 * <p>Here is an example on how to use <code>JobClient</code>:</p>
 * <p><blockquote><pre>
 *     // Create a new JobConf
 *     JobConf job = new JobConf(new Configuration(), MyJob.class);
 *     
 *     // Specify various job-specific parameters     
 *     job.setJobName("myjob");
 *     
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *
 *     // Submit the job, then poll for progress until the job is complete
 *     JobClient.runJob(job);
 * </pre></blockquote></p>
 * 
 * <h4 id="JobControl">Job Control</h4>
 * 
 * <p>At times clients would chain map-reduce jobs to accomplish complex tasks 
 * which cannot be done via a single map-reduce job. This is fairly easy since 
 * the output of the job, typically, goes to distributed file-system and that 
 * can be used as the input for the next job.</p>
 * 
 * <p>However, this also means that the onus on ensuring jobs are complete 
 * (success/failure) lies squarely on the clients. In such situations the 
 * various job-control options are:
 * <ol>
 *   <li>
 *   {@link #runJob(JobConf)} : submits the job and returns only after 
 *   the job has completed.
 *   </li>
 *   <li>
 *   {@link #submitJob(JobConf)} : only submits the job, then poll the 
 *   returned handle to the {@link RunningJob} to query status and make 
 *   scheduling decisions.
 *   </li>
 *   <li>
 *   {@link JobConf#setJobEndNotificationURI(String)} : setup a notification
 *   on job-completion, thus avoiding polling.
 *   </li>
 * </ol></p>
 * 
 * @see JobConf
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
public class JobClient extends Configured implements MRConstants, Tool  {
  private static final Log LOG = LogFactory.getLog(JobClient.class);
  public static enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }
  private TaskStatusFilter taskOutputFilter = TaskStatusFilter.FAILED; 
  private static final long MAX_JOBPROFILE_AGE = 1000 * 2;

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  /** The interval at which monitorAndPrintJob() prints status */
  private int progMonitorPollIntervalMillis;

  /** Default progMonitorPollIntervalMillis is 1000 ms. */
  private static final int DEFAULT_MONITOR_POLL_INTERVAL = 1000;

  /** Key in mapred-*.xml that sets progMonitorPollIntervalMillis */
  static final String PROGRESS_MONITOR_POLL_INTERVAL_KEY =
      "jobclient.progress.monitor.poll.interval";


  /**
   * A NetworkedJob is an implementation of RunningJob.  It holds
   * a JobProfile object to provide some info, and interacts with the
   * remote service to provide certain functionality.
   */
  class NetworkedJob implements RunningJob {
    JobProfile profile;
    JobStatus status;
    long statustime;

    /** The interval at which NetworkedJob.waitForCompletion() should check. */
    private int completionPollIntervalMillis;

    /** Default completionPollIntervalMillis is 5000 ms. */
    private static final int DEFAULT_COMPLETION_POLL_INTERVAL = 5000;

    /** Key in mapred-*.xml that sets completionPollInvervalMillis */
    static final String COMPLETION_POLL_INTERVAL_KEY = "jobclient.completion.poll.interval";

    /**
     * We store a JobProfile and a timestamp for when we last
     * acquired the job profile.  If the job is null, then we cannot
     * perform any of the tasks.  The job might be null if the JobTracker
     * has completely forgotten about the job.  (eg, 24 hours after the
     * job completes.)
     */
    public NetworkedJob(JobStatus job) throws IOException {
      this.status = job;
      this.profile = jobSubmitClient.getJobProfile(job.getJobID());
      this.statustime = System.currentTimeMillis();

      // Set the completion poll interval from the configuration.
      // Default is 5 seconds.
      Configuration conf = JobClient.this.getConf();
      this.completionPollIntervalMillis = conf.getInt(COMPLETION_POLL_INTERVAL_KEY,
          DEFAULT_COMPLETION_POLL_INTERVAL);
      if (this.completionPollIntervalMillis < 1) {
        LOG.warn(COMPLETION_POLL_INTERVAL_KEY + " has been set to an invalid value; "
            + "replacing with " + DEFAULT_COMPLETION_POLL_INTERVAL);
        this.completionPollIntervalMillis = DEFAULT_COMPLETION_POLL_INTERVAL;
      }
    }

    /**
     * Some methods rely on having a recent job profile object.  Refresh
     * it, if necessary
     */
    synchronized void ensureFreshStatus() throws IOException {
      if (System.currentTimeMillis() - statustime > MAX_JOBPROFILE_AGE) {
        updateStatus();
      }
    }
    
    /** Some methods need to update status immediately. So, refresh
     * immediately
     * @throws IOException
     */
    synchronized void updateStatus() throws IOException {
      this.status = jobSubmitClient.getJobStatus(profile.getJobID());
      this.statustime = System.currentTimeMillis();
    }

    /**
     * An identifier for the job
     */
    public JobID getID() {
      return profile.getJobID();
    }
    
    /** @deprecated This method is deprecated and will be removed. Applications should 
     * rather use {@link #getID()}.*/
    @Deprecated
    public String getJobID() {
      return profile.getJobID().toString();
    }
    
    /**
     * The user-specified job name
     */
    public String getJobName() {
      return profile.getJobName();
    }

    /**
     * The name of the job file
     */
    public String getJobFile() {
      return profile.getJobFile();
    }

    /**
     * A URL where the job's status can be seen
     */
    public String getTrackingURL() {
      return profile.getURL().toString();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of map work
     * completed.
     */
    public float mapProgress() throws IOException {
      ensureFreshStatus();
      return status.mapProgress();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of reduce work
     * completed.
     */
    public float reduceProgress() throws IOException {
      ensureFreshStatus();
      return status.reduceProgress();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of cleanup work
     * completed.
     */
    public float cleanupProgress() throws IOException {
      ensureFreshStatus();
      return status.cleanupProgress();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of setup work
     * completed.
     */
    public float setupProgress() throws IOException {
      ensureFreshStatus();
      return status.setupProgress();
    }

    /**
     * Returns immediately whether the whole job is done yet or not.
     */
    public synchronized boolean isComplete() throws IOException {
      updateStatus();
      return (status.getRunState() == JobStatus.SUCCEEDED ||
              status.getRunState() == JobStatus.FAILED ||
              status.getRunState() == JobStatus.KILLED);
    }

    /**
     * True iff job completed successfully.
     */
    public synchronized boolean isSuccessful() throws IOException {
      updateStatus();
      return status.getRunState() == JobStatus.SUCCEEDED;
    }

    /**
     * Blocks until the job is finished
     */
    public void waitForCompletion() throws IOException {
      while (!isComplete()) {
        try {
          Thread.sleep(this.completionPollIntervalMillis);
        } catch (InterruptedException ie) {
        }
      }
    }

    /**
     * Tells the service to get the state of the current job.
     */
    public synchronized int getJobState() throws IOException {
      updateStatus();
      return status.getRunState();
    }
    
    /**
     * Tells the service to terminate the current job.
     */
    public synchronized void killJob() throws IOException {
      jobSubmitClient.killJob(getID());
    }
   
    
    /** Set the priority of the job.
    * @param priority new priority of the job. 
    */
    public synchronized void setJobPriority(String priority) 
                                                throws IOException {
      jobSubmitClient.setJobPriority(getID(), priority);
    }
    
    /**
     * Kill indicated task attempt.
     * @param taskId the id of the task to kill.
     * @param shouldFail if true the task is failed and added to failed tasks list, otherwise
     * it is just killed, w/o affecting job failure status.
     */
    public synchronized void killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
      jobSubmitClient.killTask(taskId, shouldFail);
    }

    /** @deprecated Applications should rather use {@link #killTask(TaskAttemptID, boolean)}*/
    @Deprecated
    public synchronized void killTask(String taskId, boolean shouldFail) throws IOException {
      killTask(TaskAttemptID.forName(taskId), shouldFail);
    }
    
    /**
     * Fetch task completion events from jobtracker for this job. 
     */
    public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
                                                                      int startFrom) throws IOException{
      return jobSubmitClient.getTaskCompletionEvents(
                                                     getID(), startFrom, 10); 
    }

    /**
     * Dump stats to screen
     */
    @Override
    public String toString() {
      try {
        updateStatus();
      } catch (IOException e) {
      }
      return "Job: " + profile.getJobID() + "\n" + 
        "file: " + profile.getJobFile() + "\n" + 
        "tracking URL: " + profile.getURL() + "\n" + 
        "map() completion: " + status.mapProgress() + "\n" + 
        "reduce() completion: " + status.reduceProgress();
    }
        
    /**
     * Returns the counters for this job
     */
    public Counters getCounters() throws IOException {
      return jobSubmitClient.getJobCounters(getID());
    }
    
    @Override
    public String[] getTaskDiagnostics(TaskAttemptID id) throws IOException {
      return jobSubmitClient.getTaskDiagnostics(id);
    }

    @Override
    public String getFailureInfo() throws IOException {
      //assuming that this is just being called after 
      //we realized the job failed. SO we try avoiding 
      //a rpc by not calling updateStatus
      ensureFreshStatus();
      return status.getFailureInfo();
    }
  }

  private JobSubmissionProtocol jobSubmitClient;
  private Path sysDir = null;
  private Path stagingAreaDir = null;
  
  private FileSystem fs = null;
  private UserGroupInformation ugi;
  private static final String TASKLOG_PULL_TIMEOUT_KEY = 
    "mapreduce.client.tasklog.timeout";
  private static final int DEFAULT_TASKLOG_TIMEOUT = 60000;
  static int tasklogtimeout;

  /**
   * Create a job client.
   */
  public JobClient() {
    this.progMonitorPollIntervalMillis = DEFAULT_MONITOR_POLL_INTERVAL;
  }
    
  /**
   * Build a job client with the given {@link JobConf}, and connect to the 
   * default {@link JobTracker}.
   * 
   * @param conf the job configuration.
   * @throws IOException
   */
  public JobClient(JobConf conf) throws IOException {
    setConf(conf);
    init(conf);
  }

  /**
   * Connect to the default {@link JobTracker}.
   * @param conf the job configuration.
   * @throws IOException
   */
  public void init(JobConf conf) throws IOException {
    String tracker = conf.get("mapred.job.tracker", "local");
    tasklogtimeout = conf.getInt(
      TASKLOG_PULL_TIMEOUT_KEY, DEFAULT_TASKLOG_TIMEOUT);
    this.ugi = UserGroupInformation.getCurrentUser();
    if ("local".equals(tracker)) {
      conf.setNumMapTasks(1);
      this.jobSubmitClient = new LocalJobRunner(conf);
    } else {
      this.jobSubmitClient = createRPCProxy(JobTracker.getAddress(conf), conf);
    }        

    // Read progress monitor poll interval from config. Default is 1 second.
    this.progMonitorPollIntervalMillis = conf.getInt(PROGRESS_MONITOR_POLL_INTERVAL_KEY,
        DEFAULT_MONITOR_POLL_INTERVAL);
    if (this.progMonitorPollIntervalMillis < 1) {
      LOG.warn(PROGRESS_MONITOR_POLL_INTERVAL_KEY + " has been set to an invalid value; "
          + " replacing with " + DEFAULT_MONITOR_POLL_INTERVAL);
      this.progMonitorPollIntervalMillis = DEFAULT_MONITOR_POLL_INTERVAL;
    }
  }

  private JobSubmissionProtocol createRPCProxy(InetSocketAddress addr,
      Configuration conf) throws IOException {
    return (JobSubmissionProtocol) RPC.getProxy(JobSubmissionProtocol.class,
        JobSubmissionProtocol.versionID, addr, 
        UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
  }

  /**
   * Build a job client, connect to the indicated job tracker.
   * 
   * @param jobTrackAddr the job tracker to connect to.
   * @param conf configuration.
   */
  public JobClient(InetSocketAddress jobTrackAddr, 
                   Configuration conf) throws IOException {
    this.ugi = UserGroupInformation.getCurrentUser();
    jobSubmitClient = createRPCProxy(jobTrackAddr, conf);
  }

  /**
   * Close the <code>JobClient</code>.
   */
  public synchronized void close() throws IOException {
    if (!(jobSubmitClient instanceof LocalJobRunner)) {
      RPC.stopProxy(jobSubmitClient);
    }
  }

  /**
   * Get a filesystem handle.  We need this to prepare jobs
   * for submission to the MapReduce system.
   * 
   * @return the filesystem handle.
   * @throws IOException 
   */
  public synchronized FileSystem getFs() throws IOException {
    if (this.fs == null) {
      try {
        this.fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException {
            Path sysDir = getSystemDir();
            return sysDir.getFileSystem(getConf());
          }
        });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return this.fs;
  }
  
  /* see if two file systems are the same or not
   *
   */
  private boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme() == null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();    
    String dstHost = dstUri.getHost();
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch(UnknownHostException ue) {
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    }
    else if (srcHost == null && dstHost != null) {
      return false;
    }
    else if (srcHost != null && dstHost == null) {
      return false;
    }
    //check for ports
    if (srcUri.getPort() != dstUri.getPort()) {
      return false;
    }
    return true;
  }

  // copies a file to the jobtracker filesystem and returns the path where it
  // was copied to
  private Path copyRemoteFiles(FileSystem jtFs, Path parentDir, 
      final Path originalPath, final JobConf job, short replication) 
  throws IOException, InterruptedException {
    //check if we do not need to copy the files
    // is jt using the same file system.
    // just checking for uri strings... doing no dns lookups 
    // to see if the filesystems are the same. This is not optimal.
    // but avoids name resolution.
    
    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(job);
    
    if (compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    // this might have name collisions. copy will throw an exception
    //parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, job);
    jtFs.setReplication(newPath, replication);
    return newPath;
  }
 
  private URI getPathURI(Path destPath, String fragment)
      throws URISyntaxException {
    URI pathURI = destPath.toUri();
    if (pathURI.getFragment() == null) {
      if (fragment == null) {
        pathURI = new URI(pathURI.toString() + "#" + destPath.getName());
      } else {
        pathURI = new URI(pathURI.toString() + "#" + fragment);
      }
    }
    return pathURI;
  }

  /**
   * configure the jobconf of the user with the command line options of 
   * -libjars, -files, -archives
   * @param job the JobConf
   * @param submitJobDir
   * @throws IOException
   */
  private void copyAndConfigureFiles(JobConf job, Path jobSubmitDir) 
  throws IOException, InterruptedException {
    short replication = (short)job.getInt("mapred.submit.replication", 10);
    copyAndConfigureFiles(job, jobSubmitDir, replication);

    // Set the working directory
    if (job.getWorkingDirectory() == null) {
      job.setWorkingDirectory(fs.getWorkingDirectory());          
    }
  }
  
  private void copyAndConfigureFiles(JobConf job, Path submitJobDir, 
      short replication) throws IOException, InterruptedException {
    
    if (!(job.getBoolean("mapred.used.genericoptionsparser", false))) {
      LOG.warn("Use GenericOptionsParser for parsing the arguments. " +
               "Applications should implement Tool for the same.");
    }

    // Retrieve command line arguments placed into the JobConf
    // by GenericOptionsParser.
    String files = job.get("tmpfiles");
    String libjars = job.get("tmpjars");
    String archives = job.get("tmparchives");

    //
    // Figure out what fs the JobTracker is using.  Copy the
    // job to it, under a temporary name.  This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading 
    // semantics.  (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    FileSystem fs = submitJobDir.getFileSystem(job);
    LOG.debug("default FileSystem: " + fs.getUri());
    if (fs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          +" already exists!! This is unexpected.Please check what's there in" +
          " that directory");
    }
    submitJobDir = fs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission mapredSysPerms = new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(fs, submitJobDir, mapredSysPerms);
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
    Path archivesDir = JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);
    // add all the command line files/ jars and archive
    // first copy them to jobtrackers filesystem 
    
    if (files != null) {
      FileSystem.mkdirs(fs, filesDir, mapredSysPerms);
      String[] fileArr = files.split(",");
      for (String tmpFile: fileArr) {
        URI tmpURI;
        try {
          tmpURI = new URI(tmpFile);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(fs,filesDir, tmp, job, replication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheFile(pathURI, job);
        } catch(URISyntaxException ue) {
          //should not throw a uri exception 
          throw new IOException("Failed to create uri for " + tmpFile, ue);
        }
        DistributedCache.createSymlink(job);
      }
    }
    
    if (libjars != null) {
      FileSystem.mkdirs(fs, libjarsDir, mapredSysPerms);
      String[] libjarsArr = libjars.split(",");
      for (String tmpjars: libjarsArr) {
        Path tmp = new Path(tmpjars);
        Path newPath = copyRemoteFiles(fs, libjarsDir, tmp, job, replication);
        DistributedCache.addFileToClassPath(
          new Path(newPath.toUri().getPath()), job, fs);
      }
    }
    
    
    if (archives != null) {
     FileSystem.mkdirs(fs, archivesDir, mapredSysPerms); 
     String[] archivesArr = archives.split(",");
     for (String tmpArchives: archivesArr) {
       URI tmpURI;
       try {
         tmpURI = new URI(tmpArchives);
       } catch (URISyntaxException e) {
         throw new IllegalArgumentException(e);
       }
       Path tmp = new Path(tmpURI);
       Path newPath = copyRemoteFiles(fs, archivesDir, tmp, job, replication);
       try {
         URI pathURI = getPathURI(newPath, tmpURI.getFragment());
         DistributedCache.addCacheArchive(pathURI, job);
       } catch(URISyntaxException ue) {
         //should not throw an uri excpetion
         throw new IOException("Failed to create uri for " + tmpArchives, ue);
       }
       DistributedCache.createSymlink(job);
     }
    }
    
    // First we check whether the cached archives and files are legal.
    TrackerDistributedCacheManager.validate(job);
    //  set the timestamps of the archives and files
    TrackerDistributedCacheManager.determineTimestamps(job);
    //  set the public/private visibility of the archives and files
    TrackerDistributedCacheManager.determineCacheVisibilities(job);
    // get DelegationTokens for cache files
    TrackerDistributedCacheManager.getDelegationTokens(job, 
                                                       job.getCredentials());

    String originalJarPath = job.getJar();

    if (originalJarPath != null) {           // copy jar to JobTracker's fs
      // use jar name if job is not named. 
      if ("".equals(job.getJobName())){
        job.setJobName(new Path(originalJarPath).getName());
      }
      Path submitJarFile = JobSubmissionFiles.getJobJar(submitJobDir);
      job.setJar(submitJarFile.toString());
      fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);
      fs.setReplication(submitJarFile, replication);
      fs.setPermission(submitJarFile, 
          new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "+
               "See JobConf(Class) or JobConf#setJar(String).");
    }
  }
  
  /**
   * Submit a job to the MR system.
   * 
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   * 
   * @param jobFile the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws InvalidJobConfException
   * @throws IOException
   */
  public RunningJob submitJob(String jobFile) throws FileNotFoundException, 
                                                     InvalidJobConfException, 
                                                     IOException {
    // Load in the submitted job details
    JobConf job = new JobConf(jobFile);
    return submitJob(job);
  }
      
  /**
   * Submit a job to the MR system.
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   * 
   * @param job the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public RunningJob submitJob(JobConf job) throws FileNotFoundException,
                                                  IOException {
    try {
      return submitJobInternal(job);
    } catch (InterruptedException ie) {
      throw new IOException("interrupted", ie);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("class not found", cnfe);
    }
  }

  /**
   * Internal method for submitting jobs to the system.
   * @param job the configuration to submit
   * @return a proxy object for the running job
   * @throws FileNotFoundException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  public 
  RunningJob submitJobInternal(final JobConf job
                               ) throws FileNotFoundException, 
                                        ClassNotFoundException,
                                        InterruptedException,
                                        IOException {
    /*
     * configure the command line options correctly on the submitting dfs
     */
    return ugi.doAs(new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws FileNotFoundException, 
      ClassNotFoundException,
      InterruptedException,
      IOException{
        JobConf jobCopy = job;
        Path jobStagingArea = JobSubmissionFiles.getStagingDir(JobClient.this,
            jobCopy);
        JobID jobId = jobSubmitClient.getNewJobId();
        Path submitJobDir = new Path(jobStagingArea, jobId.toString());
        jobCopy.set("mapreduce.job.dir", submitJobDir.toString());
        JobStatus status = null;
        try {
          populateTokenCache(jobCopy, jobCopy.getCredentials());

          copyAndConfigureFiles(jobCopy, submitJobDir);

          // get delegation token for the dir
          TokenCache.obtainTokensForNamenodes(jobCopy.getCredentials(),
                                              new Path [] {submitJobDir},
                                              jobCopy);

          Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
          int reduces = jobCopy.getNumReduceTasks();
          InetAddress ip = InetAddress.getLocalHost();
          if (ip != null) {
            job.setJobSubmitHostAddress(ip.getHostAddress());
            job.setJobSubmitHostName(ip.getHostName());
          }
          JobContext context = new JobContext(jobCopy, jobId);

          jobCopy = (JobConf)context.getConfiguration();

          // Check the output specification
          if (reduces == 0 ? jobCopy.getUseNewMapper() : 
            jobCopy.getUseNewReducer()) {
            org.apache.hadoop.mapreduce.OutputFormat<?,?> output =
              ReflectionUtils.newInstance(context.getOutputFormatClass(),
                  jobCopy);
            output.checkOutputSpecs(context);
          } else {
            jobCopy.getOutputFormat().checkOutputSpecs(fs, jobCopy);
          }

          // Create the splits for the job
          FileSystem fs = submitJobDir.getFileSystem(jobCopy);
          LOG.debug("Creating splits at " + fs.makeQualified(submitJobDir));
          int maps = writeSplits(context, submitJobDir);
          jobCopy.setNumMapTasks(maps);

          // write "queue admins of the queue to which job is being submitted"
          // to job file.
          String queue = jobCopy.getQueueName();
          AccessControlList acl = jobSubmitClient.getQueueAdmins(queue);
          jobCopy.set(QueueManager.toFullPropertyName(queue,
              QueueACL.ADMINISTER_JOBS.getAclName()), acl.getACLString());

          // Write job file to JobTracker's fs        
          FSDataOutputStream out = 
            FileSystem.create(fs, submitJobFile,
                new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));

          // removing jobtoken referrals before copying the jobconf to HDFS
          // as the tasks don't need this setting, actually they may break
          // because of it if present as the referral will point to a
          // different job.
          TokenCache.cleanUpTokenReferral(jobCopy);

          try {
            jobCopy.writeXml(out);
          } finally {
            out.close();
          }
          //
          // Now, actually submit the job (using the submit name)
          //
          printTokens(jobId, jobCopy.getCredentials());
          status = jobSubmitClient.submitJob(
              jobId, submitJobDir.toString(), jobCopy.getCredentials());
          if (status != null) {
            return new NetworkedJob(status);
          } else {
            throw new IOException("Could not launch job");
          }
        } finally {
          if (status == null) {
            LOG.info("Cleaning up the staging area " + submitJobDir);
            if (fs != null && submitJobDir != null)
              fs.delete(submitJobDir, true);
          }
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void printTokens(JobID jobId,
                           Credentials credentials) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Printing tokens for job: " + jobId);
      for(Token<?> token: credentials.getAllTokens()) {
        if (token.getKind().toString().equals("HDFS_DELEGATION_TOKEN")) {
          LOG.debug("Submitting with " +
              DFSClient.stringifyToken((Token<org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier>) token));
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends InputSplit>
  int writeNewSplits(JobContext job, Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    InputFormat<?, ?> input =
      ReflectionUtils.newInstance(job.getInputFormatClass(), conf);

    List<InputSplit> splits = input.getSplits(job);
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);

    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(array, new SplitComparator());
    JobSplitWriter.createSplitFiles(jobSubmitDir, conf,
        jobSubmitDir.getFileSystem(conf), array);
    return array.length;
  }
  
  private int writeSplits(org.apache.hadoop.mapreduce.JobContext job,
      Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    JobConf jConf = (JobConf)job.getConfiguration();
    int maps;
    if (jConf.getUseNewMapper()) {
      maps = writeNewSplits(job, jobSubmitDir);
    } else {
      maps = writeOldSplits(jConf, jobSubmitDir);
    }
    return maps;
  }
  
  //method to write splits for old api mapper.
  private int writeOldSplits(JobConf job, Path jobSubmitDir) 
  throws IOException {
    org.apache.hadoop.mapred.InputSplit[] splits =
    job.getInputFormat().getSplits(job, job.getNumMapTasks());
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new Comparator<org.apache.hadoop.mapred.InputSplit>() {
      public int compare(org.apache.hadoop.mapred.InputSplit a,
                         org.apache.hadoop.mapred.InputSplit b) {
        try {
          long left = a.getLength();
          long right = b.getLength();
          if (left == right) {
            return 0;
          } else if (left < right) {
            return 1;
          } else {
            return -1;
          }
        } catch (IOException ie) {
          throw new RuntimeException("Problem getting input split size", ie);
        }
      }
    });
    JobSplitWriter.createSplitFiles(jobSubmitDir, job,
        jobSubmitDir.getFileSystem(job), splits);
    return splits.length;
  }
  
  private static class SplitComparator implements Comparator<InputSplit> {
    @Override
    public int compare(InputSplit o1, InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in compare", ie);
      }
    }
  }
  
  /** 
   * Checks if the job directory is clean and has all the required components 
   * for (re) starting the job
   */
  public static boolean isJobDirValid(Path jobDirPath, FileSystem fs) 
  throws IOException {
    FileStatus[] contents = fs.listStatus(jobDirPath);
    int matchCount = 0;
    if (contents != null && contents.length >=2) {
      for (FileStatus status : contents) {
        if ("job.xml".equals(status.getPath().getName())) {
          ++matchCount;
        }
        if ("job.split".equals(status.getPath().getName())) {
          ++matchCount;
        }
      }
      if (matchCount == 2) {
        return true;
      }
    }
    return false;
  }
    
  /**
   * Get an {@link RunningJob} object to track an ongoing job.  Returns
   * null if the id does not correspond to any known job.
   * 
   * @param jobid the jobid of the job.
   * @return the {@link RunningJob} handle to track the job, null if the 
   *         <code>jobid</code> doesn't correspond to any known job.
   * @throws IOException
   */
  public RunningJob getJob(JobID jobid) throws IOException {
    JobStatus status = jobSubmitClient.getJobStatus(jobid);
    if (status != null) {
      return new NetworkedJob(status);
    } else {
      return null;
    }
  }

  /**@deprecated Applications should rather use {@link #getJob(JobID)}. 
   */
  @Deprecated
  public RunningJob getJob(String jobid) throws IOException {
    return getJob(JobID.forName(jobid));
  }
  
  /**
   * Get the information of the current state of the map tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the map tips.
   * @throws IOException
   */
  public TaskReport[] getMapTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getMapTaskReports(jobId);
  }
  
  /**@deprecated Applications should rather use {@link #getMapTaskReports(JobID)}*/
  @Deprecated
  public TaskReport[] getMapTaskReports(String jobId) throws IOException {
    return getMapTaskReports(JobID.forName(jobId));
  }
  
  /**
   * Get the information of the current state of the reduce tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the reduce tips.
   * @throws IOException
   */    
  public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getReduceTaskReports(jobId);
  }

  /**
   * Get the information of the current state of the cleanup tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the cleanup tips.
   * @throws IOException
   */    
  public TaskReport[] getCleanupTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getCleanupTaskReports(jobId);
  }

  /**
   * Get the information of the current state of the setup tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the setup tips.
   * @throws IOException
   */    
  public TaskReport[] getSetupTaskReports(JobID jobId) throws IOException {
    return jobSubmitClient.getSetupTaskReports(jobId);
  }

  /**@deprecated Applications should rather use {@link #getReduceTaskReports(JobID)}*/
  @Deprecated
  public TaskReport[] getReduceTaskReports(String jobId) throws IOException {
    return getReduceTaskReports(JobID.forName(jobId));
  }
  
  /**
   * Display the information about a job's tasks, of a particular type and
   * in a particular state
   * 
   * @param jobId the ID of the job
   * @param type the type of the task (map/reduce/setup/cleanup)
   * @param state the state of the task 
   * (pending/running/completed/failed/killed)
   */
  public void displayTasks(JobID jobId, String type, String state) 
  throws IOException {
    TaskReport[] reports = new TaskReport[0];
    if (type.equals("map")) {
      reports = getMapTaskReports(jobId);
    } else if (type.equals("reduce")) {
      reports = getReduceTaskReports(jobId);
    } else if (type.equals("setup")) {
      reports = getSetupTaskReports(jobId);
    } else if (type.equals("cleanup")) {
      reports = getCleanupTaskReports(jobId);
    }
    for (TaskReport report : reports) {
      TIPStatus status = report.getCurrentStatus();
      if ((state.equals("pending") && status ==TIPStatus.PENDING) ||
          (state.equals("running") && status ==TIPStatus.RUNNING) ||
          (state.equals("completed") && status == TIPStatus.COMPLETE) ||
          (state.equals("failed") && status == TIPStatus.FAILED) ||
          (state.equals("killed") && status == TIPStatus.KILLED)) {
        printTaskAttempts(report);
      }
    }
  }
  private void printTaskAttempts(TaskReport report) {
    if (report.getCurrentStatus() == TIPStatus.COMPLETE) {
      System.out.println(report.getSuccessfulTaskAttempt());
    } else if (report.getCurrentStatus() == TIPStatus.RUNNING) {
      for (TaskAttemptID t : 
        report.getRunningTaskAttempts()) {
        System.out.println(t);
      }
    }
  }
  /**
   * Get status information about the Map-Reduce cluster.
   *  
   * @return the status information about the Map-Reduce cluster as an object
   *         of {@link ClusterStatus}.
   * @throws IOException
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return getClusterStatus(false);
  }

  /**
   * Get status information about the Map-Reduce cluster.
   *  
   * @param  detailed if true then get a detailed status including the
   *         tracker names and memory usage of the JobTracker
   * @return the status information about the Map-Reduce cluster as an object
   *         of {@link ClusterStatus}.
   * @throws IOException
   */
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    return jobSubmitClient.getClusterStatus(detailed);
  }
  
  /**
   * Grab the jobtracker's view of the staging directory path where 
   * job-specific files will  be placed.
   * 
   * @return the staging directory where job-specific files are to be placed.
   */
  public Path getStagingAreaDir() throws IOException {
    if (stagingAreaDir == null) {
      stagingAreaDir = new Path(jobSubmitClient.getStagingAreaDir());
    }
    return stagingAreaDir;
  }    

  /** 
   * Get the jobs that are not completed and not failed.
   * 
   * @return array of {@link JobStatus} for the running/to-be-run jobs.
   * @throws IOException
   */
  public JobStatus[] jobsToComplete() throws IOException {
    return jobSubmitClient.jobsToComplete();
  }

  private static void downloadProfile(TaskCompletionEvent e
                                      ) throws IOException  {
    URLConnection connection = 
      new URL(getTaskLogURL(e.getTaskAttemptId(), e.getTaskTrackerHttp()) + 
              "&filter=profile").openConnection();
    InputStream in = connection.getInputStream();
    OutputStream out = new FileOutputStream(e.getTaskAttemptId() + ".profile");
    IOUtils.copyBytes(in, out, 64 * 1024, true);
  }

  /** 
   * Get the jobs that are submitted.
   * 
   * @return array of {@link JobStatus} for the submitted jobs.
   * @throws IOException
   */
  public JobStatus[] getAllJobs() throws IOException {
    return jobSubmitClient.getAllJobs();
  }
  
  /** 
   * Utility that submits a job, then polls for progress until the job is
   * complete.
   * 
   * @param job the job configuration.
   * @throws IOException if the job fails
   */
  public static RunningJob runJob(JobConf job) throws IOException {
    JobClient jc = new JobClient(job);
    RunningJob rj = jc.submitJob(job);
    try {
      if (!jc.monitorAndPrintJob(job, rj)) {
        LOG.info("Job Failed: " + rj.getFailureInfo());
        throw new IOException("Job failed!");
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    return rj;
  }
  
  /**
   * @return true if the profile parameters indicate that this is using
   * hprof, which generates profile files in a particular location
   * that we can retrieve to the client.
   */
  private boolean shouldDownloadProfile(JobConf conf) {
    // Check the argument string that was used to initialize profiling.
    // If this indicates hprof and file-based output, then we're ok to
    // download.
    String profileParams = conf.getProfileParams();

    if (null == profileParams) {
      return false;
    }

    // Split this on whitespace.
    String [] parts = profileParams.split("[ \\t]+");

    // If any of these indicate hprof, and the use of output files, return true.
    boolean hprofFound = false;
    boolean fileFound = false;
    for (String p : parts) {
      if (p.startsWith("-agentlib:hprof") || p.startsWith("-Xrunhprof")) {
        hprofFound = true;

        // This contains a number of comma-delimited components, one of which
        // may specify the file to write to. Make sure this is present and
        // not empty.
        String [] subparts = p.split(",");
        for (String sub : subparts) {
          if (sub.startsWith("file=") && sub.length() != "file=".length()) {
            fileFound = true;
          }
        }
      }
    }

    return hprofFound && fileFound;
  }

  /**
   * Monitor a job and print status in real-time as progress is made and tasks 
   * fail.
   * @param conf the job's configuration
   * @param job the job to track
   * @return true if the job succeeded
   * @throws IOException if communication to the JobTracker fails
   */
  public boolean monitorAndPrintJob(JobConf conf, 
                                    RunningJob job
  ) throws IOException, InterruptedException {
    String lastReport = null;
    TaskStatusFilter filter;
    filter = getTaskOutputFilter(conf);
    JobID jobId = job.getID();
    LOG.info("Running job: " + jobId);
    int eventCounter = 0;
    boolean profiling = conf.getProfileEnabled();
    Configuration.IntegerRanges mapRanges = conf.getProfileTaskRange(true);
    Configuration.IntegerRanges reduceRanges = conf.getProfileTaskRange(false);

    while (!job.isComplete()) {
      Thread.sleep(this.progMonitorPollIntervalMillis);
      String report = 
        (" map " + StringUtils.formatPercent(job.mapProgress(), 0)+
            " reduce " + 
            StringUtils.formatPercent(job.reduceProgress(), 0));
      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }

      TaskCompletionEvent[] events = 
        job.getTaskCompletionEvents(eventCounter); 
      eventCounter += events.length;
      for(TaskCompletionEvent event : events){
        TaskCompletionEvent.Status status = event.getTaskStatus();
        if (profiling && shouldDownloadProfile(conf) &&
            (status == TaskCompletionEvent.Status.SUCCEEDED ||
                status == TaskCompletionEvent.Status.FAILED) &&
                (event.isMap ? mapRanges : reduceRanges).
                isIncluded(event.idWithinJob())) {
          downloadProfile(event);
        }
        switch(filter){
        case NONE:
          break;
        case SUCCEEDED:
          if (event.getTaskStatus() == 
            TaskCompletionEvent.Status.SUCCEEDED){
            LOG.info(event.toString());
            displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
          }
          break; 
        case FAILED:
          if (event.getTaskStatus() == 
            TaskCompletionEvent.Status.FAILED){
            LOG.info(event.toString());
            // Displaying the task diagnostic information
            TaskAttemptID taskId = event.getTaskAttemptId();
            String[] taskDiagnostics = 
              jobSubmitClient.getTaskDiagnostics(taskId); 
            if (taskDiagnostics != null) {
              for(String diagnostics : taskDiagnostics){
                System.err.println(diagnostics);
              }
            }
            // Displaying the task logs
            displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
          }
          break; 
        case KILLED:
          if (event.getTaskStatus() == TaskCompletionEvent.Status.KILLED){
            LOG.info(event.toString());
          }
          break; 
        case ALL:
          LOG.info(event.toString());
          displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
          break;
        }
      }
    }
    LOG.info("Job complete: " + jobId);
    Counters counters = null;
    try{
       counters = job.getCounters();
    } catch(IOException ie) {
      counters = null;
      LOG.info(ie.getMessage());
    }
    if (counters != null) {
      counters.log(LOG);
    }
    return job.isSuccessful();
  }

  static String getTaskLogURL(TaskAttemptID taskId, String baseUrl) {
    return (baseUrl + "/tasklog?plaintext=true&attemptid=" + taskId); 
  }
  
  private static void displayTaskLogs(TaskAttemptID taskId, String baseUrl)
    throws IOException {
    // The tasktracker for a 'failed/killed' job might not be around...
    if (baseUrl != null) {
      // Construct the url for the tasklogs
      String taskLogUrl = getTaskLogURL(taskId, baseUrl);
      
      // Copy tasks's stdout of the JobClient
      getTaskLogs(taskId, new URL(taskLogUrl+"&filter=stdout"), System.out);
        
      // Copy task's stderr to stderr of the JobClient 
      getTaskLogs(taskId, new URL(taskLogUrl+"&filter=stderr"), System.err);
    }
  }
    
  private static void getTaskLogs(TaskAttemptID taskId, URL taskLogUrl, 
                                  OutputStream out) {
    try {
      URLConnection connection = taskLogUrl.openConnection();
      connection.setReadTimeout(tasklogtimeout);
      connection.setConnectTimeout(tasklogtimeout);
      BufferedReader input = 
        new BufferedReader(new InputStreamReader(connection.getInputStream()));
      BufferedWriter output = 
        new BufferedWriter(new OutputStreamWriter(out));
      try {
        String logData = null;
        while ((logData = input.readLine()) != null) {
          if (logData.length() > 0) {
            output.write(taskId + ": " + logData + "\n");
            output.flush();
          }
        }
      } finally {
        input.close();
      }
    }catch(IOException ioe){
      LOG.warn("Error reading task output" + ioe.getMessage()); 
    }
  }    

  static Configuration getConfiguration(String jobTrackerSpec)
  {
    Configuration conf = new Configuration();
    if (jobTrackerSpec != null) {        
      if (jobTrackerSpec.indexOf(":") >= 0) {
        conf.set("mapred.job.tracker", jobTrackerSpec);
      } else {
        String classpathFile = "hadoop-" + jobTrackerSpec + ".xml";
        URL validate = conf.getResource(classpathFile);
        if (validate == null) {
          throw new RuntimeException(classpathFile + " not found on CLASSPATH");
        }
        conf.addResource(classpathFile);
      }
    }
    return conf;
  }

  /**
   * Sets the output filter for tasks. only those tasks are printed whose
   * output matches the filter. 
   * @param newValue task filter.
   */
  @Deprecated
  public void setTaskOutputFilter(TaskStatusFilter newValue){
    this.taskOutputFilter = newValue;
  }
    
  /**
   * Get the task output filter out of the JobConf.
   * 
   * @param job the JobConf to examine.
   * @return the filter level.
   */
  public static TaskStatusFilter getTaskOutputFilter(JobConf job) {
    return TaskStatusFilter.valueOf(job.get("jobclient.output.filter", 
                                            "FAILED"));
  }
    
  /**
   * Modify the JobConf to set the task output filter.
   * 
   * @param job the JobConf to modify.
   * @param newValue the value to set.
   */
  public static void setTaskOutputFilter(JobConf job, 
                                         TaskStatusFilter newValue) {
    job.set("jobclient.output.filter", newValue.toString());
  }
    
  /**
   * Returns task output filter.
   * @return task filter. 
   */
  @Deprecated
  public TaskStatusFilter getTaskOutputFilter(){
    return this.taskOutputFilter; 
  }

  private String getJobPriorityNames() {
    StringBuffer sb = new StringBuffer();
    for (JobPriority p : JobPriority.values()) {
      sb.append(p.name()).append(" ");
    }
    return sb.substring(0, sb.length()-1);
  }
  
  /**
   * Display usage of the command-line tool and terminate execution
   */
  private void displayUsage(String cmd) {
    String prefix = "Usage: JobClient ";
    String jobPriorityValues = getJobPriorityNames();
    String taskTypes = "map, reduce, setup, cleanup";
    String taskStates = "running, completed";
    if("-submit".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-file>]");
    } else if ("-status".equals(cmd) || "-kill".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id>]");
    } else if ("-counter".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <group-name> <counter-name>]");
    } else if ("-events".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <from-event-#> <#-of-events>]");
    } else if ("-history".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <jobOutputDir>]");
    } else if ("-list".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " [all]]");
    } else if ("-kill-task".equals(cmd) || "-fail-task".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <task-id>]");
    } else if ("-set-priority".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <priority>]. " +
          "Valid values for priorities are: " 
          + jobPriorityValues); 
    } else if ("-list-active-trackers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-blacklisted-trackers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-attempt-ids".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + 
          " <job-id> <task-type> <task-state>]. " +
          "Valid values for <task-type> are " + taskTypes + ". " +
          "Valid values for <task-state> are " + taskStates);
    } else {
      System.err.printf(prefix + "<command> <args>\n");
      System.err.printf("\t[-submit <job-file>]\n");
      System.err.printf("\t[-status <job-id>]\n");
      System.err.printf("\t[-counter <job-id> <group-name> <counter-name>]\n");
      System.err.printf("\t[-kill <job-id>]\n");
      System.err.printf("\t[-set-priority <job-id> <priority>]. " +
                                      "Valid values for priorities are: " +
                                      jobPriorityValues + "\n");
      System.err.printf("\t[-events <job-id> <from-event-#> <#-of-events>]\n");
      System.err.printf("\t[-history <jobOutputDir>]\n");
      System.err.printf("\t[-list [all]]\n");
      System.err.printf("\t[-list-active-trackers]\n");
      System.err.printf("\t[-list-blacklisted-trackers]\n");
      System.err.println("\t[-list-attempt-ids <job-id> <task-type> " +
      		"<task-state>]\n");
      System.err.printf("\t[-kill-task <task-id>]\n");
      System.err.printf("\t[-fail-task <task-id>]\n\n");
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }
    
  public int run(String[] argv) throws Exception {
    int exitCode = -1;
    if (argv.length < 1) {
      displayUsage("");
      return exitCode;
    }    
    // process arguments
    String cmd = argv[0];
    String submitJobFile = null;
    String jobid = null;
    String taskid = null;
    String outputDir = null;
    String counterGroupName = null;
    String counterName = null;
    String newPriority = null;
    String taskType = null;
    String taskState = null;
    int fromEvent = 0;
    int nEvents = 0;
    boolean getStatus = false;
    boolean getCounter = false;
    boolean killJob = false;
    boolean listEvents = false;
    boolean viewHistory = false;
    boolean viewAllHistory = false;
    boolean listJobs = false;
    boolean listAllJobs = false;
    boolean listActiveTrackers = false;
    boolean listBlacklistedTrackers = false;
    boolean displayTasks = false;
    boolean killTask = false;
    boolean failTask = false;
    boolean setJobPriority = false;

    if ("-submit".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      submitJobFile = argv[1];
    } else if ("-status".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      getStatus = true;
    } else if("-counter".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      getCounter = true;
      jobid = argv[1];
      counterGroupName = argv[2];
      counterName = argv[3];
    } else if ("-kill".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      killJob = true;
    } else if ("-set-priority".equals(cmd)) {
      if (argv.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      newPriority = argv[2];
      try {
        JobPriority jp = JobPriority.valueOf(newPriority); 
      } catch (IllegalArgumentException iae) {
        displayUsage(cmd);
        return exitCode;
      }
      setJobPriority = true; 
    } else if ("-events".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      fromEvent = Integer.parseInt(argv[2]);
      nEvents = Integer.parseInt(argv[3]);
      listEvents = true;
    } else if ("-history".equals(cmd)) {
      if (argv.length != 2 && !(argv.length == 3 && "all".equals(argv[1]))) {
         displayUsage(cmd);
         return exitCode;
      }
      viewHistory = true;
      if (argv.length == 3 && "all".equals(argv[1])) {
         viewAllHistory = true;
         outputDir = argv[2];
      } else {
         outputDir = argv[1];
      }
    } else if ("-list".equals(cmd)) {
      if (argv.length != 1 && !(argv.length == 2 && "all".equals(argv[1]))) {
        displayUsage(cmd);
        return exitCode;
      }
      if (argv.length == 2 && "all".equals(argv[1])) {
        listAllJobs = true;
      } else {
        listJobs = true;
      }
    } else if("-kill-task".equals(cmd)) {
      if(argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      killTask = true;
      taskid = argv[1];
    } else if("-fail-task".equals(cmd)) {
      if(argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      failTask = true;
      taskid = argv[1];
    } else if ("-list-active-trackers".equals(cmd)) {
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listActiveTrackers = true;
    } else if ("-list-blacklisted-trackers".equals(cmd)) {
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listBlacklistedTrackers = true;
    } else if ("-list-attempt-ids".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      taskType = argv[2];
      taskState = argv[3];
      displayTasks = true;
    } else {
      displayUsage(cmd);
      return exitCode;
    }

    // initialize JobClient
    JobConf conf = null;
    if (submitJobFile != null) {
      conf = new JobConf(submitJobFile);
    } else {
      conf = new JobConf(getConf());
    }
    init(conf);
        
    // Submit the request
    try {
      if (submitJobFile != null) {
        RunningJob job = submitJob(conf);
        System.out.println("Created job " + job.getID());
        exitCode = 0;
      } else if (getStatus) {
        RunningJob job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          Counters counters = job.getCounters();
          System.out.println();
          System.out.println(job);
          if (counters != null) {
            System.out.println(counters);
          } else {
            System.out.println("Counters not available. Job is retired.");
          }
          exitCode = 0;
        }
      } else if (getCounter) {
        RunningJob job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          Counters counters = job.getCounters();
          if (counters == null) {
            System.out.println("Counters not available for retired job " + 
                jobid);
            exitCode = -1;
          } else {
            Group group = counters.getGroup(counterGroupName);
            Counter counter = group.getCounterForName(counterName);
            System.out.println(counter.getCounter());
            exitCode = 0;
          }
        }
      } else if (killJob) {
        RunningJob job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          job.killJob();
          System.out.println("Killed job " + jobid);
          exitCode = 0;
        }
      } else if (setJobPriority) {
        RunningJob job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          job.setJobPriority(newPriority);
          System.out.println("Changed job priority.");
          exitCode = 0;
        } 
      } else if (viewHistory) {
        viewHistory(outputDir, viewAllHistory);
        exitCode = 0;
      } else if (listEvents) {
        listEvents(JobID.forName(jobid), fromEvent, nEvents);
        exitCode = 0;
      } else if (listJobs) {
        listJobs();
        exitCode = 0;
      } else if (listAllJobs) {
        listAllJobs();
        exitCode = 0;
      } else if (listActiveTrackers) {
        listActiveTrackers();
        exitCode = 0;
      } else if (listBlacklistedTrackers) {
        listBlacklistedTrackers();
        exitCode = 0;
      } else if (displayTasks) {
        displayTasks(JobID.forName(jobid), taskType, taskState);
      } else if(killTask) {
        if(jobSubmitClient.killTask(TaskAttemptID.forName(taskid), false)) {
          System.out.println("Killed task " + taskid);
          exitCode = 0;
        } else {
          System.out.println("Could not kill task " + taskid);
          exitCode = -1;
        }
      } else if(failTask) {
        if(jobSubmitClient.killTask(TaskAttemptID.forName(taskid), true)) {
          System.out.println("Killed task " + taskid + " by failing it");
          exitCode = 0;
        } else {
          System.out.println("Could not fail task " + taskid);
          exitCode = -1;
        }
      }
    } catch (RemoteException re){
      IOException unwrappedException = re.unwrapRemoteException();
      if (unwrappedException instanceof AccessControlException) {
        System.out.println(unwrappedException.getMessage());
      } else {
        throw re;
      }
    } finally {
      close();
    }
    return exitCode;
  }

  private void viewHistory(String outputDir, boolean all) 
    throws IOException {
    HistoryViewer historyViewer = new HistoryViewer(outputDir,
                                        getConf(), all);
    historyViewer.print();
  }
  
  /**
   * List the events for the given job
   * @param jobId the job id for the job's events to list
   * @throws IOException
   */
  private void listEvents(JobID jobId, int fromEventId, int numEvents)
    throws IOException {
    TaskCompletionEvent[] events = 
      jobSubmitClient.getTaskCompletionEvents(jobId, fromEventId, numEvents);
    System.out.println("Task completion events for " + jobId);
    System.out.println("Number of events (from " + fromEventId + 
                       ") are: " + events.length);
    for(TaskCompletionEvent event: events) {
      System.out.println(event.getTaskStatus() + " " + event.getTaskAttemptId() + " " + 
                         getTaskLogURL(event.getTaskAttemptId(), 
                                       event.getTaskTrackerHttp()));
    }
  }

  /**
   * Dump a list of currently running jobs
   * @throws IOException
   */
  private void listJobs() throws IOException {
    JobStatus[] jobs = jobsToComplete();
    if (jobs == null)
      jobs = new JobStatus[0];

    System.out.printf("%d jobs currently running\n", jobs.length);
    displayJobList(jobs);
  }
    
  /**
   * Dump a list of all jobs submitted.
   * @throws IOException
   */
  private void listAllJobs() throws IOException {
    JobStatus[] jobs = getAllJobs();
    if (jobs == null)
      jobs = new JobStatus[0];
    System.out.printf("%d jobs submitted\n", jobs.length);
    System.out.printf("States are:\n\tRunning : 1\tSucceded : 2" +
    "\tFailed : 3\tPrep : 4\n");
    displayJobList(jobs);
  }
  
  /**
   * Display the list of active trackers
   */
  private void listActiveTrackers() throws IOException {
    ClusterStatus c = jobSubmitClient.getClusterStatus(true);
    Collection<String> trackers = c.getActiveTrackerNames();
    for (String trackerName : trackers) {
      System.out.println(trackerName);
    }
  }

  /**
   * Display the list of blacklisted trackers
   */
  private void listBlacklistedTrackers() throws IOException {
    ClusterStatus c = jobSubmitClient.getClusterStatus(true);
    Collection<String> trackers = c.getBlacklistedTrackerNames();
    for (String trackerName : trackers) {
      System.out.println(trackerName);
    }
  }

  void displayJobList(JobStatus[] jobs) {
    System.out.printf("JobId\tState\tStartTime\tUserName\tPriority\tSchedulingInfo\n");
    for (JobStatus job : jobs) {
      System.out.printf("%s\t%d\t%d\t%s\t%s\t%s\n", job.getJobID(), job.getRunState(),
          job.getStartTime(), job.getUsername(), 
          job.getJobPriority().name(), job.getSchedulingInfo());
    }
  }

  /**
   * Get status information about the max available Maps in the cluster.
   *  
   * @return the max available Maps in the cluster
   * @throws IOException
   */
  public int getDefaultMaps() throws IOException {
    return getClusterStatus().getMaxMapTasks();
  }

  /**
   * Get status information about the max available Reduces in the cluster.
   *  
   * @return the max available Reduces in the cluster
   * @throws IOException
   */
  public int getDefaultReduces() throws IOException {
    return getClusterStatus().getMaxReduceTasks();
  }

  /**
   * Grab the jobtracker system directory path where job-specific files are to be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public Path getSystemDir() {
    if (sysDir == null) {
      sysDir = new Path(jobSubmitClient.getSystemDir());
    }
    return sysDir;
  }
  
  
  /**
   * Return an array of queue information objects about all the Job Queues
   * configured.
   * 
   * @return Array of JobQueueInfo objects
   * @throws IOException
   */
  public JobQueueInfo[] getQueues() throws IOException {
    return jobSubmitClient.getQueues();
  }
  
  /**
   * Gets all the jobs which were added to particular Job Queue
   * 
   * @param queueName name of the Job Queue
   * @return Array of jobs present in the job queue
   * @throws IOException
   */
  
  public JobStatus[] getJobsFromQueue(String queueName) throws IOException {
    return jobSubmitClient.getJobsFromQueue(queueName);
  }
  
  /**
   * Gets the queue information associated to a particular Job Queue
   * 
   * @param queueName name of the job queue.
   * @return Queue information associated to particular queue.
   * @throws IOException
   */
  public JobQueueInfo getQueueInfo(String queueName) throws IOException {
    return jobSubmitClient.getQueueInfo(queueName);
  }
  
  /**
   * Gets the Queue ACLs for current user
   * @return array of QueueAclsInfo object for current user.
   * @throws IOException
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException {
    return jobSubmitClient.getQueueAclsForCurrentUser();
  }
  /* Get a delegation token for the user from the JobTracker.
   * @param renewer the user who can renew the token
   * @return the new token
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> 
    getDelegationToken(Text renewer) throws IOException, InterruptedException {
    Token<DelegationTokenIdentifier> result =
      jobSubmitClient.getDelegationToken(renewer);
    InetSocketAddress addr = JobTracker.getAddress(getConf());
    StringBuilder service = new StringBuilder();
    service.append(NetUtils.normalizeHostName(addr.getAddress().
                                              getHostAddress()));
    service.append(':');
    service.append(addr.getPort());
    result.setService(new Text(service.toString()));
    return result;
  }

  /**
   * Renew a delegation token
   * @param token the token to renew
   * @return the new expiration time
   * @throws InvalidToken
   * @throws IOException
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
  throws InvalidToken, IOException, InterruptedException {
    try {
      return jobSubmitClient.renewDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }

  /**
   * Cancel a delegation token from the JobTracker
   * @param token the token to cancel
   * @throws IOException
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                    ) throws IOException, 
                                             InterruptedException {
    try {
      jobSubmitClient.cancelDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }
 
  /**
   */
  public static void main(String argv[]) throws Exception {
    int res = ToolRunner.run(new JobClient(), argv);
    System.exit(res);
  }
  
  @SuppressWarnings("unchecked")
  private void readTokensFromFiles(Configuration conf, Credentials credentials
                                   ) throws IOException {
    // add tokens and secrets coming from a token storage file
    String binaryTokenFilename =
      conf.get("mapreduce.job.credentials.binary");
    if (binaryTokenFilename != null) {
      Credentials binary = 
        Credentials.readTokenStorageFile(new Path("file:///" +  
                                                  binaryTokenFilename), conf);
      credentials.addAll(binary);
    }
    // add secret keys coming from a json file
    String tokensFileName = conf.get("mapreduce.job.credentials.json");
    if(tokensFileName != null) {
      LOG.info("loading user's secret keys from " + tokensFileName);
      String localFileName = new Path(tokensFileName).toUri().getPath();

      boolean json_error = false;
      try {
        // read JSON
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> nm = 
          mapper.readValue(new File(localFileName), Map.class);

        for(Map.Entry<String, String> ent: nm.entrySet()) {
          credentials.addSecretKey(new Text(ent.getKey()), 
                                   ent.getValue().getBytes());
        }
      } catch (JsonMappingException e) {
        json_error = true;
      } catch (JsonParseException e) {
        json_error = true;
      }
      if(json_error)
        LOG.warn("couldn't parse Token Cache JSON file with user secret keys");
    }
  }

  //get secret keys and tokens and store them into TokenCache
  @SuppressWarnings("unchecked")
  private void populateTokenCache(Configuration conf, Credentials credentials) 
  throws IOException{
    readTokensFromFiles(conf, credentials);
 
    // add the delegation tokens from configuration
    String [] nameNodes = conf.getStrings(JobContext.JOB_NAMENODES);
    LOG.debug("adding the following namenodes' delegation tokens:" + 
              Arrays.toString(nameNodes));
    if(nameNodes != null) {
      Path [] ps = new Path[nameNodes.length];
      for(int i=0; i< nameNodes.length; i++) {
        ps[i] = new Path(nameNodes[i]);
      }
      TokenCache.obtainTokensForNamenodes(credentials, ps, conf);
    }
  }
}

