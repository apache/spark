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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;

/** Implements MapReduce locally, in-process, for debugging. */ 
class LocalJobRunner implements JobSubmissionProtocol {
  public static final Log LOG =
    LogFactory.getLog(LocalJobRunner.class);

  private FileSystem fs;
  private HashMap<JobID, Job> jobs = new HashMap<JobID, Job>();
  private JobConf conf;
  private int map_tasks = 0;
  private int reduce_tasks = 0;
  final Random rand = new Random();
  private final TaskController taskController = new DefaultTaskController();

  private JobTrackerInstrumentation myMetrics = null;

  private static final String jobDir =  "localRunner/";
  
  public long getProtocolVersion(String protocol, long clientVersion) {
    return JobSubmissionProtocol.versionID;
  }
  
  private class Job extends Thread
    implements TaskUmbilicalProtocol {
    // The job directory on the system: JobClient places job configurations here.
    // This is analogous to JobTracker's system directory.
    private Path systemJobDir;
    private Path systemJobFile;

    // The job directory for the task.  Analagous to a task's job directory.
    private Path localJobDir;
    private Path localJobFile;

    private JobID id;
    private JobConf job;

    private JobStatus status;
    private ArrayList<TaskAttemptID> mapIds = new ArrayList<TaskAttemptID>();

    private JobProfile profile;
    private FileSystem localFs;
    boolean killed = false;
    
    private TrackerDistributedCacheManager trackerDistributedCacheManager;
    private TaskDistributedCacheManager taskDistributedCacheManager;
    
    // Counters summed over all the map/reduce tasks which
    // have successfully completed
    private Counters completedTaskCounters = new Counters();
    
    // Current counters, including incomplete task(s)
    private Counters currentCounters = new Counters();

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    public Job(JobID jobid, String jobSubmitDir) throws IOException {
      this.systemJobDir = new Path(jobSubmitDir);
      this.systemJobFile = new Path(systemJobDir, "job.xml");
      this.id = jobid;

      this.localFs = FileSystem.getLocal(conf);

      this.localJobDir = localFs.makeQualified(conf.getLocalPath(jobDir));
      this.localJobFile = new Path(this.localJobDir, id + ".xml");

      // Manage the distributed cache.  If there are files to be copied,
      // this will trigger localFile to be re-written again.
      this.trackerDistributedCacheManager =
        new TrackerDistributedCacheManager(conf, taskController);
      this.taskDistributedCacheManager =
        trackerDistributedCacheManager.newTaskDistributedCacheManager(
            jobid, conf);
      taskDistributedCacheManager.setupCache(conf, "archive", "archive");
      
      if (DistributedCache.getSymlink(conf)) {
        // This is not supported largely because,
        // for a Child subprocess, the cwd in LocalJobRunner
        // is not a fresh slate, but rather the user's working directory.
        // This is further complicated because the logic in
        // setupWorkDir only creates symlinks if there's a jarfile
        // in the configuration.
        LOG.warn("LocalJobRunner does not support " +
        "symlinking into current working dir.");
      }
      // Setup the symlinks for the distributed cache.
      TaskRunner.setupWorkDir(conf, new File(localJobDir.toUri()).getAbsoluteFile());

      // Write out configuration file.  Instead of copying it from
      // systemJobFile, we re-write it, since setup(), above, may have
      // updated it.
      OutputStream out = localFs.create(localJobFile);
      try {
        conf.writeXml(out);
      } finally {
        out.close();
      }
      this.job = new JobConf(localJobFile);

      // Job (the current object) is a Thread, so we wrap its class loader.
      if (!taskDistributedCacheManager.getClassPaths().isEmpty()) {
        setContextClassLoader(taskDistributedCacheManager.makeClassLoader(
            getContextClassLoader()));
      }

      profile = new JobProfile(job.getUser(), id, systemJobFile.toString(), 
                               "http://localhost:8080/", job.getJobName());
      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING);

      jobs.put(id, this);

      this.start();
    }

    JobProfile getProfile() {
      return profile;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      JobID jobId = profile.getJobID();
      JobContext jContext = new JobContext(conf, jobId);
      OutputCommitter outputCommitter = job.getOutputCommitter();
      try {
        TaskSplitMetaInfo[] taskSplitMetaInfos =
          SplitMetaInfoReader.readSplitMetaInfo(jobId, localFs, conf, systemJobDir);        
        int numReduceTasks = job.getNumReduceTasks();
        if (numReduceTasks > 1 || numReduceTasks < 0) {
          // we only allow 0 or 1 reducer in local mode
          numReduceTasks = 1;
          job.setNumReduceTasks(1);
        }
        outputCommitter.setupJob(jContext);
        status.setSetupProgress(1.0f);

        Map<TaskAttemptID, MapOutputFile> mapOutputFiles =
          new HashMap<TaskAttemptID, MapOutputFile>();
        for (int i = 0; i < taskSplitMetaInfos.length; i++) {
          if (!this.isInterrupted()) {
            TaskAttemptID mapId = new TaskAttemptID(new TaskID(jobId, true, i),0);  
            mapIds.add(mapId);
            MapTask map = new MapTask(systemJobFile.toString(),  
                                      mapId, i,
                                      taskSplitMetaInfos[i].getSplitIndex(), 1);
            map.setUser(UserGroupInformation.getCurrentUser().
                getShortUserName());
            JobConf localConf = new JobConf(job);
            TaskRunner.setupChildMapredLocalDirs(map, localConf);

            MapOutputFile mapOutput = new MapOutputFile();
            mapOutput.setConf(localConf);
            mapOutputFiles.put(mapId, mapOutput);

            map.setJobFile(localJobFile.toString());
            localConf.setUser(map.getUser());
            map.localizeConfiguration(localConf);
            map.setConf(localConf);
            map_tasks += 1;
            myMetrics.launchMap(mapId);
            map.run(localConf, this);
            myMetrics.completeMap(mapId);
            map_tasks -= 1;
            updateCounters(map);
          } else {
            throw new InterruptedException();
          }
        }
        TaskAttemptID reduceId = 
          new TaskAttemptID(new TaskID(jobId, false, 0), 0);
        try {
          if (numReduceTasks > 0) {
            ReduceTask reduce =
                new ReduceTask(systemJobFile.toString(), reduceId, 0, mapIds.size(),
                    1);
            reduce.setUser(UserGroupInformation.getCurrentUser().
                 getShortUserName());
            JobConf localConf = new JobConf(job);
            TaskRunner.setupChildMapredLocalDirs(reduce, localConf);
            // move map output to reduce input  
            for (int i = 0; i < mapIds.size(); i++) {
              if (!this.isInterrupted()) {
                TaskAttemptID mapId = mapIds.get(i);
                Path mapOut = mapOutputFiles.get(mapId).getOutputFile();
                MapOutputFile localOutputFile = new MapOutputFile();
                localOutputFile.setConf(localConf);
                Path reduceIn =
                  localOutputFile.getInputFileForWrite(mapId.getTaskID(),
                        localFs.getFileStatus(mapOut).getLen());
                if (!localFs.mkdirs(reduceIn.getParent())) {
                  throw new IOException("Mkdirs failed to create "
                      + reduceIn.getParent().toString());
                }
                if (!localFs.rename(mapOut, reduceIn))
                  throw new IOException("Couldn't rename " + mapOut);
              } else {
                throw new InterruptedException();
              }
            }
            if (!this.isInterrupted()) {
              reduce.setJobFile(localJobFile.toString());
              localConf.setUser(reduce.getUser());
              reduce.localizeConfiguration(localConf);
              reduce.setConf(localConf);
              reduce_tasks += 1;
              myMetrics.launchReduce(reduce.getTaskID());
              reduce.run(localConf, this);
              myMetrics.completeReduce(reduce.getTaskID());
              reduce_tasks -= 1;
              updateCounters(reduce);
            } else {
              throw new InterruptedException();
            }
          }
        } finally {
          for (MapOutputFile output : mapOutputFiles.values()) {
            output.removeAll();
          }
        }
        // delete the temporary directory in output directory
        outputCommitter.commitJob(jContext);
        status.setCleanupProgress(1.0f);

        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.SUCCEEDED);
        }

        JobEndNotifier.localRunnerNotification(job, status);

      } catch (Throwable t) {
        try {
          outputCommitter.abortJob(jContext, JobStatus.FAILED);
        } catch (IOException ioe) {
          LOG.info("Error cleaning up job:" + id);
        }
        status.setCleanupProgress(1.0f);
        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.FAILED);
        }
        LOG.warn(id, t);

        JobEndNotifier.localRunnerNotification(job, status);

      } finally {
        try {
          fs.delete(systemJobFile.getParent(), true);  // delete submit dir
          localFs.delete(localJobFile, true);              // delete local copy
          // Cleanup distributed cache
          taskDistributedCacheManager.release();
          trackerDistributedCacheManager.purgeCache();
        } catch (IOException e) {
          LOG.warn("Error cleaning up "+id+": "+e);
        }
      }
    }

    // TaskUmbilicalProtocol methods

    public JvmTask getTask(JvmContext context) { return null; }

    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus, JvmContext context) 
    throws IOException, InterruptedException {
      LOG.info(taskStatus.getStateString());
      float taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        float numTasks = mapIds.size();
        status.setMapProgress(taskIndex/numTasks + taskStatus.getProgress()/numTasks);
      } else {
        status.setReduceProgress(taskStatus.getProgress());
      }
      currentCounters = Counters.sum(completedTaskCounters, taskStatus.getCounters());
      
      // ignore phase
      
      return true;
    }

    /**
     * Task is reporting that it is in commit_pending
     * and it is waiting for the commit Response
     */
    public void commitPending(TaskAttemptID taskid,
                              TaskStatus taskStatus,
                              JvmContext jvmContext) 
    throws IOException, InterruptedException {
      statusUpdate(taskid, taskStatus, jvmContext);
    }

    /**
     * Updates counters corresponding to completed tasks.
     * @param task A map or reduce task which has just been 
     * successfully completed
     */ 
    private void updateCounters(Task task) {
      completedTaskCounters.incrAllCounters(task.getCounters());
    }

    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace,
        JvmContext jvmContext) {
      // Ignore for now
    }
    
    public void reportNextRecordRange(TaskAttemptID taskid, 
        SortedRanges.Range range, JvmContext jvmContext) throws IOException {
      LOG.info("Task " + taskid + " reportedNextRecordRange " + range);
    }

    public boolean ping(TaskAttemptID taskid, JvmContext jvmContext) throws IOException {
      return true;
    }
    
    public boolean canCommit(TaskAttemptID taskid, JvmContext jvmContext) 
    throws IOException {
      return true;
    }
    
    public void done(TaskAttemptID taskId, JvmContext jvmContext)
        throws IOException {
      int taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) { // mapping
        status.setMapProgress(1.0f);
      } else {
        status.setReduceProgress(1.0f);
      }
    }

    public synchronized void fsError(TaskAttemptID taskId, String message,
        JvmContext jvmContext) throws IOException {
      LOG.fatal("FSError: " + message + "from task: " + taskId);
    }

    public void shuffleError(TaskAttemptID taskId, String message,
        JvmContext jvmContext) throws IOException {
      LOG.fatal("shuffleError: " + message + "from task: " + taskId);
    }
    
    public synchronized void fatalError(TaskAttemptID taskId, String msg,
        JvmContext jvmContext) throws IOException {
      LOG.fatal("Fatal: " + msg + "from task: " + taskId);
    }
    
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId,
        int fromEventId, int maxLocs, TaskAttemptID id, JvmContext jvmContext)
        throws IOException {
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY,
          false);
    }

    @Override
    public void updatePrivateDistributedCacheSizes(
                                                   org.apache.hadoop.mapreduce.JobID jobId,
                                                   long[] sizes)
                                                                throws IOException {
      trackerDistributedCacheManager.setArchiveSizes(jobId, sizes);
    }
    
  }

  public LocalJobRunner(JobConf conf) throws IOException {
    this.fs = FileSystem.getLocal(conf);
    this.conf = conf;
    myMetrics = new JobTrackerMetricsInst(null, new JobConf(conf));
    taskController.setConf(conf);
  }

  // JobSubmissionProtocol methods

  private static int jobid = 0;
  public synchronized JobID getNewJobId() {
    return new JobID("local", ++jobid);
  }

  public JobStatus submitJob(JobID jobid, String jobSubmitDir, 
                             Credentials credentials) 
  throws IOException {
    Job job = new Job(jobid, jobSubmitDir);
    job.job.setCredentials(credentials);
    return job.status;
  }

  public void killJob(JobID id) {
    jobs.get(id).killed = true;
    jobs.get(id).interrupt();
  }

  public void setJobPriority(JobID id, String jp) throws IOException {
    throw new UnsupportedOperationException("Changing job priority " +
                      "in LocalJobRunner is not supported.");
  }
  
  /** Throws {@link UnsupportedOperationException} */
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
    throw new UnsupportedOperationException("Killing tasks in " +
    "LocalJobRunner is not supported");
  }

  public JobProfile getJobProfile(JobID id) {
    Job job = jobs.get(id);
    if(job != null)
      return job.getProfile();
    else 
      return null;
  }

  public TaskReport[] getMapTaskReports(JobID id) {
    return new TaskReport[0];
  }
  public TaskReport[] getReduceTaskReports(JobID id) {
    return new TaskReport[0];
  }
  public TaskReport[] getCleanupTaskReports(JobID id) {
    return new TaskReport[0];
  }
  public TaskReport[] getSetupTaskReports(JobID id) {
    return new TaskReport[0];
  }

  public JobStatus getJobStatus(JobID id) {
    Job job = jobs.get(id);
    if(job != null)
      return job.status;
    else 
      return null;
  }
  
  public Counters getJobCounters(JobID id) {
    Job job = jobs.get(id);
    return job.currentCounters;
  }

  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }
  
  public ClusterStatus getClusterStatus(boolean detailed) {
    return new ClusterStatus(1, 0, 0, map_tasks, reduce_tasks, 1, 1, 
                             JobTracker.State.RUNNING);
  }

  public JobStatus[] jobsToComplete() {return null;}

  public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid
      , int fromEventId, int maxEvents) throws IOException {
    return TaskCompletionEvent.EMPTY_ARRAY;
  }
  
  public JobStatus[] getAllJobs() {return null;}

  
  /**
   * Returns the diagnostic information for a particular task in the given job.
   * To be implemented
   */
  public String[] getTaskDiagnostics(TaskAttemptID taskid)
  		throws IOException{
	  return new String [0];
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getQueueAdmins()
   */
  public AccessControlList getQueueAdmins(String queueName) throws IOException {
    return new AccessControlList(" ");// no queue admins for local job runner
  }  

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getStagingAreaDir()
   */
  public String getStagingAreaDir() throws IOException {
    Path stagingRootDir = 
      new Path(conf.get("mapreduce.jobtracker.staging.root.dir",
        "/tmp/hadoop/mapred/staging"));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user;
    if (ugi != null) {
      user = ugi.getShortUserName() + rand.nextInt();
    } else {
      user = "dummy" + rand.nextInt();
    }
    return fs.makeQualified(new Path(stagingRootDir, user+"/.staging")).toString();
  }


  @Override
  public JobStatus[] getJobsFromQueue(String queue) throws IOException {
    return null;
  }

  @Override
  public JobQueueInfo[] getQueues() throws IOException {
    return null;
  }


  @Override
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return null;
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException{
    return null;
  }
  
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                       ) throws IOException,
                                                InterruptedException {
  }  
  @Override
  public Token<DelegationTokenIdentifier> 
     getDelegationToken(Text renewer) throws IOException, InterruptedException {
    return null;
  }  
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                      ) throws IOException,InterruptedException{
    return 0;
  }

}
