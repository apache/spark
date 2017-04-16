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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * Persists and retrieves the Job info of a job into/from DFS.
 * <p/>
 * If the retain time is zero jobs are not persisted.
 * <p/>
 * A daemon thread cleans up job info files older than the retain time
 * <p/>
 * The retain time can be set with the 'persist.jobstatus.hours'
 * configuration variable (it is in hours).
 */
class CompletedJobStatusStore implements Runnable {
  private boolean active;
  private String jobInfoDir;
  private long retainTime;
  private FileSystem fs;
  private static final String JOB_INFO_STORE_DIR = "/jobtracker/jobsInfo";

  private ACLsManager aclsManager;

  public static final Log LOG =
          LogFactory.getLog(CompletedJobStatusStore.class);

  private static long HOUR = 1000 * 60 * 60;
  private static long SLEEP_TIME = 1 * HOUR;
  final static FsPermission JOB_STATUS_STORE_DIR_PERMISSION = FsPermission
      .createImmutable((short) 0750); // rwxr-x--

  CompletedJobStatusStore(Configuration conf, ACLsManager aclsManager)
      throws IOException {
    active =
      conf.getBoolean("mapred.job.tracker.persist.jobstatus.active", false);

    if (active) {
      retainTime =
        conf.getInt("mapred.job.tracker.persist.jobstatus.hours", 0) * HOUR;

      jobInfoDir =
        conf.get("mapred.job.tracker.persist.jobstatus.dir", JOB_INFO_STORE_DIR);

      Path path = new Path(jobInfoDir);
      
      // set the fs
      this.fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        if (!fs.mkdirs(path, new FsPermission(JOB_STATUS_STORE_DIR_PERMISSION))) {
          throw new IOException(
              "CompletedJobStatusStore mkdirs failed to create "
                  + path.toString());
        }
      } else {
        FileStatus stat = fs.getFileStatus(path);
        FsPermission actual = stat.getPermission();
        if (!stat.isDir())
          throw new DiskErrorException("not a directory: "
                                   + path.toString());
        FsAction user = actual.getUserAction();
        if (!user.implies(FsAction.READ))
          throw new DiskErrorException("directory is not readable: "
                                   + path.toString());
        if (!user.implies(FsAction.WRITE))
          throw new DiskErrorException("directory is not writable: "
                                   + path.toString());
      }

      if (retainTime == 0) {
        // as retain time is zero, all stored jobstatuses are deleted.
        deleteJobStatusDirs();
      }

      this.aclsManager = aclsManager;

      LOG.info("Completed job store activated/configured with retain-time : " 
               + retainTime + " , job-info-dir : " + jobInfoDir);
    } else {
      LOG.info("Completed job store is inactive");
    }
  }

  /**
   * Indicates if job status persistency is active or not.
   *
   * @return TRUE if active, FALSE otherwise.
   */
  public boolean isActive() {
    return active;
  }

  public void run() {
    if (retainTime > 0) {
      while (true) {
        deleteJobStatusDirs();
        try {
          Thread.sleep(SLEEP_TIME);
        }
        catch (InterruptedException ex) {
          break;
        }
      }
    }
  }

  private void deleteJobStatusDirs() {
    try {
      long currentTime = System.currentTimeMillis();
      FileStatus[] jobInfoFiles = fs.listStatus(
              new Path[]{new Path(jobInfoDir)});

      //noinspection ForLoopReplaceableByForEach
      for (FileStatus jobInfo : jobInfoFiles) {
        try {
          if ((currentTime - jobInfo.getModificationTime()) > retainTime) {
            fs.delete(jobInfo.getPath(), true);
          }
        }
        catch (IOException ie) {
          LOG.warn("Could not do housekeeping for [ " +
                  jobInfo.getPath() + "] job info : " + ie.getMessage(), ie);
        }
      }
    }
    catch (IOException ie) {
      LOG.warn("Could not obtain job info files : " + ie.getMessage(), ie);
    }
  }

  private Path getInfoFilePath(JobID jobId) {
    return new Path(jobInfoDir, jobId + ".info");
  }
  
  /**
   * Persists a job in DFS.
   *
   * @param job the job about to be 'retired'
   */
  public void store(JobInProgress job) {
    if (active && retainTime > 0) {
      JobID jobId = job.getStatus().getJobID();
      Path jobStatusFile = getInfoFilePath(jobId);
      try {
        FSDataOutputStream dataOut = fs.create(jobStatusFile);

        job.getStatus().write(dataOut);

        job.getProfile().write(dataOut);
        
        Counters counters = new Counters();
        boolean isFine = job.getCounters(counters);
        counters = (isFine? counters: new Counters());
        counters.write(dataOut);

        TaskCompletionEvent[] events = 
                job.getTaskCompletionEvents(0, Integer.MAX_VALUE);
        dataOut.writeInt(events.length);
        for (TaskCompletionEvent event : events) {
          event.write(dataOut);
        }

        dataOut.close();
      } catch (IOException ex) {
        LOG.warn("Could not store [" + jobId + "] job info : " +
                 ex.getMessage(), ex);
        try {
          fs.delete(jobStatusFile, true);
        }
        catch (IOException ex1) {
          //ignore
        }
      }
    }
  }

  private FSDataInputStream getJobInfoFile(JobID jobId) throws IOException {
    Path jobStatusFile = getInfoFilePath(jobId);
    return (fs.exists(jobStatusFile)) ? fs.open(jobStatusFile) : null;
  }

  private JobStatus readJobStatus(FSDataInputStream dataIn) throws IOException {
    JobStatus jobStatus = new JobStatus();
    jobStatus.readFields(dataIn);
    return jobStatus;
  }

  private JobProfile readJobProfile(FSDataInputStream dataIn)
          throws IOException {
    JobProfile jobProfile = new JobProfile();
    jobProfile.readFields(dataIn);
    return jobProfile;
  }

  private Counters readCounters(FSDataInputStream dataIn) throws IOException {
    Counters counters = new Counters();
    counters.readFields(dataIn);
    return counters;
  }

  private TaskCompletionEvent[] readEvents(FSDataInputStream dataIn,
                                           int offset, int len)
          throws IOException {
    int size = dataIn.readInt();
    if (offset > size) {
      return TaskCompletionEvent.EMPTY_ARRAY;
    }
    if (offset + len > size) {
      len = size - offset;
    }
    TaskCompletionEvent[] events = new TaskCompletionEvent[len];
    for (int i = 0; i < (offset + len); i++) {
      TaskCompletionEvent event = new TaskCompletionEvent();
      event.readFields(dataIn);
      if (i >= offset) {
        events[i - offset] = event;
      }
    }
    return events;
  }

  /**
   * This method retrieves JobStatus information from DFS stored using
   * store method.
   *
   * @param jobId the jobId for which jobStatus is queried
   * @return JobStatus object, null if not able to retrieve
   */
  public JobStatus readJobStatus(JobID jobId) {
    JobStatus jobStatus = null;
    
    if (null == jobId) {
      LOG.warn("Could not read job status for null jobId");
      return null;
    }
    
    if (active) {
      try {
        FSDataInputStream dataIn = getJobInfoFile(jobId);
        if (dataIn != null) {
          jobStatus = readJobStatus(dataIn);
          dataIn.close();
        }
      } catch (IOException ex) {
        LOG.warn("Could not read [" + jobId + "] job status : " + ex, ex);
      }
    }
    return jobStatus;
  }

  /**
   * This method retrieves JobProfile information from DFS stored using
   * store method.
   *
   * @param jobId the jobId for which jobProfile is queried
   * @return JobProfile object, null if not able to retrieve
   */
  public JobProfile readJobProfile(JobID jobId) {
    JobProfile jobProfile = null;
    if (active) {
      try {
        FSDataInputStream dataIn = getJobInfoFile(jobId);
        if (dataIn != null) {
          readJobStatus(dataIn);
          jobProfile = readJobProfile(dataIn);
          dataIn.close();
        }
      } catch (IOException ex) {
        LOG.warn("Could not read [" + jobId + "] job profile : " + ex, ex);
      }
    }
    return jobProfile;
  }

  /**
   * This method retrieves Counters information from file stored using
   * store method.
   *
   * @param jobId the jobId for which Counters is queried
   * @return Counters object, null if not able to retrieve
   * @throws AccessControlException 
   */
  public Counters readCounters(JobID jobId) throws AccessControlException {
    Counters counters = null;
    if (active) {
      try {
        FSDataInputStream dataIn = getJobInfoFile(jobId);
        if (dataIn != null) {
          JobStatus jobStatus = readJobStatus(dataIn);
          JobProfile profile = readJobProfile(dataIn);
          String queue = profile.getQueueName();
          // authorize the user for job view access
          aclsManager.checkAccess(jobStatus,
              UserGroupInformation.getCurrentUser(), queue,
              Operation.VIEW_JOB_COUNTERS);

          counters = readCounters(dataIn);
          dataIn.close();
        }
      } catch (AccessControlException ace) {
        throw ace;
      } catch (IOException ex) {
        LOG.warn("Could not read [" + jobId + "] job counters : " + ex, ex);
      }
    }
    return counters;
  }

  /**
   * This method retrieves TaskCompletionEvents information from DFS stored
   * using store method.
   *
   * @param jobId       the jobId for which TaskCompletionEvents is queried
   * @param fromEventId events offset
   * @param maxEvents   max number of events
   * @return TaskCompletionEvent[], empty array if not able to retrieve
   */
  public TaskCompletionEvent[] readJobTaskCompletionEvents(JobID jobId,
                                                               int fromEventId,
                                                               int maxEvents) {
    TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
    if (active) {
      try {
        FSDataInputStream dataIn = getJobInfoFile(jobId);
        if (dataIn != null) {
          readJobStatus(dataIn);
          readJobProfile(dataIn);
          readCounters(dataIn);
          events = readEvents(dataIn, fromEventId, maxEvents);
          dataIn.close();
        }
      } catch (IOException ex) {
        LOG.warn("Could not read [" + jobId + "] job events : " + ex, ex);
      }
    }
    return events;
  }

}
