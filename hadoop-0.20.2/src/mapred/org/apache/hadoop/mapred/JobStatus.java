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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.authorize.AccessControlList;

/**************************************************
 * Describes the current status of a job.
 * 
 * @see JobProfile for some more information.
 **************************************************/
public class JobStatus implements Writable, Cloneable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (JobStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new JobStatus(); }
       });
  }

  public static final int RUNNING = 1;
  public static final int SUCCEEDED = 2;
  public static final int FAILED = 3;
  public static final int PREP = 4;
  public static final int KILLED = 5;

  private Map<JobACL, AccessControlList> jobACLs =
    new HashMap<JobACL, AccessControlList>();

  private static final String UNKNOWN = "UNKNOWN";
  private static final String[] runStates =
      {UNKNOWN, "RUNNING", "SUCCEEDED", "FAILED", "PREP", "KILLED"};
  
  /**
   * Helper method to get human-readable state of the job.
   * @param state job state
   * @return human-readable state of the job
   */
  public static String getJobRunState(int state) {
    if (state < 1 || state >= runStates.length) {
      return UNKNOWN;
    }
    return runStates[state];
  }
  
  private JobID jobid;
  private float mapProgress;
  private float reduceProgress;
  private float cleanupProgress;
  private float setupProgress;
  private int runState;
  private long startTime;
  private String user;
  private JobPriority priority;
  private String schedulingInfo="NA";
  private String failureInfo = "NA";
  
  /**
   */
  public JobStatus() {
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on cleanup
   * @param runState The current state of the job
   */
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                   float cleanupProgress, int runState) {
    this(jobid, mapProgress, reduceProgress, cleanupProgress, runState,
                  JobPriority.NORMAL);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   */
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                   int runState) {
    this(jobid, mapProgress, reduceProgress, 0.0f, runState);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   * @param jp Priority of the job.
   */
   public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                      float cleanupProgress, int runState, JobPriority jp) {
     this(jobid, 0.0f, mapProgress, reduceProgress, 
          cleanupProgress, runState, jp);
   }
   
  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param setupProgress The progress made on the setup
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on the cleanup
   * @param runState The current state of the job
   * @param jp Priority of the job.
   */
   public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                    float reduceProgress, float cleanupProgress, 
                    int runState, JobPriority jp) {
     this.jobid = jobid;
     this.setupProgress = setupProgress;
     this.mapProgress = mapProgress;
     this.reduceProgress = reduceProgress;
     this.cleanupProgress = cleanupProgress;
     this.runState = runState;
     this.user = "nobody";
     if (jp == null) {
       throw new IllegalArgumentException("Job Priority cannot be null.");
     }
     priority = jp;
   }

  /**
   * @deprecated use getJobID instead
   */
  @Deprecated
  public String getJobId() { return jobid.toString(); }
  
  /**
   * @return The jobid of the Job
   */
  public JobID getJobID() { return jobid; }
    
  /**
   * @return Percentage of progress in maps 
   */
  public synchronized float mapProgress() { return mapProgress; }

  /**
   * Set the job acls
   * 
   * @param acls {@link Map} from {@link JobACL}  to {@link AccessControlList} 
   */
  protected synchronized void setJobACLs(Map<JobACL, AccessControlList> acls) {
    this.jobACLs = acls;
  }

  /**
   * Sets the map progress of this job
   * @param p The value of map progress to set to
   */
  synchronized void setMapProgress(float p) { 
    this.mapProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * @return Percentage of progress in cleanup 
   */
  public synchronized float cleanupProgress() { return cleanupProgress; }
    
  /**
   * Sets the cleanup progress of this job
   * @param p The value of cleanup progress to set to
   */
  synchronized void setCleanupProgress(float p) { 
    this.cleanupProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * @return Percentage of progress in setup 
   */
  public synchronized float setupProgress() { return setupProgress; }
    
  /**
   * Sets the setup progress of this job
   * @param p The value of setup progress to set to
   */
  synchronized void setSetupProgress(float p) { 
    this.setupProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * @return Percentage of progress in reduce 
   */
  public synchronized float reduceProgress() { return reduceProgress; }
    
  /**
   * Sets the reduce progress of this Job
   * @param p The value of reduce progress to set to
   */
  synchronized void setReduceProgress(float p) { 
    this.reduceProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }
    
  /**
   * @return running state of the job
   */
  public synchronized int getRunState() { return runState; }
    
  /**
   * Change the current run state of the job.
   */
  public synchronized void setRunState(int state) {
    this.runState = state;
  }

  /** 
   * Set the start time of the job
   * @param startTime The startTime of the job
   */
  synchronized void setStartTime(long startTime) { this.startTime = startTime;}
    
  /**
   * @return start time of the job
   */
  synchronized public long getStartTime() { return startTime;}

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen since we do implement Clonable
      throw new InternalError(cnse.toString());
    }
  }
  
  /**
   * @param user The username of the job
   */
  synchronized void setUsername(String userName) { this.user = userName;}

  /**
   * @return the username of the job
   */
  public synchronized String getUsername() { return this.user;}
  
  /**
   * Gets the Scheduling information associated to a particular Job.
   * @return the scheduling information of the job
   */
  public synchronized String getSchedulingInfo() {
   return schedulingInfo;
  }
  
  /**
   * gets any available info on the reason of failure of the job.
   * @return diagnostic information on why a job might have failed.
   */
  public synchronized String getFailureInfo() {
    return this.failureInfo;
  }

  /**
   * set the reason for failuire of this job
   * @param failureInfo the reason for failure of this job.
   */
  public synchronized void setFailureInfo(String failureInfo) {
    this.failureInfo = failureInfo;
  }
  
  /**
   * Used to set the scheduling information associated to a particular Job.
   * 
   * @param schedulingInfo Scheduling information of the job
   */
  public synchronized void setSchedulingInfo(String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }
  
  /**
   * Get the acls for Job.
   * 
   * @return a {@link Map} from {@link JobACL} to {@link AccessControlList}
   */
  public synchronized Map<JobACL, AccessControlList> getJobACLs() {
    return jobACLs;
  }

  /**
   * Return the priority of the job
   * @return job priority
   */
   public synchronized JobPriority getJobPriority() { return priority; }
  
  /**
   * Set the priority of the job, defaulting to NORMAL.
   * @param jp new job priority
   */
   public synchronized void setJobPriority(JobPriority jp) {
     if (jp == null) {
       throw new IllegalArgumentException("Job priority cannot be null.");
     }
     priority = jp;
   }
  
   /**
    * Returns true if the status is for a completed job.
    */
   public synchronized boolean isJobComplete() {
     return (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED 
             || runState == JobStatus.KILLED);
   }

  ///////////////////////////////////////
  // Writable
  ///////////////////////////////////////
  public synchronized void write(DataOutput out) throws IOException {
    jobid.write(out);
    out.writeFloat(setupProgress);
    out.writeFloat(mapProgress);
    out.writeFloat(reduceProgress);
    out.writeFloat(cleanupProgress);
    out.writeInt(runState);
    out.writeLong(startTime);
    Text.writeString(out, user);
    WritableUtils.writeEnum(out, priority);
    Text.writeString(out, schedulingInfo);

    // Serialize the job's ACLs
    out.writeInt(jobACLs.size());
    for (Entry<JobACL, AccessControlList> entry : jobACLs.entrySet()) {
      WritableUtils.writeEnum(out, entry.getKey());
      entry.getValue().write(out);
    }
    Text.writeString(out, failureInfo);
  }

  public synchronized void readFields(DataInput in) throws IOException {
    this.jobid = JobID.read(in);
    this.setupProgress = in.readFloat();
    this.mapProgress = in.readFloat();
    this.reduceProgress = in.readFloat();
    this.cleanupProgress = in.readFloat();
    this.runState = in.readInt();
    this.startTime = in.readLong();
    this.user = Text.readString(in);
    this.priority = WritableUtils.readEnum(in, JobPriority.class);
    this.schedulingInfo = Text.readString(in);

    // De-serialize the job's ACLs
    int numACLs = in.readInt();
    for (int i = 0; i < numACLs; i++) {
      JobACL aclType = WritableUtils.readEnum(in, JobACL.class);
      AccessControlList acl = new AccessControlList(" ");
      acl.readFields(in);
      this.jobACLs.put(aclType, acl);
    }
    this.failureInfo = Text.readString(in);
  }

  // A utility to convert new job runstates to the old ones.
  static int getOldNewJobRunState(
    org.apache.hadoop.mapreduce.JobStatus.State state) {
    return state.getValue();
  }
}
