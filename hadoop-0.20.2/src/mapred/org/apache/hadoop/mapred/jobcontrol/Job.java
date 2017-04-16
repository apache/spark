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

package org.apache.hadoop.mapred.jobcontrol;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.StringUtils;

/** This class encapsulates a MapReduce job and its dependency. It monitors 
 *  the states of the depending jobs and updates the state of this job.
 *  A job starts in the WAITING state. If it does not have any depending jobs, or
 *  all of the depending jobs are in SUCCESS state, then the job state will become
 *  READY. If any depending jobs fail, the job will fail too. 
 *  When in READY state, the job can be submitted to Hadoop for execution, with
 *  the state changing into RUNNING state. From RUNNING state, the job can get into 
 *  SUCCESS or FAILED state, depending the status of the job execution.
 *  
 */

public class Job {

  // A job will be in one of the following states
  final public static int SUCCESS = 0;
  final public static int WAITING = 1;
  final public static int RUNNING = 2;
  final public static int READY = 3;
  final public static int FAILED = 4;
  final public static int DEPENDENT_FAILED = 5;
	
	
  private JobConf theJobConf;
  private int state;
  private String jobID; 		// assigned and used by JobControl class
  private JobID mapredJobID; // the job ID assigned by map/reduce
  private String jobName;		// external name, assigned/used by client app
  private String message;		// some info for human consumption, 
  // e.g. the reason why the job failed
  private ArrayList<Job> dependingJobs;	// the jobs the current job depends on
	
  private JobClient jc = null;		// the map reduce job client
	
  /** 
   * Construct a job.
   * @param jobConf a mapred job configuration representing a job to be executed.
   * @param dependingJobs an array of jobs the current job depends on
   */
  public Job(JobConf jobConf, ArrayList<Job> dependingJobs) throws IOException {
    this.theJobConf = jobConf;
    this.dependingJobs = dependingJobs;
    this.state = Job.WAITING;
    this.jobID = "unassigned";
    this.mapredJobID = null; //not yet assigned 
    this.jobName = jobConf.getJobName();
    this.message = "just initialized";
    this.jc = new JobClient(jobConf);
  }
  
  /**
   * Construct a job.
   * 
   * @param jobConf mapred job configuration representing a job to be executed.
   * @throws IOException
   */
  public Job(JobConf jobConf) throws IOException {
    this(jobConf, null);
  }
	
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("job name:\t").append(this.jobName).append("\n");
    sb.append("job id:\t").append(this.jobID).append("\n");
    sb.append("job state:\t").append(this.state).append("\n");
    sb.append("job mapred id:\t").append(this.mapredJobID==null ? "unassigned" 
        : this.mapredJobID).append("\n");
    sb.append("job message:\t").append(this.message).append("\n");
		
    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      sb.append("job has no depending job:\t").append("\n");
    } else {
      sb.append("job has ").append(this.dependingJobs.size()).append(" dependeng jobs:\n");
      for (int i = 0; i < this.dependingJobs.size(); i++) {
        sb.append("\t depending job ").append(i).append(":\t");
        sb.append((this.dependingJobs.get(i)).getJobName()).append("\n");
      }
    }
    return sb.toString();
  }
	
  /**
   * @return the job name of this job
   */
  public String getJobName() {
    return this.jobName;
  }
	
  /**
   * Set the job name for  this job.
   * @param jobName the job name
   */
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }
	
  /**
   * @return the job ID of this job assigned by JobControl
   */
  public String getJobID() {
    return this.jobID;
  }
	
  /**
   * Set the job ID for  this job.
   * @param id the job ID
   */
  public void setJobID(String id) {
    this.jobID = id;
  }
	
  /**
   * @return the mapred ID of this job
   * @deprecated use {@link #getAssignedJobID()} instead
   */
  @Deprecated
  public String getMapredJobID() {
    return this.mapredJobID.toString();
  }
	
  /**
   * Set the mapred ID for this job.
   * @param mapredJobID the mapred job ID for this job.
   * @deprecated use {@link #setAssignedJobID(JobID)} instead
   */
  @Deprecated
  public void setMapredJobID(String mapredJobID) {
    this.mapredJobID = JobID.forName(mapredJobID);
  }
	
  /**
   * @return the mapred ID of this job as assigned by the 
   * mapred framework.
   */
  public JobID getAssignedJobID() {
    return this.mapredJobID;
  }
  
  /**
   * Set the mapred ID for this job as assigned by the 
   * mapred framework.
   * @param mapredJobID the mapred job ID for this job.
   */
  public void setAssignedJobID(JobID mapredJobID) {
    this.mapredJobID = mapredJobID;
  }
  
  /**
   * @return the mapred job conf of this job
   */
  public JobConf getJobConf() {
    return this.theJobConf;
  }
	

  /**
   * Set the mapred job conf for this job.
   * @param jobConf the mapred job conf for this job.
   */
  public void setJobConf(JobConf jobConf) {
    this.theJobConf = jobConf;
  }
	
  /**
   * @return the state of this job
   */
  public synchronized int getState() {
    return this.state;
  }
	
  /**
   * Set the state for this job.
   * @param state the new state for this job.
   */
  protected synchronized void setState(int state) {
    this.state = state;
  }
	
  /**
   * @return the message of this job
   */
  public String getMessage() {
    return this.message;
  }
	
  /**
   * Set the message for this job.
   * @param message the message for this job.
   */
  public void setMessage(String message) {
    this.message = message;
  }
	

  /**
   * @return the job client of this job
   */
  public JobClient getJobClient(){
          return this.jc;
  }

  /**
   * @return the depending jobs of this job
   */
  public ArrayList<Job> getDependingJobs() {
    return this.dependingJobs;
  }
  
  /**
   * Add a job to this jobs' dependency list. Dependent jobs can only be added while a Job 
   * is waiting to run, not during or afterwards.
   * 
   * @param dependingJob Job that this Job depends on.
   * @return <tt>true</tt> if the Job was added.
   */
  public synchronized boolean addDependingJob(Job dependingJob) {
    if (this.state == Job.WAITING) { //only allowed to add jobs when waiting
      if (this.dependingJobs == null) {
        this.dependingJobs = new ArrayList<Job>();
      }
      return this.dependingJobs.add(dependingJob);
    } else {
      return false;
    }
  }
	
  /**
   * @return true if this job is in a complete state
   */
  public boolean isCompleted() {
    return this.state == Job.FAILED || 
      this.state == Job.DEPENDENT_FAILED ||
      this.state == Job.SUCCESS;
  }
	
  /**
   * @return true if this job is in READY state
   */
  public boolean isReady() {
    return this.state == Job.READY;
  }
	
  /**
   * Check the state of this running job. The state may 
   * remain the same, become SUCCESS or FAILED.
   */
  private void checkRunningState() {
    RunningJob running = null;
    try {
      running = jc.getJob(this.mapredJobID);
      if (running.isComplete()) {
        if (running.isSuccessful()) {
          this.state = Job.SUCCESS;
        } else {
          this.state = Job.FAILED;
          this.message = "Job failed! Error - " + running.getFailureInfo();
          try {
            running.killJob();
          } catch (IOException e1) {

          }
          try {
            this.jc.close();
          } catch (IOException e2) {

          }
        }
      }

    } catch (IOException ioe) {
      this.state = Job.FAILED;
      this.message = StringUtils.stringifyException(ioe);
      try {
        if (running != null)
          running.killJob();
      } catch (IOException e1) {

      }
      try {
        this.jc.close();
      } catch (IOException e1) {

      }
    }
  }
	
  /**
   * Check and update the state of this job. The state changes  
   * depending on its current state and the states of the depending jobs.
   */
   synchronized int checkState() {
    if (this.state == Job.RUNNING) {
      checkRunningState();
    }
    if (this.state != Job.WAITING) {
      return this.state;
    }
    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      this.state = Job.READY;
      return this.state;
    }
    Job pred = null;
    int n = this.dependingJobs.size();
    for (int i = 0; i < n; i++) {
      pred = this.dependingJobs.get(i);
      int s = pred.checkState();
      if (s == Job.WAITING || s == Job.READY || s == Job.RUNNING) {
        break; // a pred is still not completed, continue in WAITING
        // state
      }
      if (s == Job.FAILED || s == Job.DEPENDENT_FAILED) {
        this.state = Job.DEPENDENT_FAILED;
        this.message = "depending job " + i + " with jobID "
          + pred.getJobID() + " failed. " + pred.getMessage();
        break;
      }
      // pred must be in success state
      if (i == n - 1) {
        this.state = Job.READY;
      }
    }

    return this.state;
  }
	
  /**
   * Submit this job to mapred. The state becomes RUNNING if submission 
   * is successful, FAILED otherwise.  
   */
  protected synchronized void submit() {
    try {
      if (theJobConf.getBoolean("create.empty.dir.if.nonexist", false)) {
        FileSystem fs = FileSystem.get(theJobConf);
        Path inputPaths[] = FileInputFormat.getInputPaths(theJobConf);
        for (int i = 0; i < inputPaths.length; i++) {
          if (!fs.exists(inputPaths[i])) {
            try {
              fs.mkdirs(inputPaths[i]);
            } catch (IOException e) {

            }
          }
        }
      }
      RunningJob running = jc.submitJob(theJobConf);
      this.mapredJobID = running.getID();
      this.state = Job.RUNNING;
    } catch (IOException ioe) {
      this.state = Job.FAILED;
      this.message = StringUtils.stringifyException(ioe);
    }
  }
	
}
