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

package org.apache.hadoop.mapreduce.lib.jobcontrol;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

/** 
 *  This class encapsulates a MapReduce job and its dependency. It monitors 
 *  the states of the depending jobs and updates the state of this job.
 *  A job starts in the WAITING state. If it does not have any depending jobs,
 *  or all of the depending jobs are in SUCCESS state, then the job state 
 *  will become READY. If any depending jobs fail, the job will fail too. 
 *  When in READY state, the job can be submitted to Hadoop for execution, with
 *  the state changing into RUNNING state. From RUNNING state, the job 
 *  can get into SUCCESS or FAILED state, depending 
 *  the status of the job execution.
 */
public class ControlledJob {

  // A job will be in one of the following states
  public static enum State {SUCCESS, WAITING, RUNNING, READY, FAILED,
                            DEPENDENT_FAILED}; 
  public static final String CREATE_DIR = "mapreduce.jobcontrol.createdir.ifnotexist";
  private State state;
  private String controlID;     // assigned and used by JobControl class
  private Job job;               // mapreduce job to be executed.
  // some info for human consumption, e.g. the reason why the job failed
  private String message;
  // the jobs the current job depends on
  private List<ControlledJob> dependingJobs;
	
  /** 
   * Construct a job.
   * @param job a mapreduce job to be executed.
   * @param dependingJobs an array of jobs the current job depends on
   */
  public ControlledJob(Job job, List<ControlledJob> dependingJobs) 
      throws IOException {
    this.job = job;
    this.dependingJobs = dependingJobs;
    this.state = State.WAITING;
    this.controlID = "unassigned";
    this.message = "just initialized";
  }
  
  /**
   * Construct a job.
   * 
   * @param conf mapred job configuration representing a job to be executed.
   * @throws IOException
   */
  public ControlledJob(Configuration conf) throws IOException {
    this(new Job(conf), null);
  }
	
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("job name:\t").append(this.job.getJobName()).append("\n");
    sb.append("job id:\t").append(this.controlID).append("\n");
    sb.append("job state:\t").append(this.state).append("\n");
    sb.append("job mapred id:\t").append(this.job.getJobID()).append("\n");
    sb.append("job message:\t").append(this.message).append("\n");
		
    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      sb.append("job has no depending job:\t").append("\n");
    } else {
      sb.append("job has ").append(this.dependingJobs.size()).
         append(" dependeng jobs:\n");
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
    return job.getJobName();
  }
	
  /**
   * Set the job name for  this job.
   * @param jobName the job name
   */
  public void setJobName(String jobName) {
    job.setJobName(jobName);
  }
	
  /**
   * @return the job ID of this job assigned by JobControl
   */
  public String getJobID() {
    return this.controlID;
  }
	
  /**
   * Set the job ID for  this job.
   * @param id the job ID
   */
  public void setJobID(String id) {
    this.controlID = id;
  }
	
  /**
   * @return the mapred ID of this job as assigned by the 
   * mapred framework.
   */
  public JobID getMapredJobID() {
    return this.job.getJobID();
  }
  
  /**
   * @return the mapreduce job 
   */
  public synchronized Job getJob() {
    return this.job;
  }

  /**
   * Set the mapreduce job
   * @param job the mapreduce job for this job.
   */
  public synchronized void setJob(Job job) {
    this.job = job;
  }

  /**
   * @return the state of this job
   */
  public synchronized State getJobState() {
    return this.state;
  }
	
  /**
   * Set the state for this job.
   * @param state the new state for this job.
   */
  protected synchronized void setJobState(State state) {
    this.state = state;
  }
	
  /**
   * @return the message of this job
   */
  public synchronized String getMessage() {
    return this.message;
  }

  /**
   * Set the message for this job.
   * @param message the message for this job.
   */
  public synchronized void setMessage(String message) {
    this.message = message;
  }

  /**
   * @return the depending jobs of this job
   */
  public List<ControlledJob> getDependentJobs() {
    return this.dependingJobs;
  }
  
  /**
   * Add a job to this jobs' dependency list. 
   * Dependent jobs can only be added while a Job 
   * is waiting to run, not during or afterwards.
   * 
   * @param dependingJob Job that this Job depends on.
   * @return <tt>true</tt> if the Job was added.
   */
  public synchronized boolean addDependingJob(ControlledJob dependingJob) {
    if (this.state == State.WAITING) { //only allowed to add jobs when waiting
      if (this.dependingJobs == null) {
        this.dependingJobs = new ArrayList<ControlledJob>();
      }
      return this.dependingJobs.add(dependingJob);
    } else {
      return false;
    }
  }
	
  /**
   * @return true if this job is in a complete state
   */
  public synchronized boolean isCompleted() {
    return this.state == State.FAILED || 
      this.state == State.DEPENDENT_FAILED ||
      this.state == State.SUCCESS;
  }
	
  /**
   * @return true if this job is in READY state
   */
  public synchronized boolean isReady() {
    return this.state == State.READY;
  }

  public void killJob() throws IOException, InterruptedException {
    job.killJob();
  }
  
  /**
   * Check the state of this running job. The state may 
   * remain the same, become SUCCESS or FAILED.
   */
  private void checkRunningState() throws IOException, InterruptedException {
    try {
      if (job.isComplete()) {
        if (job.isSuccessful()) {
          this.state = State.SUCCESS;
        } else {
          this.state = State.FAILED;
          this.message = "Job failed!";
        }
      }
    } catch (IOException ioe) {
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
      try {
        if (job != null) {
          job.killJob();
        }
      } catch (IOException e) {}
    }
  }
	
  /**
   * Check and update the state of this job. The state changes  
   * depending on its current state and the states of the depending jobs.
   */
   synchronized State checkState() throws IOException, InterruptedException {
    if (this.state == State.RUNNING) {
      checkRunningState();
    }
    if (this.state != State.WAITING) {
      return this.state;
    }
    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      this.state = State.READY;
      return this.state;
    }
    ControlledJob pred = null;
    int n = this.dependingJobs.size();
    for (int i = 0; i < n; i++) {
      pred = this.dependingJobs.get(i);
      State s = pred.checkState();
      if (s == State.WAITING || s == State.READY || s == State.RUNNING) {
        break; // a pred is still not completed, continue in WAITING
        // state
      }
      if (s == State.FAILED || s == State.DEPENDENT_FAILED) {
        this.state = State.DEPENDENT_FAILED;
        this.message = "depending job " + i + " with jobID "
          + pred.getJobID() + " failed. " + pred.getMessage();
        break;
      }
      // pred must be in success state
      if (i == n - 1) {
        this.state = State.READY;
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
      Configuration conf = job.getConfiguration();
      if (conf.getBoolean(CREATE_DIR, false)) {
        FileSystem fs = FileSystem.get(conf);
        Path inputPaths[] = FileInputFormat.getInputPaths(job);
        for (int i = 0; i < inputPaths.length; i++) {
          if (!fs.exists(inputPaths[i])) {
            try {
              fs.mkdirs(inputPaths[i]);
            } catch (IOException e) {

            }
          }
        }
      }
      job.submit();
      this.state = State.RUNNING;
    } catch (Exception ioe) {
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
    }
  }
	
}
