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

package org.apache.hadoop.eclipse.server;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

/**
 * Representation of a Map/Reduce running job on a given location
 */

public class HadoopJob {

  /**
   * Enum representation of a Job state
   */
  public enum JobState {
    PREPARE(JobStatus.PREP), RUNNING(JobStatus.RUNNING), FAILED(
        JobStatus.FAILED), SUCCEEDED(JobStatus.SUCCEEDED);

    final int state;

    JobState(int state) {
      this.state = state;
    }

    static JobState ofInt(int state) {
      switch (state) {
        case JobStatus.PREP:
          return PREPARE;
        case JobStatus.RUNNING:
          return RUNNING;
        case JobStatus.FAILED:
          return FAILED;
        case JobStatus.SUCCEEDED:
          return SUCCEEDED;
        default:
          return null;
      }
    }
  }

  /**
   * Location this Job runs on
   */
  private final HadoopServer location;

  /**
   * Unique identifier of this Job
   */
  final JobID jobId;

  /**
   * Status representation of a running job. This actually contains a
   * reference to a JobClient. Its methods might block.
   */
  RunningJob running;

  /**
   * Last polled status
   * 
   * @deprecated should apparently not be used
   */
  JobStatus status;

  /**
   * Last polled counters
   */
  Counters counters;

  /**
   * Job Configuration
   */
  JobConf jobConf = null;

  boolean completed = false;

  boolean successful = false;

  boolean killed = false;

  int totalMaps;

  int totalReduces;

  int completedMaps;

  int completedReduces;

  float mapProgress;

  float reduceProgress;

  /**
   * Constructor for a Hadoop job representation
   * 
   * @param location
   * @param id
   * @param running
   * @param status
   */
  public HadoopJob(HadoopServer location, JobID id, RunningJob running,
      JobStatus status) {

    this.location = location;
    this.jobId = id;
    this.running = running;

    loadJobFile();

    update(status);
  }

  /**
   * Try to locate and load the JobConf file for this job so to get more
   * details on the job (number of maps and of reduces)
   */
  private void loadJobFile() {
    try {
      String jobFile = getJobFile();
      FileSystem fs = location.getDFS();
      File tmp = File.createTempFile(getJobID().toString(), ".xml");
      if (FileUtil.copy(fs, new Path(jobFile), tmp, false, location
          .getConfiguration())) {
        this.jobConf = new JobConf(tmp.toString());

        this.totalMaps = jobConf.getNumMapTasks();
        this.totalReduces = jobConf.getNumReduceTasks();
      }

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  /* @inheritDoc */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
    result = prime * result + ((location == null) ? 0 : location.hashCode());
    return result;
  }

  /* @inheritDoc */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof HadoopJob))
      return false;
    final HadoopJob other = (HadoopJob) obj;
    if (jobId == null) {
      if (other.jobId != null)
        return false;
    } else if (!jobId.equals(other.jobId))
      return false;
    if (location == null) {
      if (other.location != null)
        return false;
    } else if (!location.equals(other.location))
      return false;
    return true;
  }

  /**
   * Get the running status of the Job (@see {@link JobStatus}).
   * 
   * @return
   */
  public JobState getState() {
    if (this.completed) {
      if (this.successful) {
        return JobState.SUCCEEDED;
      } else {
        return JobState.FAILED;
      }
    } else {
      return JobState.RUNNING;
    }
    // return JobState.ofInt(this.status.getRunState());
  }

  /**
   * @return
   */
  public JobID getJobID() {
    return this.jobId;
  }

  /**
   * @return
   */
  public HadoopServer getLocation() {
    return this.location;
  }

  /**
   * @return
   */
  public boolean isCompleted() {
    return this.completed;
  }

  /**
   * @return
   */
  public String getJobName() {
    return this.running.getJobName();
  }

  /**
   * @return
   */
  public String getJobFile() {
    return this.running.getJobFile();
  }

  /**
   * Return the tracking URL for this Job.
   * 
   * @return string representation of the tracking URL for this Job
   */
  public String getTrackingURL() {
    return this.running.getTrackingURL();
  }

  /**
   * Returns a string representation of this job status
   * 
   * @return string representation of this job status
   */
  public String getStatus() {

    StringBuffer s = new StringBuffer();

    s.append("Maps : " + completedMaps + "/" + totalMaps);
    s.append(" (" + mapProgress + ")");
    s.append("  Reduces : " + completedReduces + "/" + totalReduces);
    s.append(" (" + reduceProgress + ")");

    return s.toString();
  }

  /**
   * Update this job status according to the given JobStatus
   * 
   * @param status
   */
  void update(JobStatus status) {
    this.status = status;
    try {
      this.counters = running.getCounters();
      this.completed = running.isComplete();
      this.successful = running.isSuccessful();
      this.mapProgress = running.mapProgress();
      this.reduceProgress = running.reduceProgress();
      // running.getTaskCompletionEvents(fromEvent);

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    this.completedMaps = (int) (this.totalMaps * this.mapProgress);
    this.completedReduces = (int) (this.totalReduces * this.reduceProgress);
  }

  /**
   * Print this job counters (for debugging purpose)
   */
  void printCounters() {
    System.out.printf("New Job:\n", counters);
    for (String groupName : counters.getGroupNames()) {
      Counters.Group group = counters.getGroup(groupName);
      System.out.printf("\t%s[%s]\n", groupName, group.getDisplayName());

      for (Counters.Counter counter : group) {
        System.out.printf("\t\t%s: %s\n", counter.getDisplayName(),
                                          counter.getCounter());
      }
    }
    System.out.printf("\n");
  }

  /**
   * Kill this job
   */
  public void kill() {
    try {
      this.running.killJob();
      this.killed = true;

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Print this job status (for debugging purpose)
   */
  public void display() {
    System.out.printf("Job id=%s, name=%s\n", getJobID(), getJobName());
    System.out.printf("Configuration file: %s\n", getJobID());
    System.out.printf("Tracking URL: %s\n", getTrackingURL());

    System.out.printf("Completion: map: %f reduce %f\n",
        100.0 * this.mapProgress, 100.0 * this.reduceProgress);

    System.out.println("Job total maps = " + totalMaps);
    System.out.println("Job completed maps = " + completedMaps);
    System.out.println("Map percentage complete = " + mapProgress);
    System.out.println("Job total reduces = " + totalReduces);
    System.out.println("Job completed reduces = " + completedReduces);
    System.out.println("Reduce percentage complete = " + reduceProgress);
    System.out.flush();
  }

}
