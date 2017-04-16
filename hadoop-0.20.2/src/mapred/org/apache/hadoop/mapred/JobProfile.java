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
import java.net.URL;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**************************************************
 * A JobProfile is a MapReduce primitive.  Tracks a job,
 * whether living or dead.
 *
 **************************************************/
public class JobProfile implements Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (JobProfile.class,
       new WritableFactory() {
         public Writable newInstance() { return new JobProfile(); }
       });
  }

  String user;
  final JobID jobid;
  String jobFile;
  String url;
  String name;
  String queueName;
  
  /**
   * Construct an empty {@link JobProfile}.
   */
  public JobProfile() {
    jobid = new JobID();
  }

  /**
   * Construct a {@link JobProfile} the userid, jobid, 
   * job config-file, job-details url and job name. 
   * 
   * @param user userid of the person who submitted the job.
   * @param jobid id of the job.
   * @param jobFile job configuration file. 
   * @param url link to the web-ui for details of the job.
   * @param name user-specified job name.
   */
  public JobProfile(String user, org.apache.hadoop.mapreduce.JobID jobid, 
                    String jobFile, String url,
                    String name) {
    this(user, jobid, jobFile, url, name, JobConf.DEFAULT_QUEUE_NAME);
  }

  /**
   * Construct a {@link JobProfile} the userid, jobid, 
   * job config-file, job-details url and job name. 
   * 
   * @param user userid of the person who submitted the job.
   * @param jobid id of the job.
   * @param jobFile job configuration file. 
   * @param url link to the web-ui for details of the job.
   * @param name user-specified job name.
   * @param queueName name of the queue to which the job is submitted
   */
  public JobProfile(String user, org.apache.hadoop.mapreduce.JobID jobid, 
                    String jobFile, String url,
                    String name, String queueName) {
    this.user = user;
    this.jobid = JobID.downgrade(jobid);
    this.jobFile = jobFile;
    this.url = url;
    this.name = name;
    this.queueName = queueName;
  }
  
  /**
   * @deprecated use JobProfile(String, JobID, String, String, String) instead
   */
  @Deprecated
  public JobProfile(String user, String jobid, String jobFile, String url,
      String name) {
    this(user, JobID.forName(jobid), jobFile, url, name);
  }
  
  /**
   * Get the user id.
   */
  public String getUser() {
    return user;
  }
    
  /**
   * Get the job id.
   */
  public JobID getJobID() {
    return jobid;
  }

  /**
   * @deprecated use getJobID() instead
   */
  @Deprecated
  public String getJobId() {
    return jobid.toString();
  }
  
  /**
   * Get the configuration file for the job.
   */
  public String getJobFile() {
    return jobFile;
  }

  /**
   * Get the link to the web-ui for details of the job.
   */
  public URL getURL() {
    try {
      return new URL(url);
    } catch (IOException ie) {
      return null;
    }
  }

  /**
   * Get the user-specified job name.
   */
  public String getJobName() {
    return name;
  }
  
  /**
   * Get the name of the queue to which the job is submitted.
   * @return name of the queue.
   */
  public String getQueueName() {
    return queueName;
  }
  
  ///////////////////////////////////////
  // Writable
  ///////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    jobid.write(out);
    Text.writeString(out, jobFile);
    Text.writeString(out, url);
    Text.writeString(out, user);
    Text.writeString(out, name);
    Text.writeString(out, queueName);
  }

  public void readFields(DataInput in) throws IOException {
    jobid.readFields(in);
    this.jobFile = Text.readString(in);
    this.url = Text.readString(in);
    this.user = Text.readString(in);
    this.name = Text.readString(in);
    this.queueName = Text.readString(in);
  }
}


