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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents the basic information that is saved per a job when the 
 * JobTracker receives a submitJob request. The information is saved
 * so that the JobTracker can recover incomplete jobs upon restart.
 */
class JobInfo implements Writable {
  private org.apache.hadoop.mapreduce.JobID id;
  private Text user;
  private Path jobSubmitDir;
  public JobInfo() {}
  
  public JobInfo(org.apache.hadoop.mapreduce.JobID id, 
      Text user,
      Path jobSubmitDir) {
    this.id = id;
    this.user = user;
    this.jobSubmitDir = jobSubmitDir;
  }
  
  /**
   * Get the job id.
   */
  public org.apache.hadoop.mapreduce.JobID getJobID() {
    return id;
  }
  
  /**
   * Get the configured job's user-name.
   */
  public Text getUser() {
    return user;
  }
      
  /**
   * Get the job submission directory
   */
  public Path getJobSubmitDir() {
    return this.jobSubmitDir;
  }
  
  public void readFields(DataInput in) throws IOException {
    id = new org.apache.hadoop.mapreduce.JobID();
    id.readFields(in);
    user = new Text();
    user.readFields(in);
    jobSubmitDir = new Path(WritableUtils.readString(in));
  }

  public void write(DataOutput out) throws IOException {
    id.write(out);
    user.write(out);
    WritableUtils.writeString(out, jobSubmitDir.toString());
  }
}