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

package org.apache.hadoop.mapreduce.security.token;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * The token identifier for job token
 */
public class JobTokenIdentifier extends TokenIdentifier {
  private Text jobid;
  final static Text KIND_NAME = new Text("mapreduce.job");
  
  /**
   * Default constructor
   */
  public JobTokenIdentifier() {
    this.jobid = new Text();
  }

  /**
   * Create a job token identifier from a jobid
   * @param jobid the jobid to use
   */
  public JobTokenIdentifier(Text jobid) {
    this.jobid = jobid;
  }

  /** {@inheritDoc} */
  @Override
  public Text getKind() {
    return KIND_NAME;
  }
  
  /** {@inheritDoc} */
  @Override
  public UserGroupInformation getUser() {
    if (jobid == null || "".equals(jobid.toString())) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(jobid.toString());
  }

  /**
   * Get the jobid
   * @return the jobid
   */
  public Text getJobId() {
    return jobid;
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    jobid.readFields(in);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    jobid.write(out);
  }
}
