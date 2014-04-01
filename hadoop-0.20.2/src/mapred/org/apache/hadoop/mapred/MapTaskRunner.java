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

import java.io.*;

import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;

/** Runs a map task. */
class MapTaskRunner extends TaskRunner {
  
  public MapTaskRunner(TaskInProgress task, TaskTracker tracker, JobConf conf,
                       TaskTracker.RunningJob rjob) 
  throws IOException {
    super(task, tracker, conf, rjob);
  }
  
  /** Delete any temporary files from previous failed attempts. */
  public boolean prepare() throws IOException {
    if (!super.prepare()) {
      return false;
    }
    
    mapOutputFile.removeAll();
    return true;
  }

  /** Delete all of the temporary map output files. */
  public void close() throws IOException {
    LOG.info(getTask()+" done; removing files.");
    mapOutputFile.removeAll();
  }

  @Override
  public String getChildJavaOpts(JobConf jobConf, String defaultValue) {
    return jobConf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, 
                       super.getChildJavaOpts(jobConf, 
                           JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS));
  }

  @Override
  public int getChildUlimit(JobConf jobConf) {
    return jobConf.getInt(JobConf.MAPRED_MAP_TASK_ULIMIT, 
                          super.getChildUlimit(jobConf));
  }

  @Override
  public String getChildEnv(JobConf jobConf) {
    return jobConf.get(JobConf.MAPRED_MAP_TASK_ENV, super.getChildEnv(jobConf));
  }

}
