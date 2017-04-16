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

package org.apache.hadoop.mrunit.mapreduce.mock;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Mock implementation of OutputCommitter that does nothing.
 */
public class MockOutputCommitter extends OutputCommitter {

  public void setupJob(JobContext jobContext) {
  }

  public void cleanupJob(JobContext jobContext) {
  }

  public void setupTask(TaskAttemptContext taskContext) {
  }

  public boolean needsTaskCommit(TaskAttemptContext taskContext) {
    return false;
  }

  public void commitTask(TaskAttemptContext taskContext) {
  }

  public void abortTask(TaskAttemptContext taskContext) {
  }
}

