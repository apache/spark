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

package org.apache.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import org.junit.Test;

public class TestMRJobClient extends ClusterMapReduceTestCase {

  @Test
  public void testMissingProfileOutput() throws Exception {
    Configuration conf = createJobConf();
    final String input = "hello1\n";

    // Set a job to be profiled with an empty agentlib parameter.
    // This will fail to create profile.out files for tasks.
    // This will succeed by skipping the HTTP fetch of the
    // profiler output.
    Job job = MapReduceTestUtil.createJob(conf,
        getInputDir(), getOutputDir(), 1, 1, input);
    job.setJobName("disable-profile-fetch");
    JobConf jobConf = (JobConf) job.getConfiguration();
    jobConf.setProfileEnabled(true);
    jobConf.setProfileParams("-agentlib:,verbose=n,file=%s");
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    job.waitForCompletion(true);

    // Run another job with an hprof agentlib param; verify
    // that the HTTP fetch works here.
    Job job2 = MapReduceTestUtil.createJob(conf,
        getInputDir(), getOutputDir(), 1, 1, input);
    JobConf jobConf2 = (JobConf) job2.getConfiguration();
    job2.setJobName("enable-profile-fetch");
    jobConf2.setProfileEnabled(true);
    jobConf2.setProfileParams(
        "-agentlib:hprof=cpu=samples,heap=sites,force=n,"
        + "thread=y,verbose=n,file=%s");
    jobConf2.setProfileTaskRange(true, "0-1");
    jobConf2.setProfileTaskRange(false, "");
    jobConf2.setMaxMapAttempts(1);
    jobConf2.setMaxReduceAttempts(1);
    job2.waitForCompletion(true);

    // Find the first map task, verify that we got its profile output file.
    TaskCompletionEvent [] completionEvents = job2.getTaskCompletionEvents(0);
    TaskAttemptID attemptId = null;
    for (TaskCompletionEvent event : completionEvents) {
      if (event.isMapTask()) {
        attemptId = event.getTaskAttemptId();
        break;
      }
    }

    assertNotNull("Could not find a map task attempt", attemptId);
    File profileOutFile = new File(attemptId.toString() + ".profile");
    assertTrue("Couldn't find profiler output", profileOutFile.exists());
    assertTrue("Couldn't remove profiler output", profileOutFile.delete());
  }
}
