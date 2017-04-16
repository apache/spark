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

import java.io.File;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTaskOutputSize {
  private static Path rootDir = new Path(System.getProperty("test.build.data",
      "/tmp"), "test");

  @After
  public void tearDown() throws Exception {
    FileUtil.fullyDelete(new File(rootDir.toString()));
  }

  @Test
  public void testTaskOutputSize() throws Exception {
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1);
    Path inDir = new Path(rootDir, "input");
    Path outDir = new Path(rootDir, "output");
    Job job = MapReduceTestUtil.createJob(mr.createJobConf(), inDir, outDir, 1, 1);
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    for (TaskCompletionEvent tce : job.getTaskCompletionEvents(0)) {
      TaskStatus ts = jt.getTaskStatus(tce.getTaskAttemptId());
      if (tce.isMapTask()) {
        assertTrue(
            "map output size is not found for " + tce.getTaskAttemptId(), ts
                .getOutputSize() > 0);
      } else {
        assertEquals("task output size not expected for "
            + tce.getTaskAttemptId(), -1, ts.getOutputSize());
      }
    }

    // test output sizes for job with no reduces
    job = MapReduceTestUtil.createJob(mr.createJobConf(), inDir, outDir, 1, 0);
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    for (TaskCompletionEvent tce : job.getTaskCompletionEvents(0)) {
      TaskStatus ts = jt.getTaskStatus(tce.getTaskAttemptId());
      assertEquals("task output size not expected for "
          + tce.getTaskAttemptId(), -1, ts.getOutputSize());
    }

    // test output sizes for failed job
    job = MapReduceTestUtil.createFailJob(mr.createJobConf(), outDir, inDir);
    job.waitForCompletion(true);
    assertFalse("Job not failed", job.isSuccessful());
    for (TaskCompletionEvent tce : job.getTaskCompletionEvents(0)) {
      TaskStatus ts = jt.getTaskStatus(tce.getTaskAttemptId());
      assertEquals("task output size not expected for "
          + tce.getTaskAttemptId(), -1, ts.getOutputSize());
    }
  }

}
