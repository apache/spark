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

import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestJvmReuse extends TestCase {
  private static Path rootDir = new Path(System.getProperty("test.build.data",
      "/tmp"), TestJvmReuse.class.getName());
  private int numMappers = 5;
  private static int taskWithCleanup = 2; // third task

  /**
   * A mapper class in which all attempts log taskid. Zeroth attempt of task
   * with id=taskWithCleanup, fails with System.exit to force a cleanup attempt
   * for the task in a new jvm.
   */
  public static class MapperClass extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {
    String taskid;
    static int instances = 0;
    Reporter reporter = null;

    public void configure(JobConf job) {
      taskid = job.get("mapred.task.id");
    }

    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      System.err.println(taskid);
      this.reporter = reporter;

      if (TaskAttemptID.forName(taskid).getTaskID().getId() == taskWithCleanup) {
        if (taskid.endsWith("_0")) {
          System.exit(-1);
        }
      }
    }
    
    public void close() throws IOException {
      reporter.incrCounter("jvm", "use", ++instances);
    }
  }

  public RunningJob launchJob(JobConf conf, Path inDir, Path outDir)
      throws IOException {
    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    for (int i = 0; i < numMappers; i++) {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-" + i));
      file.writeBytes("input");
      file.close();
    }

    // configure the mapred Job
    conf.setMapperClass(MapperClass.class);
    conf.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    // enable jvm reuse
    conf.setNumTasksToExecutePerJvm(-1);
    // return the RunningJob handle.
    return new JobClient(conf).submitJob(conf);
  }

  private void validateAttempt(TaskInProgress tip, TaskAttemptID attemptId,
      TaskStatus ts, boolean isCleanup) throws IOException {
    assertEquals(isCleanup, tip.isCleanupAttempt(attemptId));
    // validate tasklogs for task attempt
    String log = TestMiniMRMapRedDebugScript.readTaskLog(
        TaskLog.LogName.STDERR, attemptId, false);
    assertTrue(log.equals(attemptId.toString()));
    assertTrue(ts != null);
    if (!isCleanup) {
      assertEquals(TaskStatus.State.SUCCEEDED, ts.getRunState());
    } else {
      assertEquals(TaskStatus.State.FAILED, ts.getRunState());
      // validate tasklogs for cleanup attempt
      log = TestMiniMRMapRedDebugScript.readTaskLog(TaskLog.LogName.STDERR,
          attemptId, true);
      assertTrue(log.equals(TestTaskFail.cleanupLog));
    }
  }

  // validates logs of all attempts of the job.
  private void validateJob(RunningJob job, MiniMRCluster mr) throws IOException {
    assertEquals(JobStatus.SUCCEEDED, job.getJobState());
    long uses = job.getCounters().findCounter("jvm", "use").getValue();
    System.out.println("maps:" + numMappers + " uses:" + uses);
    assertTrue("maps = " + numMappers + ", jvms = " + uses, numMappers < uses);

    JobID jobId = job.getID();

    for (int i = 0; i < numMappers; i++) {
      TaskAttemptID attemptId = new TaskAttemptID(new TaskID(jobId, true, i), 0);
      TaskInProgress tip = mr.getJobTrackerRunner().getJobTracker().getTip(
          attemptId.getTaskID());
      TaskStatus ts = mr.getJobTrackerRunner().getJobTracker().getTaskStatus(
          attemptId);
      validateAttempt(tip, attemptId, ts, i == taskWithCleanup);
      if (i == taskWithCleanup) {
        // validate second attempt of the task
        attemptId = new TaskAttemptID(new TaskID(jobId, true, i), 1);
        ts = mr.getJobTrackerRunner().getJobTracker().getTaskStatus(attemptId);
        validateAttempt(tip, attemptId, ts, false);
      }
    }
  }

  public void testTaskLogs() throws IOException {
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      final int taskTrackers = 1; // taskTrackers should be 1 to test jvm reuse.
      conf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
      mr = new MiniMRCluster(taskTrackers, "file:///", 1);

      final Path inDir = new Path(rootDir, "input");
      final Path outDir = new Path(rootDir, "output");
      JobConf jobConf = mr.createJobConf();
      jobConf.setOutputCommitter(TestTaskFail.CommitterWithLogs.class);
      RunningJob rJob = launchJob(jobConf, inDir, outDir);
      rJob.waitForCompletion();
      validateJob(rJob, mr);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}
