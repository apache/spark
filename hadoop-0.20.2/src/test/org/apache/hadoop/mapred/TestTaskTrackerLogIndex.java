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
import java.net.HttpURLConnection;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

public class TestTaskTrackerLogIndex extends TestCase {
  private static String taskLog = "Task attempt log";
  private static String INDEX_FILENAME_DIRS
    = "test.tracker.log.index.directory";
  static String cleanupLog = "cleanup attempt log";

  private static Pattern ATTEMPT_ID_PATTERN
    = Pattern.compile(".+_([0-9a-z]+)_([0-9]+)_([a-z]+)_([0-9]+)_([0-9]+)$");

  public static class MapperClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
    String taskid;

    public void configure(JobConf job) {
      taskid = job.get("mapred.task.id");
      String indexDir = job.get(INDEX_FILENAME_DIRS);
      Path indexDirPath = new Path(indexDir);
      Path thisIndexPath = new Path(indexDirPath, taskid);
      try {
        FileSystem fs = indexDirPath.getFileSystem(job);
        fs.create(thisIndexPath).close();
      } catch (IOException e) {
        throw new RuntimeException
          ("TestTaskTrackerLogIndex.MapperClass.configure(...)", e);
      }
    }

    public void map (LongWritable key, Text value, 
                     OutputCollector<Text, IntWritable> output, 
                     Reporter reporter) throws IOException {
      System.err.println(taskLog);
      assertFalse(Boolean.getBoolean(System
          .getProperty("hadoop.tasklog.iscleanup")));
      if (taskid.endsWith("_0")) {
        throw new IOException();
      } else if (taskid.endsWith("_1")) {
        System.exit(-1);
      } else if (taskid.endsWith("_2")) {
        throw new Error();
      }
    }
  }

  static class CommitterWithLogs extends FileOutputCommitter {
    public void abortTask(TaskAttemptContext context) throws IOException {
      System.err.println(cleanupLog);
      String attemptId = System.getProperty("hadoop.tasklog.taskid");
      assertNotNull(attemptId);
      if (attemptId.endsWith("_0")) {
        assertFalse(Boolean.getBoolean(System
            .getProperty("hadoop.tasklog.iscleanup")));
      } else {
        assertTrue(Boolean.getBoolean(System
            .getProperty("hadoop.tasklog.iscleanup")));
      }
      super.abortTask(context);
    }
  }

  static class CommitterWithFailTaskCleanup extends FileOutputCommitter {
    public void abortTask(TaskAttemptContext context) throws IOException {
      System.err.println(cleanupLog);
      System.exit(-1);
    }
  }

  static class CommitterWithFailTaskCleanup2 extends FileOutputCommitter {
    public void abortTask(TaskAttemptContext context) throws IOException {
      System.err.println(cleanupLog);
      throw new IOException();
    }
  }

  public RunningJob launchJob(JobConf conf,
                              Path inDir,
                              Path outDir,
                              Path indexDir,
                              String input) 
  throws IOException {
    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    // configure the mapred Job
    conf.setMapperClass(MapperClass.class);        
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.set(INDEX_FILENAME_DIRS, indexDir.toString());
    conf.setSpeculativeExecution(false);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                    "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);
    // return the RunningJob handle.
    return new JobClient(conf).submitJob(conf);
  }
  
  private void validateAttempt(TaskInProgress tip, TaskAttemptID attemptId, 
		  TaskStatus ts, boolean isCleanup, JobTracker jt) 
  throws IOException {
    assertEquals(isCleanup, tip.isCleanupAttempt(attemptId));
    assertTrue(ts != null);
    assertEquals(TaskStatus.State.FAILED, ts.getRunState());
    // validate tasklogs for task attempt
    String log = TestMiniMRMapRedDebugScript.readTaskLog(
    TaskLog.LogName.STDERR, attemptId, false);
    assertTrue(log.contains(taskLog));
    // access the logs from web url
    TaskTrackerStatus ttStatus = jt.getTaskTracker(
        tip.machineWhereTaskRan(attemptId)).getStatus();
    String tasklogUrl = TaskLogServlet.getTaskLogUrl("localhost",
        String.valueOf(ttStatus.getHttpPort()), attemptId.toString());
    assertEquals(HttpURLConnection.HTTP_OK, TestWebUIAuthorization
        .getHttpStatusCode(tasklogUrl, tip.getUser(), "GET"));
    if (!isCleanup) {
      // validate task logs: tasklog should contain both task logs
      // and cleanup logs
      assertTrue(log.contains(cleanupLog));
    } else {
      // validate tasklogs for cleanup attempt
      log = TestMiniMRMapRedDebugScript.readTaskLog(
      TaskLog.LogName.STDERR, attemptId, true);
      assertTrue(log.contains(cleanupLog));
      // access the cleanup attempt's logs from web url
      ttStatus = jt.getTaskTracker(tip.machineWhereCleanupRan(attemptId))
          .getStatus();
      String cleanupTasklogUrl = TaskLogServlet.getTaskLogUrl(
          "localhost", String.valueOf(ttStatus.getHttpPort()), attemptId
              .toString()) + "&cleanup=true";
      assertEquals(HttpURLConnection.HTTP_OK, TestWebUIAuthorization
          .getHttpStatusCode(cleanupTasklogUrl, tip.getUser(), "GET"));
    }
  }

  private void validateJob(RunningJob job, JobTracker jt) 
  throws IOException {
    assertEquals(JobStatus.SUCCEEDED, job.getJobState());
	    
    JobID jobId = job.getID();
    // construct the task id of first map task
    // this should not be cleanup attempt since the first attempt 
    // fails with an exception
    TaskAttemptID attemptId = 
      new TaskAttemptID(new TaskID(jobId, true, 0), 0);
    TaskInProgress tip = jt.getTip(attemptId.getTaskID());
    TaskStatus ts = jt.getTaskStatus(attemptId);
    validateAttempt(tip, attemptId, ts, false, jt);
    
    attemptId =  new TaskAttemptID(new TaskID(jobId, true, 0), 1);
    // this should be cleanup attempt since the second attempt fails
    // with System.exit
    ts = jt.getTaskStatus(attemptId);
    validateAttempt(tip, attemptId, ts, true, jt);
    
    attemptId =  new TaskAttemptID(new TaskID(jobId, true, 0), 2);
    // this should be cleanup attempt since the third attempt fails
    // with Error
    ts = jt.getTaskStatus(attemptId);
    validateAttempt(tip, attemptId, ts, true, jt);
  }
  
  public void testWithDFS() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;

      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1);
      JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
      final Path inDir = new Path("./input");
      final Path outDir = new Path("./output");
      final Path indexDir = new Path(fileSys.getHomeDirectory(), "index-names");
      String input = "The quick brown fox\nhas many silly\nred fox sox\n";
      // launch job with fail tasks
      fileSys.delete(indexDir, true);
      fileSys.delete(indexDir, true);
      JobConf jobConf = mr.createJobConf();
      jobConf.setOutputCommitter(CommitterWithLogs.class);
      jobConf.setNumTasksToExecutePerJvm(1);
      RunningJob rJob = launchJob(jobConf, inDir, outDir, indexDir, input);
      rJob.waitForCompletion();
      validateJob(rJob, jt);
      // launch job with fail tasks and fail-cleanups
      fileSys.delete(outDir, true);
      jobConf.setOutputCommitter(CommitterWithFailTaskCleanup.class);
      jobConf.setNumTasksToExecutePerJvm(2);
      rJob = launchJob(jobConf, inDir, outDir, indexDir, input);
      rJob.waitForCompletion();
      validateJob(rJob, jt);
      fileSys.delete(outDir, true);
      jobConf.setOutputCommitter(CommitterWithFailTaskCleanup2.class);
      jobConf.setNumTasksToExecutePerJvm(-1);
      rJob = launchJob(jobConf, inDir, outDir, indexDir, input);
      rJob.waitForCompletion();
      validateJob(rJob, jt);

      // now find the indices
      FileStatus[] statuses = fileSys.listStatus(indexDir);

      for (int i = 0; i < statuses.length; ++i) {
        String pathName = statuses[i].getPath().toString();

        Matcher matcher = ATTEMPT_ID_PATTERN.matcher(pathName);

        assertTrue
          ("Syntactically invalid file name, " + pathName, matcher.matches());

        int jobIndex = Integer.parseInt(matcher.group(2));

        TaskAttemptID attemptId
          = new TaskAttemptID(matcher.group(1),
                              jobIndex,
                              true,
                              Integer.parseInt(matcher.group(4)),
                              Integer.parseInt(matcher.group(5)));

        File indexFile = TaskLog.getIndexFile(attemptId, false);

        BufferedReader fis = null;

        try {
          fis = new BufferedReader(new FileReader(indexFile));

          String str = fis.readLine();
          int count = 0;

          while (str != null && count < 10) {
            if (str.contains(TaskLog.LogName.STDERR.toString())) {
              str = str.substring(TaskLog.LogName.STDERR.toString().length() + 1);
              String[] startAndLen = str.split(" ");

              long declaredLength = Long.parseLong(startAndLen[1]);

              assertTrue("The first job has a non-default length",
                         !(jobIndex == 1) || declaredLength == -1L);
              assertTrue("A subsequent job has a default length",
                         (jobIndex == 1) || declaredLength > -1L);
            }

            str = fis.readLine();
          }
        } finally {
          if (fis != null) {
            fis.close();
          }
        }
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  public static void main(String[] argv) throws Exception {
    TestTaskTrackerLogIndex td = new TestTaskTrackerLogIndex();
    td.testWithDFS();
  }
}
