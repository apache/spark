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
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.apache.log4j.Level;

public class TestTaskClasspathPrecedence {
  private static File TEST_DIR = new File(System.getProperty("test.build.data",
      "/tmp"), TestJvmManager.class.getSimpleName());
  private static int MAP_SLOTS = 1;
  private static int REDUCE_SLOTS = 1;
  private TaskTracker tt;
  private JvmManager jvmManager;
  private JobConf ttConf;

  private static class MyTaskRunner extends TaskRunner {
    public MyTaskRunner(TaskInProgress tip, TaskTracker tracker, JobConf conf,
        TaskTracker.RunningJob job) throws IOException {
      super(tip, tracker, conf, job);
    }
    private static String SYSTEM_PATH_SEPARATOR = System.getProperty("path.separator");
    private Vector<String> getVMArgs(TaskAttemptID taskid, File workDir,
        List<String> classPaths, long logSize)
        throws IOException {
      Vector<String> vargs = new Vector<String>(8);
      File jvm =                                  // use same jvm as parent
        new File(new File(System.getProperty("java.home"), "bin"), "java");

      vargs.add(jvm.toString());
      vargs.add("-classpath");
      String classPath = StringUtils.join(SYSTEM_PATH_SEPARATOR, classPaths);
      vargs.add(classPath);
      return vargs;
    }
  }

  @Before
  public void setUp() {
    TEST_DIR.mkdirs();
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

  public TestTaskClasspathPrecedence() throws Exception {
    ttConf = new JobConf();
    FileSystem fs = FileSystem.get(ttConf);
    ttConf.setJar("build/test/testjar.jar");
    Path dfsPath = new Path("build/test/lib/testjob.jar");
    fs.copyFromLocalFile(new Path("build/test/testjar/testjob.jar"), dfsPath);
    tt = new TaskTracker();
    tt.setConf(new JobConf());
    tt.setMaxMapSlots(MAP_SLOTS);
    tt.setMaxReduceSlots(REDUCE_SLOTS);
    jvmManager = new JvmManager(tt);
    tt.setJvmManagerInstance(jvmManager);
  }

  @Test
  public void testWithClasspathPrecedence() throws Throwable {
    ttConf.set(JobContext.MAPREDUCE_TASK_CLASSPATH_PRECEDENCE, "true");
    JobConf taskConf = new JobConf(ttConf);

    TaskTracker.RunningJob rjob = new TaskTracker.RunningJob(new JobID("jt", 1));
    TaskAttemptID attemptID = new TaskAttemptID("test", 0, true, 0, 0);
    Task task = new MapTask(null, attemptID, 0, null, MAP_SLOTS);
    task.setConf(taskConf);
    TaskInProgress tip = tt.new TaskInProgress(task, taskConf);
    MyTaskRunner taskRunner = new MyTaskRunner(tip, tt, taskConf, rjob);
    final File workDir = new File(TEST_DIR, "work");
    workDir.mkdir();
    
    List<String> classPaths = TaskRunner.getClassPaths(taskConf, workDir, null);
    Vector<String> vargs = taskRunner.getVMArgs(task.getTaskID(), workDir, classPaths, 100);

    String classpath = vargs.get(2);
    String[] cp = classpath.split(":");
    assertTrue(cp[0], cp[0].contains("testjob"));
  }
  
  @Test
  public void testWithoutClasspathPrecedence() throws Throwable {
    ttConf.set(JobContext.MAPREDUCE_TASK_CLASSPATH_PRECEDENCE, "false");
    JobConf taskConf = new JobConf(ttConf);

    TaskTracker.RunningJob rjob = new TaskTracker.RunningJob(new JobID("jt", 1));
    TaskAttemptID attemptID = new TaskAttemptID("test", 0, true, 0, 0);
    Task task = new MapTask(null, attemptID, 0, null, MAP_SLOTS);
    task.setConf(taskConf);
    TaskInProgress tip = tt.new TaskInProgress(task, taskConf);
    MyTaskRunner taskRunner = new MyTaskRunner(tip, tt, taskConf, rjob);
    final File workDir = new File(TEST_DIR, "work");
    workDir.mkdir();
    
    List<String> classPaths = TaskRunner.getClassPaths(taskConf, workDir, null);
    Vector<String> vargs = taskRunner.getVMArgs(task.getTaskID(), workDir, classPaths, 100);

    String classpath = vargs.get(2);
    String[] cp = classpath.split(":");
    assertFalse(cp[0], cp[0].contains("testjob"));
  }
}
