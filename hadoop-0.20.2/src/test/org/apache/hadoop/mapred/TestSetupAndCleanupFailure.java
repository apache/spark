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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Tests various failures in setup/cleanup of job, like 
 * throwing exception, command line kill and lost tracker 
 */
public class TestSetupAndCleanupFailure extends TestCase {

  final Path inDir = new Path("./input");
  final Path outDir = new Path("./output");
  static Path setupSignalFile = new Path("/setup-signal");
  static Path cleanupSignalFile = new Path("/cleanup-signal");
  String input = "The quick brown fox\nhas many silly\nred fox sox\n";
 
  // Commiter with setupJob throwing exception
  static class CommitterWithFailSetup extends FileOutputCommitter {
    @Override
    public void setupJob(JobContext context) throws IOException {
      throw new IOException();
    }
  }

  // Commiter with commitJob throwing exception
  static class CommitterWithFailCleanup extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      throw new IOException();
    }
  }

  // Committer waits for a file to be created on dfs.
  static class CommitterWithLongSetupAndCleanup extends FileOutputCommitter {
    
    private void waitForSignalFile(FileSystem fs, Path signalFile) 
    throws IOException {
      while (!fs.exists(signalFile)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
         break;
        }
      }
    }
    
    @Override
    public void setupJob(JobContext context) throws IOException {
      waitForSignalFile(FileSystem.get(context.getJobConf()), setupSignalFile);
      super.setupJob(context);
    }
    
    @Override
    public void commitJob(JobContext context) throws IOException {
      waitForSignalFile(FileSystem.get(context.getJobConf()), cleanupSignalFile);
      super.commitJob(context);
    }
  }
  
  public RunningJob launchJob(JobConf conf) 
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
    conf.setMapperClass(IdentityMapper.class);        
    conf.setReducerClass(IdentityReducer.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                    "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);

    // return the RunningJob handle.
    return new JobClient(conf).submitJob(conf);
  }

  // Among these tips only one of the tasks will be running,
  // get the taskid for that task 
  private TaskAttemptID getRunningTaskID(TaskInProgress[] tips) {
    TaskAttemptID taskid = null;
    while (taskid == null) {
      for (TaskInProgress tip :tips) {
        TaskStatus[] statuses = tip.getTaskStatuses();
        for (TaskStatus status : statuses) {
          if (status.getRunState() == TaskStatus.State.RUNNING) {
            taskid = status.getTaskID();
            break;
          }
        }
        if (taskid != null) break;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {}
    }
    return taskid;
  }
  
  // Tests the failures in setup/cleanup job. Job should cleanly fail.
  private void testFailCommitter(Class<? extends OutputCommitter> theClass,
                                 JobConf jobConf) 
  throws IOException {
    jobConf.setOutputCommitter(theClass);
    RunningJob job = launchJob(jobConf);
    // wait for the job to finish.
    job.waitForCompletion();
    assertEquals(JobStatus.FAILED, job.getJobState());
  }
  
  // launch job with CommitterWithLongSetupAndCleanup as committer
  // and wait till the job is inited.
  private RunningJob launchJobWithWaitingSetupAndCleanup(MiniMRCluster mr) 
  throws IOException {
    // launch job with waiting setup/cleanup
    JobConf jobConf = mr.createJobConf();
    jobConf.setOutputCommitter(CommitterWithLongSetupAndCleanup.class);
    RunningJob job = launchJob(jobConf);
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jt.getJob(job.getID());
    while (!jip.inited()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {}
    }
    return job;
  }
  
  /**
   * Tests setup and cleanup attempts getting killed from command-line 
   * and lost tracker
   * 
   * @param mr
   * @param dfs
   * @param commandLineKill if true, test with command-line kill
   *                        else, test with lost tracker
   * @throws IOException
   */
  private void testSetupAndCleanupKill(MiniMRCluster mr, 
                                       MiniDFSCluster dfs, 
                                       boolean commandLineKill) 
  throws IOException {
    // launch job with waiting setup/cleanup
    RunningJob job = launchJobWithWaitingSetupAndCleanup(mr);
    
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jt.getJob(job.getID());
    // get the running setup task id
    TaskAttemptID setupID = 
      getRunningTaskID(jip.getTasks(TaskType.JOB_SETUP));
    if (commandLineKill) {
      killTaskFromCommandLine(job, setupID, jt);
    } else {
      killTaskWithLostTracker(mr, setupID);
    }
    // signal the setup to complete
    UtilsForTests.writeFile(dfs.getNameNode(), 
                            dfs.getFileSystem().getConf(), 
                            setupSignalFile, (short)3);
    // wait for maps and reduces to complete
    while (job.reduceProgress() != 1.0f) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {}
    }
    // get the running cleanup task id
    TaskAttemptID cleanupID = 
      getRunningTaskID(jip.getTasks(TaskType.JOB_CLEANUP));
    if (commandLineKill) {
      killTaskFromCommandLine(job, cleanupID, jt);
    } else {
      killTaskWithLostTracker(mr, cleanupID);
    }
    // signal the cleanup to complete
    UtilsForTests.writeFile(dfs.getNameNode(), 
                            dfs.getFileSystem().getConf(), 
                            cleanupSignalFile, (short)3);
    // wait for the job to finish.
    job.waitForCompletion();
    assertEquals(JobStatus.SUCCEEDED, job.getJobState());
    assertEquals(TaskStatus.State.KILLED, 
                 jt.getTaskStatus(setupID).getRunState());
    assertEquals(TaskStatus.State.KILLED, 
                 jt.getTaskStatus(cleanupID).getRunState());
  }
  
  // kill the task from command-line 
  // wait till it kill is reported back
  private void killTaskFromCommandLine(RunningJob job, 
                                       TaskAttemptID taskid,
                                       JobTracker jt) 
  throws IOException {
    job.killTask(taskid, false);
    // wait till the kill happens
    while (jt.getTaskStatus(taskid).getRunState() != 
           TaskStatus.State.KILLED) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {}
    }

  }
  // kill the task by losing the tracker
  private void killTaskWithLostTracker(MiniMRCluster mr, 
                                       TaskAttemptID taskid) {
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    String trackerName = jt.getTaskStatus(taskid).getTaskTracker();
    int trackerID = mr.getTaskTrackerID(trackerName);
    assertTrue(trackerID != -1);
    mr.stopTaskTracker(trackerID);
  }
  
  // Tests the failures in setup/cleanup job. Job should cleanly fail.
  // Also Tests the command-line kill for setup/cleanup attempts. 
  // tests the setup/cleanup attempts getting killed if 
  // they were running on a lost tracker
  public void testWithDFS() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      JobConf jtConf = new JobConf();
      jtConf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
      jtConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
      jtConf.setLong("mapred.tasktracker.expiry.interval", 10 * 1000);
      jtConf.setInt("mapred.reduce.copy.backoff", 4);
      mr = new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1,
                             null, null, jtConf);
      // test setup/cleanup throwing exceptions
      testFailCommitter(CommitterWithFailSetup.class, mr.createJobConf());
      testFailCommitter(CommitterWithFailCleanup.class, mr.createJobConf());
      // test the command-line kill for setup/cleanup attempts. 
      testSetupAndCleanupKill(mr, dfs, true);
      // remove setup/cleanup signal files.
      fileSys.delete(setupSignalFile , true);
      fileSys.delete(cleanupSignalFile , true);
      // test the setup/cleanup attempts getting killed if 
      // they were running on a lost tracker
      testSetupAndCleanupKill(mr, dfs, false);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }

  public static void main(String[] argv) throws Exception {
    TestSetupAndCleanupFailure td = new TestSetupAndCleanupFailure();
    td.testWithDFS();
  }
}
