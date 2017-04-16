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

import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

/**
 * Test whether the JobInProgressListeners are informed as expected.
 */
public class TestJobInProgressListener extends TestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestJobInProgressListener.class);
  private final Path testDir = new Path("test-jip-listener-update");
  
  private static String TEST_ROOT_DIR = new File(System.getProperty(
          "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');

  private JobConf configureJob(JobConf conf, int m, int r, 
                               Path inDir, Path outputDir,
                               String mapSignalFile, String redSignalFile) 
  throws IOException {
    UtilsForTests.configureWaitingJobConf(conf, inDir, outputDir,  m, r, 
        "job-listener-test", mapSignalFile, redSignalFile);
    return conf; 
  }
  
  /**
   * This test case tests if external updates to JIP do not result into 
   * undesirable effects
   * Test is as follows
   *   - submit 2 jobs of normal priority. job1 is a waiting job which waits and
   *     blocks the cluster
   *   - change one parameter of job2 such that the job bumps up in the queue
   *   - check if the queue looks ok
   *   
   */
  public void testJobQueueChanges() throws IOException {
    LOG.info("Testing job queue changes");
    JobConf conf = new JobConf();
    MiniDFSCluster dfs = new MiniDFSCluster(conf, 1, true, null, null);
    dfs.waitActive();
    FileSystem fileSys = dfs.getFileSystem();
    
    dfs.startDataNodes(conf, 1, true, null, null, null, null);
    dfs.waitActive();
    
    String namenode = (dfs.getFileSystem()).getUri().getHost() + ":" 
                      + (dfs.getFileSystem()).getUri().getPort();
    MiniMRCluster mr = new MiniMRCluster(1, namenode, 1);
    JobClient jobClient = new JobClient(mr.createJobConf());
    
    // clean up
    fileSys.delete(testDir, true);
    
    if (!fileSys.mkdirs(testDir)) {
      throw new IOException("Mkdirs failed to create " + testDir.toString());
    }

    // Write the input file
    Path inDir = new Path(testDir, "input");
    Path shareDir = new Path(testDir, "share");
    String mapSignalFile = UtilsForTests.getMapSignalFile(shareDir);
    String redSignalFile = UtilsForTests.getReduceSignalFile(shareDir);
    UtilsForTests.writeFile(dfs.getNameNode(), conf, new Path(inDir + "/file"), 
                            (short)1);
    
    JobQueueJobInProgressListener myListener = 
      new JobQueueJobInProgressListener();
    
    // add the listener
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);
    
    // big blocking job
    Path outputDir = new Path(testDir, "output");
    Path newOutputDir = outputDir.suffix("0");
    JobConf job1 = configureJob(mr.createJobConf(), 10, 0, inDir, newOutputDir,
                                mapSignalFile, redSignalFile);
    
    // short blocked job
    newOutputDir = outputDir.suffix("1");
    JobConf job2 = configureJob(mr.createJobConf(), 1, 0, inDir, newOutputDir,
                                mapSignalFile, redSignalFile);
    
    RunningJob rJob1 = jobClient.submitJob(job1);
    LOG.info("Running job " + rJob1.getID().toString());
    
    RunningJob rJob2 = jobClient.submitJob(job2);
    LOG.info("Running job " + rJob2.getID().toString());
    
    // I. Check job-priority change
    LOG.info("Testing job priority changes");
    
    // bump up job2's priority
    LOG.info("Increasing job2's priority to HIGH");
    rJob2.setJobPriority("HIGH");
    
    // check if the queue is sane
    assertTrue("Priority change garbles the queue", 
               myListener.getJobQueue().size() == 2);
    
    JobInProgress[] queue = 
      myListener.getJobQueue().toArray(new JobInProgress[0]);
    
    // check if the bump has happened
    assertTrue("Priority change failed to bump up job2 in the queue", 
               queue[0].getJobID().equals(rJob2.getID()));
    
    assertTrue("Priority change failed to bump down job1 in the queue", 
               queue[1].getJobID().equals(rJob1.getID()));
    
    assertEquals("Priority change has garbled the queue", 
                 2, queue.length);
    
    // II. Check start-time change
    LOG.info("Testing job start-time changes");
    
    // reset the priority which will make the order as
    //  - job1
    //  - job2
    // this will help in bumping job2 on start-time change
    LOG.info("Increasing job2's priority to NORMAL"); 
    rJob2.setJobPriority("NORMAL");
    
    // create the change event
    JobInProgress jip2 = mr.getJobTrackerRunner().getJobTracker()
                          .getJob(rJob2.getID());
    JobInProgress jip1 = mr.getJobTrackerRunner().getJobTracker()
                           .getJob(rJob1.getID());
    
    JobStatus prevStatus = (JobStatus)jip2.getStatus().clone();
    
    // change job2's start-time and the status
    jip2.startTime =  jip1.startTime - 1;
    jip2.status.setStartTime(jip2.startTime);
    
    
    JobStatus newStatus = (JobStatus)jip2.getStatus().clone();
    
    // inform the listener
    LOG.info("Updating the listener about job2's start-time change");
    JobStatusChangeEvent event = 
      new JobStatusChangeEvent(jip2, EventType.START_TIME_CHANGED, 
                              prevStatus, newStatus);
    myListener.jobUpdated(event);
    
    // check if the queue is sane
    assertTrue("Start time change garbles the queue", 
               myListener.getJobQueue().size() == 2);
    
    queue = myListener.getJobQueue().toArray(new JobInProgress[0]);
    
    // check if the bump has happened
    assertTrue("Start time change failed to bump up job2 in the queue", 
               queue[0].getJobID().equals(rJob2.getID()));
    
    assertTrue("Start time change failed to bump down job1 in the queue", 
               queue[1].getJobID().equals(rJob1.getID()));
    
    assertEquals("Start time change has garbled the queue", 
                 2, queue.length);
    
    // signal the maps to complete
    UtilsForTests.signalTasks(dfs, fileSys, true, mapSignalFile, redSignalFile);
    
    // check if job completion leaves the queue sane
    while (rJob2.getJobState() != JobStatus.SUCCEEDED) {
      UtilsForTests.waitFor(10);
    }
    
    while (rJob1.getJobState() != JobStatus.SUCCEEDED) {
      UtilsForTests.waitFor(10);
    }
    
    assertTrue("Job completion garbles the queue", 
               myListener.getJobQueue().size() == 0);
  }
  
  // A listener that inits the tasks one at a time and also listens to the 
  // events
  public static class MyListener extends JobInProgressListener {
    private List<JobInProgress> wjobs = new ArrayList<JobInProgress>();
    private List<JobInProgress> jobs = new ArrayList<JobInProgress>(); 
    
    public boolean contains (JobID id) {
      return contains(id, true) || contains(id, false);
    }
    
    public boolean contains (JobID id, boolean waiting) {
      List<JobInProgress> queue = waiting ? wjobs : jobs;
      for (JobInProgress job : queue) {
        if (job.getJobID().equals(id)) {
          return true;
        }
      }
      return false;
    }
    
    public void jobAdded(JobInProgress job) {
      LOG.info("Job " + job.getJobID().toString() + " added");
      wjobs.add(job);
    }
    
    public void jobRemoved(JobInProgress job) {
      LOG.info("Job " + job.getJobID().toString() + " removed");
    }
    
    public void jobUpdated(JobChangeEvent event) {
      LOG.info("Job " + event.getJobInProgress().getJobID().toString() + " updated");
      // remove the job is the event is for a completed job
      if (event instanceof JobStatusChangeEvent) {
        JobStatusChangeEvent statusEvent = (JobStatusChangeEvent)event;
        if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
          // check if the state changes from 
          // RUNNING->COMPLETE(SUCCESS/KILLED/FAILED)
          JobInProgress jip = event.getJobInProgress();
          String jobId = jip.getJobID().toString();
          if (jip.isComplete()) {
            LOG.info("Job " +  jobId + " deleted from the running queue");
            if (statusEvent.getOldStatus().getRunState() == JobStatus.PREP) {
              wjobs.remove(jip);
            } else {
              jobs.remove(jip);
            }
          } else {
            // PREP->RUNNING
            LOG.info("Job " +  jobId + " deleted from the waiting queue");
            wjobs.remove(jip);
            jobs.add(jip);
          }
        }
      }
    }
  }
  
  public void testJobFailure() throws Exception {
    LOG.info("Testing job-success");
    
    MyListener myListener = new MyListener();
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1);
    
    JobConf job = mr.createJobConf();
    
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);

    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerfailjob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistenerfailjob/output");

    // submit a job that fails 
    RunningJob rJob = UtilsForTests.runJobFail(job, inDir, outDir);
    JobID id = rJob.getID();

    // check if the job failure was notified
    assertFalse("Missing event notification on failing a running job", 
                myListener.contains(id));
    
  }
  
  public void testJobKill() throws Exception {
    LOG.info("Testing job-kill");
    
    MyListener myListener = new MyListener();
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1);
    
    JobConf job = mr.createJobConf();
    
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);
    
    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerkilljob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistenerkilljob/output");

    // submit and kill the job   
    RunningJob rJob = UtilsForTests.runJobKill(job, inDir, outDir);
    JobID id = rJob.getID();

    // check if the job failure was notified
    assertFalse("Missing event notification on killing a running job", 
                myListener.contains(id));
    
  }
  
  public void testJobSuccess() throws Exception {
    LOG.info("Testing job-success");
    MyListener myListener = new MyListener();
    
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1);
    
    JobConf job = mr.createJobConf();
    
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);
    
    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/output");

    // submit the job   
    RunningJob rJob = UtilsForTests.runJob(job, inDir, outDir);
    
    // wait for the job to be running
    while (rJob.getJobState() != JobStatus.RUNNING) {
      UtilsForTests.waitFor(10);
    }
    
    LOG.info("Job " +  rJob.getID().toString() + " started running");
    
    // check if the listener was updated about this change
    assertFalse("Missing event notification for a running job", 
                myListener.contains(rJob.getID(), true));
    
    while (rJob.getJobState() != JobStatus.SUCCEEDED) {
      UtilsForTests.waitFor(10);
    }
    
    // check if the job success was notified
    assertFalse("Missing event notification for a successful job", 
                myListener.contains(rJob.getID(), false));
  }
  
  /**
   * This scheduler never schedules any task as it doesnt init any task. So all
   * the jobs are queued forever.
   */
  public static class MyScheduler extends JobQueueTaskScheduler {

    @Override
    public synchronized void start() throws IOException {
      super.start();
      // Remove the eager task initializer
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      // terminate it
      eagerTaskInitializationListener.terminate();
    }
  }
  
  public void testQueuedJobKill() throws Exception {
    LOG.info("Testing queued-job-kill");
    
    MyListener myListener = new MyListener();
    
    JobConf job = new JobConf();
    job.setClass("mapred.jobtracker.taskScheduler", MyScheduler.class,
                 TaskScheduler.class);
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1, null, null, job);
    
    job = mr.createJobConf();
    
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);
    
    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/output");

    RunningJob rJob = UtilsForTests.runJob(job, inDir, outDir);
    JobID id = rJob.getID();
    LOG.info("Job : " + id.toString() + " submitted");
    
    // check if the job is in the waiting queue
    assertTrue("Missing event notification on submiting a job", 
                myListener.contains(id, true));
    
    // kill the job
    LOG.info("Killing job : " + id.toString());
    rJob.killJob();
    
    // check if the job is killed
    assertEquals("Job status doesnt reflect the kill-job action", 
                 JobStatus.KILLED, rJob.getJobState());

    // check if the job is correctly moved
    // from the waiting list
    assertFalse("Missing event notification on killing a waiting job", 
                myListener.contains(id, true));
  }
}
