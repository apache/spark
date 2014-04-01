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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.security.UserGroupInformation;

import junit.framework.TestCase;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.*;
/** 
 * TestJobTrackerRestart checks if the jobtracker can restart. JobTracker 
 * should be able to continue running the previously running jobs and also 
 * recover previosuly submitted jobs.
 */
/**UNTIL MAPREDUCE-873 is backported, we will not run recovery manager tests
 */
@Ignore
public class TestJobTrackerRestart extends TestCase {
  static final Path testDir = 
    new Path(System.getProperty("test.build.data","/tmp"), 
             "jt-restart-testing");
  final Path inDir = new Path(testDir, "input");
  static final Path shareDir = new Path(testDir, "share");
  final Path outputDir = new Path(testDir, "output");
  private static int numJobsSubmitted = 0;
  
  /**
   * Return the job conf configured with the priorities and mappers as passed.
   * @param conf The default conf
   * @param priorities priorities for the jobs
   * @param numMaps number of maps for the jobs
   * @param numReds number of reducers for the jobs
   * @param outputDir output dir
   * @param inDir input dir
   * @param mapSignalFile filename thats acts as a signal for maps
   * @param reduceSignalFile filename thats acts as a signal for reducers
   * @return a array of jobconfs configured as needed
   * @throws IOException
   */
  private static JobConf[] getJobs(JobConf conf, JobPriority[] priorities, 
                           int[] numMaps, int[] numReds,
                           Path outputDir, Path inDir,
                           String mapSignalFile, String reduceSignalFile) 
  throws IOException {
    JobConf[] jobs = new JobConf[priorities.length];
    for (int i = 0; i < jobs.length; ++i) {
      jobs[i] = new JobConf(conf);
      Path newOutputDir = outputDir.suffix(String.valueOf(numJobsSubmitted++));
      UtilsForTests.configureWaitingJobConf(jobs[i], inDir, newOutputDir, 
          numMaps[i], numReds[i], "jt restart test job", mapSignalFile, 
          reduceSignalFile);
      jobs[i].setJobPriority(priorities[i]);
    }
    return jobs;
  }

  /**
   * Clean up the signals.
   */
  private static void cleanUp(FileSystem fileSys, Path dir) throws IOException {
    // Delete the map signal file
    fileSys.delete(new Path(getMapSignalFile(dir)), false);
    // Delete the reduce signal file
    fileSys.delete(new Path(getReduceSignalFile(dir)), false);
  }
  
 /**
   * Tests the jobtracker with restart-recovery turned off.
   * Submit a job with normal priority, maps = 2, reducers = 0}
   * 
   * Wait for the job to complete 50%
   * 
   * Restart the jobtracker with recovery turned off
   * 
   * Check if the job is missing
   */
  public void testRestartWithoutRecovery(MiniDFSCluster dfs, 
                                         MiniMRCluster mr) 
  throws IOException {
    // III. Test a job with waiting mapper and recovery turned off
    
    FileSystem fileSys = dfs.getFileSystem();
    
    cleanUp(fileSys, shareDir);
    
    JobConf newConf = getJobs(mr.createJobConf(), 
                              new JobPriority[] {JobPriority.NORMAL}, 
                              new int[] {2}, new int[] {0},
                              outputDir, inDir, 
                              getMapSignalFile(shareDir), 
                              getReduceSignalFile(shareDir))[0];
    
    JobClient jobClient = new JobClient(newConf);
    RunningJob job = jobClient.submitJob(newConf);
    JobID id = job.getID();
    
    //  make sure that the job is 50% completed
    while (UtilsForTests.getJobStatus(jobClient, id).mapProgress() < 0.5f) {
      UtilsForTests.waitFor(100);
    }
    
    mr.stopJobTracker();
    
    // Turn off the recovery
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      false);
    
    // Wait for a minute before submitting a job
    UtilsForTests.waitFor(60 * 1000);
    
    mr.startJobTracker();
    
    // Signal the tasks
    UtilsForTests.signalTasks(dfs, fileSys, true, getMapSignalFile(shareDir), 
                              getReduceSignalFile(shareDir));
    
    // Wait for the JT to be ready
    UtilsForTests.waitForJobTracker(jobClient);
    
    UtilsForTests.waitTillDone(jobClient);
    
    // The submitted job should not exist
    assertTrue("Submitted job was detected with recovery disabled", 
               UtilsForTests.getJobStatus(jobClient, id) == null);
  }

  /** Tests a job on jobtracker with restart-recovery turned on.
   * Preparation :
   *    - Configure a job with
   *       - num-maps : 50
   *       - num-reducers : 1
   *    - Configure the cluster to run 1 reducer
   *    - Lower the history file block size and buffer
   *    
   * Wait for the job to complete 50%. Note that all the job is configured to 
   * use {@link HalfWaitingMapper} and {@link WaitingReducer}. So job will 
   * eventually wait on 50%
   * 
   * Make a note of the following things
   *    - Task completion events
   *    - Cluster status
   *    - Task Reports
   *    - Job start time
   *    
   * Restart the jobtracker
   * 
   * Wait for job to finish all the maps and note the TaskCompletion events at
   * the tracker.
   * 
   * Wait for all the jobs to finish and note the following
   *    - New task completion events at the jobtracker
   *    - Task reports
   *    - Cluster status
   * 
   * Check for the following
   *    - Task completion events for recovered tasks should match 
   *    - Task completion events at the tasktracker and the restarted 
   *      jobtracker should be same
   *    - Cluster status should be fine.
   *    - Task Reports for recovered tasks should match
   *      Checks
   *        - start time
   *        - finish time
   *        - counters
   *        - http-location
   *        - task-id
   *    - Job start time should match
   *    - Check if the counters can be accessed
   *    - Check if the history files are (re)named properly
   */
  public void testTaskEventsAndReportsWithRecovery(MiniDFSCluster dfs, 
                                                   MiniMRCluster mr) 
  throws IOException {
    // II. Test a tasktracker with waiting mapper and recovery turned on.
    //     Ideally the tracker should SYNC with the new/restarted jobtracker
    
    FileSystem fileSys = dfs.getFileSystem();
    final int numMaps = 50;
    final int numReducers = 1;
    
    
    cleanUp(fileSys, shareDir);
    
    JobConf newConf = getJobs(mr.createJobConf(), 
                              new JobPriority[] {JobPriority.NORMAL}, 
                              new int[] {numMaps}, new int[] {numReducers},
                              outputDir, inDir, 
                              getMapSignalFile(shareDir), 
                              getReduceSignalFile(shareDir))[0];
    
    JobClient jobClient = new JobClient(newConf);
    RunningJob job = jobClient.submitJob(newConf);
    JobID id = job.getID();
    
    // change the job priority
    mr.setJobPriority(id, JobPriority.HIGH);
    
    mr.initializeJob(id);
    
    //  make sure that atleast on reducer is spawned
    while (jobClient.getClusterStatus().getReduceTasks() == 0) {
      UtilsForTests.waitFor(100);
    }
    
    while(true) {
      // Since we are using a half waiting mapper, maps should be stuck at 50%
      TaskCompletionEvent[] trackerEvents = 
        mr.getMapTaskCompletionEventsUpdates(0, id, numMaps)
          .getMapTaskCompletionEvents();
      if (trackerEvents.length < numMaps / 2) {
        UtilsForTests.waitFor(1000);
      } else {
        break;
      }
    }
    
    TaskCompletionEvent[] prevEvents = 
      mr.getTaskCompletionEvents(id, 0, numMaps);
    TaskReport[] prevSetupReports = jobClient.getSetupTaskReports(id);
    TaskReport[] prevMapReports = jobClient.getMapTaskReports(id);
    ClusterStatus prevStatus = jobClient.getClusterStatus();
    
    mr.stopJobTracker();
    
    // Turn off the recovery
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);
    
    //  Wait for a minute before submitting a job
    UtilsForTests.waitFor(60 * 1000);
    
    mr.startJobTracker();
    
    // Signal the map tasks
    UtilsForTests.signalTasks(dfs, fileSys, true, getMapSignalFile(shareDir), 
                              getReduceSignalFile(shareDir));
    
    // Wait for the JT to be ready
    UtilsForTests.waitForJobTracker(jobClient);
    
    int numToMatch = mr.getNumEventsRecovered() / 2;
    
    //  make sure that the maps are completed
    while (UtilsForTests.getJobStatus(jobClient, id).mapProgress() < 1.0f) {
      UtilsForTests.waitFor(100);
    }
    
    // Get the new jobtrackers events
    TaskCompletionEvent[] jtEvents =  
      mr.getTaskCompletionEvents(id, 0, 2 * numMaps);
    
    // Test if all the events that were recovered match exactly
    testTaskCompletionEvents(prevEvents, jtEvents, false, numToMatch);
    
    // Check the task reports
    // The reports should match exactly if the attempts are same
    TaskReport[] afterMapReports = jobClient.getMapTaskReports(id);
    TaskReport[] afterSetupReports = jobClient.getSetupTaskReports(id);
    testTaskReports(prevMapReports, afterMapReports, numToMatch - 1);
    testTaskReports(prevSetupReports, afterSetupReports, 1);
    
    // check the job priority
    assertEquals("Job priority change is not reflected", 
                 JobPriority.HIGH, mr.getJobPriority(id));
    
    List<TaskCompletionEvent> jtMapEvents =
      new ArrayList<TaskCompletionEvent>();
    for (TaskCompletionEvent tce : jtEvents) {
      if (tce.isMapTask()) {
        jtMapEvents.add(tce);
      }
    }
   
    TaskCompletionEvent[] trackerEvents; 
    while(true) {
     // Wait for the tracker to pull all the map events
     trackerEvents =
       mr.getMapTaskCompletionEventsUpdates(0, id, jtMapEvents.size())
         .getMapTaskCompletionEvents();
     if (trackerEvents.length < jtMapEvents.size()) {
       UtilsForTests.waitFor(1000);
     } else {
       break;
     }
   }

    //  Signal the reduce tasks
    UtilsForTests.signalTasks(dfs, fileSys, false, getMapSignalFile(shareDir), 
                              getReduceSignalFile(shareDir));
    
    UtilsForTests.waitTillDone(jobClient);
    
    testTaskCompletionEvents(jtMapEvents.toArray(new TaskCompletionEvent[0]), 
                              trackerEvents, true, -1);
    
    // validate the history file
    TestJobHistory.validateJobHistoryFileFormat(id, newConf, "SUCCESS", true);
    TestJobHistory.validateJobHistoryFileContent(mr, job, newConf);
    
    // check if the cluster status is insane
    ClusterStatus status = jobClient.getClusterStatus();
    assertTrue("Cluster status is insane", 
               checkClusterStatusOnCompletion(status, prevStatus));
  }

  /**
   * Matches specified number of task reports.
   * @param source the reports to be matched
   * @param target reports to match with
   * @param numToMatch num reports to match
   * @param mismatchSet reports that should not match
   */
  private void testTaskReports(TaskReport[] source, TaskReport[] target, 
                               int numToMatch) {
    for (int i = 0; i < numToMatch; ++i) {
      // Check if the task reports was recovered correctly
      assertTrue("Task reports for same attempt has changed", 
                 source[i].equals(target[i]));
    }
  }
  
  /**
   * Matches the task completion events.
   * @param source the events to be matched
   * @param target events to match with
   * @param fullMatch whether to match the events completely or partially
   * @param numToMatch number of events to match in case full match is not 
   *        desired
   * @param ignoreSet a set of taskids to ignore
   */
  private void testTaskCompletionEvents(TaskCompletionEvent[] source, 
                                       TaskCompletionEvent[] target, 
                                       boolean fullMatch,
                                       int numToMatch) {
    //  Check if the event list size matches
    // The lengths should match only incase of full match
    if (fullMatch) {
      assertEquals("Map task completion events mismatch", 
                   source.length, target.length);
      numToMatch = source.length;
    }
    // Check if the events match
    for (int i = 0; i < numToMatch; ++i) {
      if (source[i].getTaskAttemptId().equals(target[i].getTaskAttemptId())){
        assertTrue("Map task completion events ordering mismatch", 
                   source[i].equals(target[i]));
      }
    }
  }
  
  private boolean checkClusterStatusOnCompletion(ClusterStatus status, 
                                                 ClusterStatus prevStatus) {
    return status.getJobTrackerState() == prevStatus.getJobTrackerState()
           && status.getMapTasks() == 0
           && status.getReduceTasks() == 0;
  }
  
  /** Committer with setup waiting
   */
  static class CommitterWithDelaySetup extends FileOutputCommitter {
    @Override
    public void setupJob(JobContext context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      while (true) {
        if (fs.exists(shareDir)) {
          break;
        }
        UtilsForTests.waitFor(100);
      }
      super.cleanupJob(context);
    }
  }

  /** Tests a job on jobtracker with restart-recovery turned on and empty 
   *  jobhistory file.
   * Preparation :
   *    - Configure a job with
   *       - num-maps : 0 (long waiting setup)
   *       - num-reducers : 0
   *    
   * Check if the job succeedes after restart.
   * 
   * Assumption that map slots are given first for setup.
   */
  public void testJobRecoveryWithEmptyHistory(MiniDFSCluster dfs, 
                                              MiniMRCluster mr) 
  throws IOException {
    mr.startTaskTracker(null, null, 1, 1);
    FileSystem fileSys = dfs.getFileSystem();
    
    cleanUp(fileSys, shareDir);
    cleanUp(fileSys, inDir);
    cleanUp(fileSys, outputDir);
    
    JobConf conf = mr.createJobConf();
    conf.setNumReduceTasks(0);
    conf.setOutputCommitter(TestEmptyJob.CommitterWithDelayCleanup.class);
    fileSys.delete(outputDir, false);
    RunningJob job1 = 
      UtilsForTests.runJob(conf, inDir, outputDir, 30, 0);
    
    conf.setNumReduceTasks(0);
    conf.setOutputCommitter(CommitterWithDelaySetup.class);
    Path inDir2 = new Path(testDir, "input2");
    fileSys.mkdirs(inDir2);
    Path outDir2 = new Path(testDir, "output2");
    fileSys.delete(outDir2, false);
    JobConf newConf = getJobs(mr.createJobConf(),
                              new JobPriority[] {JobPriority.NORMAL},
                              new int[] {10}, new int[] {0},
                              outDir2, inDir2,
                              getMapSignalFile(shareDir),
                              getReduceSignalFile(shareDir))[0];

    JobClient jobClient = new JobClient(newConf);
    RunningJob job2 = jobClient.submitJob(newConf);
    JobID id = job2.getID();

    /*RunningJob job2 = 
      UtilsForTests.runJob(mr.createJobConf(), inDir2, outDir2, 0);
    
    JobID id = job2.getID();*/
    JobInProgress jip = mr.getJobTrackerRunner().getJobTracker().getJob(id);
    
    mr.getJobTrackerRunner().getJobTracker().initJob(jip);
    
    // find out the history filename
    String history = 
      JobHistory.JobInfo.getJobHistoryFileName(jip.getJobConf(), id);
    Path historyPath = JobHistory.JobInfo.getJobHistoryLogLocation(history);
    // get the conf file name
    String parts[] = history.split("_");
    // jobtracker-hostname_jobtracker-identifier_conf.xml
    String jobUniqueString = parts[0] + "_" + parts[1] + "_" +  id;
    Path confPath = new Path(historyPath.getParent(), jobUniqueString + "_conf.xml");
    
    //  make sure that setup is launched
    while (jip.runningMaps() == 0) {
      UtilsForTests.waitFor(100);
    }
    
    id = job1.getID();
    jip = mr.getJobTrackerRunner().getJobTracker().getJob(id);
    
    mr.getJobTrackerRunner().getJobTracker().initJob(jip);
    
    //  make sure that cleanup is launched and is waiting
    while (!jip.isCleanupLaunched()) {
      UtilsForTests.waitFor(100);
    }
    
    mr.stopJobTracker();
    
    // delete the history file .. just to be safe.
    FileSystem historyFS = historyPath.getFileSystem(conf);
    historyFS.delete(historyPath, false);
    historyFS.create(historyPath).close(); // create an empty file
    
    
    UtilsForTests.signalTasks(dfs, fileSys, getMapSignalFile(shareDir), getReduceSignalFile(shareDir), (short)1);

    // Turn on the recovery
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);
    
    mr.startJobTracker();
    
    job1.waitForCompletion();
    job2.waitForCompletion();

    // check if the old files are deleted
    assertFalse("Old jobhistory file is not deleted", historyFS.exists(historyPath));
    assertFalse("Old jobconf file is not deleted", historyFS.exists(confPath));
  }
  
  public void testJobTrackerRestart() throws IOException {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;

    try {
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, null, null);
      dfs.waitActive();
      fileSys = dfs.getFileSystem();
      
      // clean up
      fileSys.delete(testDir, true);
      
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }

      // Write the input file
      UtilsForTests.writeFile(dfs.getNameNode(), conf, 
                              new Path(inDir + "/file"), (short)1);

      dfs.startDataNodes(conf, 1, true, null, null, null, null);
      dfs.waitActive();

      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" 
                 + (dfs.getFileSystem()).getUri().getPort();

      // Make sure that jobhistory leads to a proper job restart
      // So keep the blocksize and the buffer size small
      JobConf jtConf = new JobConf();
      jtConf.set("mapred.jobtracker.job.history.block.size", "1024");
      jtConf.set("mapred.jobtracker.job.history.buffer.size", "1024");
      jtConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
      jtConf.setLong("mapred.tasktracker.expiry.interval", 25 * 1000);
      jtConf.setBoolean(JobConf.MR_ACLS_ENABLED, true);
      // get the user group info
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      jtConf.set(QueueManager.toFullPropertyName("default",
          QueueACL.SUBMIT_JOB.getAclName()), ugi.getUserName());
      
      mr = new MiniMRCluster(1, namenode, 1, null, null, jtConf);
      
      // Test the tasktracker SYNC
      testTaskEventsAndReportsWithRecovery(dfs, mr);
      
      // Test jobtracker with restart-recovery turned off
      testRestartWithoutRecovery(dfs, mr);
      
      // test recovery with empty file
      testJobRecoveryWithEmptyHistory(dfs, mr);
    } finally {
      if (mr != null) {
        try {
          mr.shutdown();
        } catch (Exception e) {}
      }
      if (dfs != null) {
        try {
          dfs.shutdown();
        } catch (Exception e) {}
      }
    }
  }

  private static String getMapSignalFile(Path dir) {
    return (new Path(dir, "jt-restart-map-signal")).toString();
  }

  private static String getReduceSignalFile(Path dir) {
    return (new Path(dir, "jt-restart-reduce-signal")).toString();
  }
  
  public static void main(String[] args) throws IOException {
    new TestJobTrackerRestart().testJobTrackerRestart();
  }
}
