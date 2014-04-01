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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

public class TestUserLogCleanup {
  private static String jtid = "test";
  private static long ONE_HOUR = 1000 * 60 * 60;
  private Localizer localizer;
  private UserLogManager userLogManager;
  private UserLogCleaner userLogCleaner;
  private TaskTracker tt;
  private FakeClock myClock;
  private JobID jobid1 = new JobID(jtid, 1);
  private JobID jobid2 = new JobID(jtid, 2);
  private JobID jobid3 = new JobID(jtid, 3);
  private JobID jobid4 = new JobID(jtid, 4);
  private File foo = new File(TaskLog.getUserLogDir(), "foo");
  private File bar = new File(TaskLog.getUserLogDir(), "bar");
  private static String TEST_ROOT_DIR = 
	  				System.getProperty("test.build.data", "/tmp");

  public TestUserLogCleanup() throws IOException, InterruptedException {
    JobConf conf= new JobConf();
    startTT(conf);
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(TaskLog.getUserLogDir());
    FileUtil.fullyDelete(new File(TEST_ROOT_DIR));
  }

  private File localizeJob(JobID jobid) throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    new JobLocalizer(tt.getJobConf(), user, 
                     jobid.toString()).initializeJobLogDir();
    File jobUserlog = TaskLog.getJobDir(jobid);
    JobConf conf = new JobConf();
    // localize job log directory
    tt.saveLogDir(jobid, conf);
    assertTrue(jobUserlog + " directory is not created.", jobUserlog.exists());
    return jobUserlog;
  }

  private void jobFinished(JobID jobid, int logRetainHours) {
    JobCompletedEvent jce = new JobCompletedEvent(jobid, myClock.getTime(),
        logRetainHours);
    userLogManager.addLogEvent(jce);
  }

  private void startTT(JobConf conf) throws IOException, InterruptedException {
    myClock = new FakeClock(); // clock is reset.
    String localdirs = TEST_ROOT_DIR + "/userlogs/local/0," + 
    					TEST_ROOT_DIR + "/userlogs/local/1";
    conf.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, localdirs);
    tt = new TaskTracker();
    tt.setConf(new JobConf(conf));
    LocalDirAllocator localDirAllocator = 
      new LocalDirAllocator("mapred.local.dir");
    tt.setLocalDirAllocator(localDirAllocator);
    LocalStorage localStorage = new LocalStorage(conf.getLocalDirs());
    LocalFileSystem localFs = FileSystem.getLocal(conf);
    localStorage.checkDirs(localFs, true);
    tt.setLocalStorage(localStorage);
    localizer = new Localizer(FileSystem.get(conf), conf
        .getTrimmedStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
    tt.setLocalizer(localizer);
    userLogManager = new UtilsForTests.InLineUserLogManager(conf);
    TaskController taskController = userLogManager.getTaskController();
    taskController.setup(localDirAllocator, localStorage);
    tt.setTaskController(taskController);
    userLogCleaner = userLogManager.getUserLogCleaner();
    userLogCleaner.setClock(myClock);
    tt.setUserLogManager(userLogManager);
    userLogManager.clearOldUserLogs(conf);
  }

  private void ttReinited() throws IOException {
    JobConf conf=new JobConf();
    conf.setInt(JobContext.USER_LOG_RETAIN_HOURS, 3);
    userLogManager.clearOldUserLogs(conf);
  }

  private void ttRestarted() throws IOException, InterruptedException {
    JobConf conf=new JobConf();
    conf.setInt(JobContext.USER_LOG_RETAIN_HOURS, 3);
    startTT(conf);
  }

  /**
   * Tests job user-log directory deletion.
   * 
   * Adds two jobs for log deletion. One with one hour retain hours, other with
   * two retain hours. After an hour,
   * TaskLogCleanupThread.processCompletedJobs() call, makes sure job with 1hr
   * retain hours is removed and other is retained. After one more hour, job
   * with 2hr retain hours is also removed.
   * 
   * @throws IOException
   */
  @Test
  public void testJobLogCleanup() throws IOException {
    File jobUserlog1 = localizeJob(jobid1);
    File jobUserlog2 = localizeJob(jobid2);

    // add job user log directory for deletion, with 2 hours for deletion
    jobFinished(jobid1, 2);

    // add the job for deletion with one hour as retain hours
    jobFinished(jobid2, 1);

    // remove old logs and see jobid1 is not removed and jobid2 is removed
    myClock.advance(ONE_HOUR);
    userLogCleaner.processCompletedJobs();
    assertTrue(jobUserlog1 + " got deleted", jobUserlog1.exists());
    assertFalse(jobUserlog2 + " still exists.", jobUserlog2.exists());

    myClock.advance(ONE_HOUR);
    // remove old logs and see jobid1 is removed now
    userLogCleaner.processCompletedJobs();
    assertFalse(jobUserlog1 + " still exists.", jobUserlog1.exists());
  }

  /**
   * Tests user-log directory cleanup on a TT re-init with 3 hours as log retain
   * hours for tracker.
   * 
   * Adds job1 deletion before the re-init with 2 hour retain hours. Adds job2
   * for which there are no tasks/killJobAction after the re-init. Adds job3 for
   * which there is localizeJob followed by killJobAction with 3 hours as retain
   * hours. Adds job4 for which there are some tasks after the re-init.
   * 
   * @throws IOException
   */
  @Test
  public void testUserLogCleanup() throws IOException {
    File jobUserlog1 = localizeJob(jobid1);
    File jobUserlog2 = localizeJob(jobid2);
    File jobUserlog3 = localizeJob(jobid3);
    File jobUserlog4 = localizeJob(jobid4);
    // create a some files/dirs in userlog
    foo.mkdirs();
    bar.createNewFile();

    // add the jobid1 for deletion with retainhours = 2
    jobFinished(jobid1, 2);

    // time is now 1.
    myClock.advance(ONE_HOUR);

    // mimic TaskTracker reinit
    // re-init the tt with 3 hours as user log retain hours.
    // This re-init clears the user log directory
    // job directories will be added with 3 hours as retain hours.
    // i.e. They will be deleted at time 4.
    ttReinited();

    assertFalse(foo.exists());
    assertFalse(bar.exists());
    assertTrue(jobUserlog1.exists());
    assertTrue(jobUserlog2.exists());
    assertTrue(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());

    myClock.advance(ONE_HOUR);
    // time is now 2.
    userLogCleaner.processCompletedJobs();
    assertFalse(jobUserlog1.exists());
    assertTrue(jobUserlog2.exists());
    assertTrue(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());

    // mimic localizeJob followed KillJobAction for jobid3
    // add the job for deletion with retainhours = 3.
    // jobid3 should be deleted at time 5.
    jobUserlog3 = localizeJob(jobid3);
    jobFinished(jobid3, 3);

    // mimic localizeJob for jobid4
    jobUserlog4 = localizeJob(jobid4);

    // do cleanup
    myClock.advance(2 * ONE_HOUR);
    // time is now 4.
    userLogCleaner.processCompletedJobs();

    // jobid2 will be deleted
    assertFalse(jobUserlog1.exists());
    assertFalse(jobUserlog2.exists());
    assertTrue(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());

    myClock.advance(ONE_HOUR);
    // time is now 5.
    // do cleanup again
    userLogCleaner.processCompletedJobs();

    // jobid3 will be deleted
    assertFalse(jobUserlog1.exists());
    assertFalse(jobUserlog2.exists());
    assertFalse(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());
  }

  /**
   * Tests user-log directory cleanup on a TT restart.
   * 
   * Adds job1 deletion before the restart with 2 hour retain hours. Adds job2
   * for which there are no tasks/killJobAction after the restart. Adds job3 for
   * which there is localizeJob followed by killJobAction after the restart with
   * 3 hours retain hours. Adds job4 for which there are some tasks after the
   * restart.
   * 
   * @throws IOException
 * @throws InterruptedException 
   */
  @Test
  public void testUserLogCleanupAfterRestart() 
  					throws IOException, InterruptedException {
    File jobUserlog1 = localizeJob(jobid1);
    File jobUserlog2 = localizeJob(jobid2);
    File jobUserlog3 = localizeJob(jobid3);
    File jobUserlog4 = localizeJob(jobid4);
    // create a some files/dirs in userlog
    foo.mkdirs();
    bar.createNewFile();

    // add the jobid1 for deletion with retain hours = 2
    jobFinished(jobid1, 2);

    // time is now 1.
    myClock.advance(ONE_HOUR);

    // Mimic the TaskTracker restart
    // Restart the tt with 3 hours as user log retain hours.
    // This restart clears the user log directory
    // job directories will be added with 3 hours as retain hours.
    // i.e. They will be deleted at time 3 as clock will reset after the restart
    ttRestarted();

    assertFalse(foo.exists());
    assertFalse(bar.exists());
    assertTrue(jobUserlog1.exists());
    assertTrue(jobUserlog2.exists());
    assertTrue(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());

    myClock.advance(ONE_HOUR);
    // time is now 1.
    userLogCleaner.processCompletedJobs();
    assertTrue(jobUserlog1.exists());
    assertTrue(jobUserlog2.exists());
    assertTrue(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());

    // mimic localizeJob followed KillJobAction for jobid3
    // add the job for deletion with retainhours = 3.
    // jobid3 should be deleted at time 4.
    jobUserlog3 = localizeJob(jobid3);
    jobFinished(jobid3, 3);

    // mimic localizeJob for jobid4
    jobUserlog4 = localizeJob(jobid4);

    // do cleanup
    myClock.advance(2 * ONE_HOUR);
    // time is now 3.
    userLogCleaner.processCompletedJobs();

    // jobid1 and jobid2 will be deleted
    assertFalse(jobUserlog1.exists());
    assertFalse(jobUserlog2.exists());
    assertTrue(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());

    myClock.advance(ONE_HOUR);
    // time is now 4.
    // do cleanup again
    userLogCleaner.processCompletedJobs();

    // jobid3 will be deleted
    assertFalse(jobUserlog1.exists());
    assertFalse(jobUserlog2.exists());
    assertFalse(jobUserlog3.exists());
    assertTrue(jobUserlog4.exists());
  }
}
