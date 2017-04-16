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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController.MyLinuxTaskController;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Test to verify localization of a job and localization of a task on a
 * TaskTracker when {@link LinuxTaskController} is used.
 * 
 */
public class TestLocalizationWithLinuxTaskController extends
    TestTaskTrackerLocalization {

  private static final Log LOG =
      LogFactory.getLog(TestLocalizationWithLinuxTaskController.class);

  private File configFile;
  private static String taskTrackerUserName;

  @Override
  protected boolean canRun() {
    return ClusterWithLinuxTaskController.shouldRun();
  }

  @Override
  protected void setUp()
      throws Exception {

    if (!canRun()) {
      return;
    }

    super.setUp();

    taskTrackerUserName = UserGroupInformation.getLoginUser()
                          .getShortUserName();
  }

  @Override
  protected void tearDown()
      throws Exception {
    if (!canRun()) {
      return;
    }
    super.tearDown();
    if (configFile != null) {
      configFile.delete();
    }
  }

  protected TaskController getTaskController() {
    return new MyLinuxTaskController();
  }

  protected UserGroupInformation getJobOwner() {
    String ugi = System
        .getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_UGI);
    String[] splits = ugi.split(",");
    return UserGroupInformation.createUserForTesting(splits[0],
        new String[] { splits[1] });
  }

  /** @InheritDoc */
  @Override
  public void testTaskControllerSetup() {
    // Do nothing.
  }

  @Override
  protected void checkUserLocalization()
      throws IOException {
    // Check the directory structure and permissions
    for (String dir : localDirs) {

      File localDir = new File(dir);
      assertTrue("mapred.local.dir " + localDir + " isn'task created!",
          localDir.exists());

      File taskTrackerSubDir = new File(localDir, TaskTracker.SUBDIR);
      assertTrue("taskTracker sub-dir in the local-dir " + localDir
          + "is not created!", taskTrackerSubDir.exists());

      // user-dir, jobcache and distcache will have
      //     2770 permissions if jobOwner is same as tt_user
      //     2570 permissions for any other user
      String expectedDirPerms = taskTrackerUserName.equals(task.getUser())
                                ? "drwxrws---"
                                : "dr-xrws---";

      File userDir = new File(taskTrackerSubDir, task.getUser());
      assertTrue("user-dir in taskTrackerSubdir " + taskTrackerSubDir
          + "is not created!", userDir.exists());

      checkFilePermissions(userDir.getAbsolutePath(), expectedDirPerms, task
          .getUser(), ClusterWithLinuxTaskController.taskTrackerSpecialGroup);

      File jobCache = new File(userDir, TaskTracker.JOBCACHE);
      assertTrue("jobcache in the userDir " + userDir + " isn't created!",
          jobCache.exists());

      checkFilePermissions(jobCache.getAbsolutePath(), expectedDirPerms, task
          .getUser(), ClusterWithLinuxTaskController.taskTrackerSpecialGroup);

      // Verify the distributed cache dir.
      File distributedCacheDir =
          new File(localDir, TaskTracker
              .getPrivateDistributedCacheDir(task.getUser()));
      assertTrue("distributed cache dir " + distributedCacheDir
          + " doesn't exists!", distributedCacheDir.exists());
      checkFilePermissions(distributedCacheDir.getAbsolutePath(),
          expectedDirPerms, task.getUser(),
          ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    }
  }

  @Override
  protected void checkJobLocalization()
      throws IOException {
    // job-dir, jars-dir and subdirectories in them will have
    //     2770 permissions if jobOwner is same as tt_user
    //     2570 permissions for any other user
    // Files under these dirs will have
    //      770 permissions if jobOwner is same as tt_user
    //      570 permissions for any other user
    String expectedDirPerms = taskTrackerUserName.equals(task.getUser())
                              ? "drwxrws---"
                              : "dr-xrws---";
    String expectedFilePerms = taskTrackerUserName.equals(task.getUser())
                               ? "-rwxrwx---"
                               : "-r-xrwx---";

    for (String localDir : trackerFConf.getStrings("mapred.local.dir")) {
      File jobDir =
          new File(localDir, TaskTracker.getLocalJobDir(task.getUser(), jobId
              .toString()));
      // check the private permissions on the job directory
      checkFilePermissions(jobDir.getAbsolutePath(), expectedDirPerms, task
          .getUser(), ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    }

    // check the private permissions of various directories
    List<Path> dirs = new ArrayList<Path>();
    Path jarsDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarsDir(task.getUser(),
            jobId.toString()), trackerFConf);
    dirs.add(jarsDir);
    dirs.add(new Path(jarsDir, "lib"));
    for (Path dir : dirs) {
      checkFilePermissions(dir.toUri().getPath(), expectedDirPerms,
          task.getUser(),
          ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    }

    // job-work dir needs user writable permissions i.e. 2770 for any user
    Path jobWorkDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobWorkDir(task.getUser(),
            jobId.toString()), trackerFConf);
    checkFilePermissions(jobWorkDir.toUri().getPath(), "drwxrws---", task
        .getUser(), ClusterWithLinuxTaskController.taskTrackerSpecialGroup);

    // check the private permissions of various files
    List<Path> files = new ArrayList<Path>();
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getLocalJobConfFile(
        task.getUser(), jobId.toString()), trackerFConf));
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarFile(task
        .getUser(), jobId.toString()), trackerFConf));
    files.add(new Path(jarsDir, "lib" + Path.SEPARATOR + "lib1.jar"));
    files.add(new Path(jarsDir, "lib" + Path.SEPARATOR + "lib2.jar"));
    for (Path file : files) {
      checkFilePermissions(file.toUri().getPath(), expectedFilePerms, task
          .getUser(), ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    }

    // check job user-log directory permissions
    File jobLogDir = TaskLog.getJobDir(jobId);
    checkFilePermissions(jobLogDir.toString(), expectedDirPerms, task.getUser(),
        ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    // check job-acls.xml file permissions
    checkFilePermissions(jobLogDir.toString() + Path.SEPARATOR
        + TaskTracker.jobACLsFile, expectedFilePerms, task.getUser(),
        ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    
    // validate the content of job ACLs file
    validateJobACLsFileContent();
  }

  @Override
  protected void checkTaskLocalization()
      throws IOException {
    // check the private permissions of various directories
    List<Path> dirs = new ArrayList<Path>();
    dirs.add(lDirAlloc.getLocalPathToRead(TaskTracker.getLocalTaskDir(task
        .getUser(), jobId.toString(), taskId.toString(),
        task.isTaskCleanupTask()), trackerFConf));
    dirs.add(attemptWorkDir);
    dirs.add(new Path(attemptWorkDir, "tmp"));
    dirs.add(new Path(attemptLogFiles[1].getParentFile().getAbsolutePath()));
    for (Path dir : dirs) {
      checkFilePermissions(dir.toUri().getPath(), "drwxrws---",
          task.getUser(),
          ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    }

    // check the private permissions of various files
    List<Path> files = new ArrayList<Path>();
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getTaskConfFile(task
        .getUser(), task.getJobID().toString(), task.getTaskID().toString(),
        task.isTaskCleanupTask()), trackerFConf));
    for (Path file : files) {
      checkFilePermissions(file.toUri().getPath(), "-rwxrwx---", task
          .getUser(), ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    }
  }
}
