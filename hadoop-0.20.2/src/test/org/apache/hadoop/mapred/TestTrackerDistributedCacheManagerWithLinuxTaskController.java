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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController.MyLinuxTaskController;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.filecache.TestTrackerDistributedCacheManager;

/**
 * Test the DistributedCacheManager when LinuxTaskController is used.
 * 
 */
public class TestTrackerDistributedCacheManagerWithLinuxTaskController extends
    TestTrackerDistributedCacheManager {

  private File configFile;

  private static final Log LOG =
      LogFactory
          .getLog(TestTrackerDistributedCacheManagerWithLinuxTaskController.class);

  @Override
  protected void setUp()
      throws IOException, InterruptedException {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    TEST_ROOT_DIR =
        new File(System.getProperty("test.build.data", "/tmp"),
            TestTrackerDistributedCacheManagerWithLinuxTaskController.class
                .getSimpleName()).getAbsolutePath();

    super.setUp();

    taskController = new MyLinuxTaskController();
    String path =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_PATH);
    String execPath = path + "/task-controller";
    ((MyLinuxTaskController)taskController).setTaskControllerExe(execPath);
    taskController.setConf(conf);
    UtilsForTests.setupTC(taskController, localDirAllocator,
        conf.getStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
  }

  @Override
  protected void refreshConf(Configuration conf) throws IOException {
    super.refreshConf(conf);
    String path =
      System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_PATH);
    configFile =
      ClusterWithLinuxTaskController.createTaskControllerConf(path, conf);
   
  }

  @Override
  protected void tearDown()
      throws IOException {
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }
    if (configFile != null) {
      configFile.delete();
    }
    super.tearDown();
  }

  @Override
  protected boolean canRun() {
    return ClusterWithLinuxTaskController.shouldRun();
  }

  @Override
  protected String getJobOwnerName() {
    String ugi =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_UGI);
    String userName = ugi.split(",")[0];
    return userName;
  }

  @Override
  protected void checkFilePermissions(Path[] localCacheFiles)
      throws IOException {
    String userName = getJobOwnerName();
    String filePermissions = UserGroupInformation.getLoginUser()
        .getShortUserName().equals(userName) ? "-rwxrwx---" : "-r-xrwx---";

    for (Path p : localCacheFiles) {
      // First make sure that the cache file has proper permissions.
      TestTaskTrackerLocalization.checkFilePermissions(p.toUri().getPath(),
          filePermissions, userName,
          ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
      // Now. make sure that all the path components also have proper
      // permissions.
      checkPermissionOnPathComponents(p.toUri().getPath(), userName);
    }

  }

  /**
   * @param cachedFilePath
   * @param userName
   * @throws IOException
   */
  private void checkPermissionOnPathComponents(String cachedFilePath,
      String userName)
      throws IOException {
    // The trailing distcache/file/... string
    String trailingStringForFirstFile =
        cachedFilePath.replaceFirst(ROOT_MAPRED_LOCAL_DIR.getAbsolutePath()
            + Path.SEPARATOR + "0_[0-" + (numLocalDirs - 1) + "]"
            + Path.SEPARATOR + TaskTracker.getPrivateDistributedCacheDir(userName),
            "");
    LOG.info("Trailing path for cacheFirstFile is : "
        + trailingStringForFirstFile);
    // The leading mapred.local.dir/0_[0-n]/taskTracker/$user string.
    String leadingStringForFirstFile =
        cachedFilePath.substring(0, cachedFilePath
            .lastIndexOf(trailingStringForFirstFile));
    LOG.info("Leading path for cacheFirstFile is : "
        + leadingStringForFirstFile);

    String dirPermissions = UserGroupInformation.getLoginUser()
        .getShortUserName().equals(userName) ? "drwxrws---" : "dr-xrws---";

    // Now check path permissions, starting with cache file's parent dir.
    File path = new File(cachedFilePath).getParentFile();
    while (!path.getAbsolutePath().equals(leadingStringForFirstFile)) {
      TestTaskTrackerLocalization.checkFilePermissions(path.getAbsolutePath(),
          dirPermissions, userName, 
          ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
      path = path.getParentFile();
    }
  }
}
