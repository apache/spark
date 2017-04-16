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
package org.apache.hadoop.mapreduce.server.tasktracker;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapreduce.JobID;

/**
 * 
 * NOTE: This class is internal only and not intended for users!!
 */
public class Localizer {

  static final Log LOG = LogFactory.getLog(Localizer.class);

  private FileSystem fs;
  private String[] localDirs;

  /**
   * Create a Localizer instance
   * 
   * @param fileSys
   * @param lDirs
   */
  public Localizer(FileSystem fileSys, String[] lDirs) {
    fs = fileSys;
    localDirs = lDirs;
  }


  // Data-structure for synchronizing localization of user directories.
  private Map<String, AtomicBoolean> localizedUsers =
      new HashMap<String, AtomicBoolean>();

  /**
   * Initialize the local directories for a particular user on this TT. This
   * involves creation and setting permissions of the following directories
   * <ul>
   * <li>$mapred.local.dir/taskTracker/$user</li>
   * <li>$mapred.local.dir/taskTracker/$user/jobcache</li>
   * <li>$mapred.local.dir/taskTracker/$user/distcache</li>
   * </ul>
   * 
   * @param user
   * @throws IOException
   */
  public void initializeUserDirs(String user)
      throws IOException {

    if (user == null) {
      // This shouldn't happen in general
      throw new IOException(
          "User is null. Cannot initialized user-directories.");
    }

    AtomicBoolean localizedUser;
    synchronized (localizedUsers) {
      if (!localizedUsers.containsKey(user)) {
        localizedUsers.put(user, new AtomicBoolean(false));
      }
      localizedUser = localizedUsers.get(user);
    }

    synchronized (localizedUser) {

      if (localizedUser.get()) {
        // User-directories are already localized for this user.
        LOG.info("User-directories for the user " + user
            + " are already initialized on this TT. Not doing anything.");
        return;
      }

      LOG.info("Initializing user " + user + " on this TT.");

      boolean userDirStatus = false;
      boolean jobCacheDirStatus = false;
      boolean distributedCacheDirStatus = false;

      for (String localDir : localDirs) {

        Path userDir = new Path(localDir, TaskTracker.getUserDir(user));

        // Set up the user-directory.
        if (fs.exists(userDir) || fs.mkdirs(userDir)) {

          // Set permissions on the user-directory
          fs.setPermission(userDir, new FsPermission((short)0700));
          userDirStatus = true;

          // Set up the jobcache directory
          Path jobCacheDir =
              new Path(localDir, TaskTracker.getJobCacheSubdir(user));
          if (fs.exists(jobCacheDir) || fs.mkdirs(jobCacheDir)) {
            // Set permissions on the jobcache-directory
            fs.setPermission(jobCacheDir, new FsPermission((short)0700));
            jobCacheDirStatus = true;
          } else {
            LOG.warn("Unable to create job cache directory : "
                + jobCacheDir);
          }

          // Set up the cache directory used for distributed cache files
          Path distributedCacheDir =
              new Path(localDir, TaskTracker.getPrivateDistributedCacheDir(user));
          if (fs.exists(distributedCacheDir) || fs.mkdirs(distributedCacheDir)) {
            // Set permissions on the distcache-directory
            fs.setPermission(distributedCacheDir, new FsPermission((short)0700));
            distributedCacheDirStatus = true;
          } else {
            LOG.warn("Unable to create distributed-cache directory : "
                + distributedCacheDir);
          }
        } else {
          LOG.warn("Unable to create the user directory : " + userDir);
        }
      }

      if (!userDirStatus) {
        throw new IOException("Not able to initialize user directories "
            + "in any of the configured local directories for user " + user);
      }
      if (!jobCacheDirStatus) {
        throw new IOException("Not able to initialize job-cache directories "
            + "in any of the configured local directories for user " + user);
      }
      if (!distributedCacheDirStatus) {
        throw new IOException(
            "Not able to initialize distributed-cache directories "
                + "in any of the configured local directories for user "
                + user);
      }

      // Localization of the user is done
      localizedUser.set(true);
    }
  }

  /**
   * Prepare the job directories for a given job. To be called by the job
   * localization code, only if the job is not already localized.
   * 
   * <br>
   * Here, we set 700 permissions on the job directories created on all disks.
   * This we do so as to avoid any misuse by other users till the time
   * {@link TaskController#initializeJob} is run at a
   * later time to set proper private permissions on the job directories. <br>
   * 
   * @param user
   * @param jobId
   * @throws IOException
   */
  public void initializeJobDirs(String user, JobID jobId)
      throws IOException {
    boolean initJobDirStatus = false;
    String jobDirPath = TaskTracker.getLocalJobDir(user, jobId.toString());
    for (String localDir : localDirs) {
      Path jobDir = new Path(localDir, jobDirPath);
      if (fs.exists(jobDir)) {
        // this will happen on a partial execution of localizeJob. Sometimes
        // copying job.xml to the local disk succeeds but copying job.jar might
        // throw out an exception. We should clean up and then try again.
        fs.delete(jobDir, true);
      }

      boolean jobDirStatus = fs.mkdirs(jobDir);
      if (!jobDirStatus) {
        LOG.warn("Not able to create job directory " + jobDir.toString());
      }

      initJobDirStatus = initJobDirStatus || jobDirStatus;

      // job-dir has to be private to the TT
      fs.setPermission(jobDir, new FsPermission((short)0700));
    }

    if (!initJobDirStatus) {
      throw new IOException("Not able to initialize job directories "
          + "in any of the configured local directories for job "
          + jobId.toString());
    }
  }

  /**
   * Create taskDirs on all the disks. Otherwise, in some cases, like when
   * LinuxTaskController is in use, child might wish to balance load across
   * disks but cannot itself create attempt directory because of the fact that
   * job directory is writable only by the TT.
   * 
   * @param user
   * @param jobId
   * @param attemptId
   * @throws IOException
   */
  public void initializeAttemptDirs(String user, String jobId,
      String attemptId)
      throws IOException {

    boolean initStatus = false;
    String attemptDirPath =
        TaskTracker.getLocalTaskDir(user, jobId, attemptId);

    for (String localDir : localDirs) {
      Path localAttemptDir = new Path(localDir, attemptDirPath);

      boolean attemptDirStatus = fs.mkdirs(localAttemptDir);
      if (!attemptDirStatus) {
        LOG.warn("localAttemptDir " + localAttemptDir.toString()
            + " couldn't be created.");
      }
      initStatus = initStatus || attemptDirStatus;
    }

    if (!initStatus) {
      throw new IOException("Not able to initialize attempt directories "
          + "in any of the configured local directories for the attempt "
          + attemptId);
    }
  }

  /**
   * Create job log directory and set appropriate permissions for the directory.
   * 
   * @param jobId
   */
  public void initializeJobLogDir(JobID jobId) throws IOException {
    Path jobUserLogDir = new Path(TaskLog.getJobDir(jobId).getCanonicalPath());
    if (!fs.mkdirs(jobUserLogDir)) {
      throw new IOException("Could not create job user log directory: " +
                            jobUserLogDir);
    }
    fs.setPermission(jobUserLogDir, new FsPermission((short)0700));
  }
}
