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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A utility to manage job submission files.<br/>
 * <b><i>Note that this class is for framework internal usage only and is
 * not to be used by users directly.</i></b>
 */
public class JobSubmissionFiles {

  private final static Log LOG = LogFactory.getLog(JobSubmissionFiles.class);

  // job submission directory is private!
  final public static FsPermission JOB_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx--------
  //job files are world-wide readable and owner writable
  final public static FsPermission JOB_FILE_PERMISSION = 
    FsPermission.createImmutable((short) 0644); // rw-r--r--
  
  public static Path getJobSplitFile(Path jobSubmissionDir) {
    return new Path(jobSubmissionDir, "job.split");
  }

  public static Path getJobSplitMetaFile(Path jobSubmissionDir) {
    return new Path(jobSubmissionDir, "job.splitmetainfo");
  }
  
  /**
   * Get the job conf path.
   */
  public static Path getJobConfPath(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "job.xml");
  }
    
  /**
   * Get the job jar path.
   */
  public static Path getJobJar(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "job.jar");
  }
  
  /**
   * Get the job distributed cache files path.
   * @param jobSubmitDir
   */
  public static Path getJobDistCacheFiles(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "files");
  }
  /**
   * Get the job distributed cache archives path.
   * @param jobSubmitDir 
   */
  public static Path getJobDistCacheArchives(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "archives");
  }
  /**
   * Get the job distributed cache libjars path.
   * @param jobSubmitDir 
   */
  public static Path getJobDistCacheLibjars(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "libjars");
  }

  /**
   * Initializes the staging directory and returns the path. It also
   * keeps track of all necessary ownership & permissions
   * @param client
   * @param conf
   */
  public static Path getStagingDir(JobClient client, Configuration conf) 
  throws IOException, InterruptedException {
    Path stagingArea = client.getStagingAreaDir();
    FileSystem fs = stagingArea.getFileSystem(conf);
    String realUser;
    String currentUser;
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    realUser = ugi.getShortUserName();
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    if (fs.exists(stagingArea)) {
      FileStatus fsStatus = fs.getFileStatus(stagingArea);
      String owner = fsStatus.getOwner();
      if (!(owner.equals(currentUser) || owner.equals(realUser))) {
         throw new IOException("The ownership on the staging directory " +
                      stagingArea + " is not as expected. " + 
                      "It is owned by " + owner + ". The directory must " +
                      "be owned by the submitter " + currentUser + " or " +
                      "by " + realUser);
      }
      if (!fsStatus.getPermission().equals(JOB_DIR_PERMISSION)) {
        LOG.info("Permissions on staging directory " + stagingArea + " are " +
          "incorrect: " + fsStatus.getPermission() + ". Fixing permissions " +
          "to correct value " + JOB_DIR_PERMISSION);
        fs.setPermission(stagingArea, JOB_DIR_PERMISSION);
      }
    } else {
      fs.mkdirs(stagingArea, 
          new FsPermission(JOB_DIR_PERMISSION));
    }
    return stagingArea;
  }
  
}
