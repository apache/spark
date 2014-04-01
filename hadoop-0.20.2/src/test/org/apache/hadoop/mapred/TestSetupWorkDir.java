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
import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * Verifies if TaskRunner.SetupWorkDir() is cleaning up files/dirs pointed
 * to by symlinks under work dir.
 */
public class TestSetupWorkDir extends TestCase {

  /**
   * Creates 1 subdirectory and 1 file under dir2. Creates 1 subdir, 1 file,
   * 1 symlink to a dir and a symlink to a file under dir1.
   * Creates dir1/subDir, dir1/file, dir2/subDir, dir2/file,
   * dir1/symlinkSubDir->dir2/subDir, dir1/symlinkFile->dir2/file.
   */
  static void createSubDirsAndSymLinks(JobConf jobConf, Path dir1, Path dir2)
       throws IOException {
    FileSystem fs = FileSystem.getLocal(jobConf);
    createSubDirAndFile(fs, dir1);
    createSubDirAndFile(fs, dir2);
    // now create symlinks under dir1 that point to file/dir under dir2
    FileUtil.symLink(dir2+"/subDir", dir1+"/symlinkSubDir");
    FileUtil.symLink(dir2+"/file", dir1+"/symlinkFile");
  }

  static void createSubDirAndFile(FileSystem fs, Path dir) throws IOException {
    Path subDir = new Path(dir, "subDir");
    fs.mkdirs(subDir);
    Path p = new Path(dir, "file");
    DataOutputStream out = fs.create(p);
    out.writeBytes("dummy input");
    out.close();    
  }

  void createEmptyDir(FileSystem fs, Path dir) throws IOException {
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }
    if (!fs.mkdirs(dir)) {
      throw new IOException("Unable to create directory " + dir);
    }
  }

  /**
   * Validates if TaskRunner.setupWorkDir() is properly cleaning up the
   * contents of workDir and creating tmp dir under it (even though workDir
   * contains symlinks to files/directories).
   */
  public void testSetupWorkDir() throws IOException {
    Path rootDir = new Path(System.getProperty("test.build.data",  "/tmp"),
                            "testSetupWorkDir");
    Path myWorkDir = new Path(rootDir, "./work");
    Path myTargetDir = new Path(rootDir, "./tmp");
    JobConf jConf = new JobConf();
    FileSystem fs = FileSystem.getLocal(jConf);
    createEmptyDir(fs, myWorkDir);
    createEmptyDir(fs, myTargetDir);

    // create subDirs and symlinks under work dir
    createSubDirsAndSymLinks(jConf, myWorkDir, myTargetDir);

    assertTrue("Did not create symlinks/files/dirs properly. Check "
        + myWorkDir + " and " + myTargetDir,
        (fs.listStatus(myWorkDir).length == 4) &&
        (fs.listStatus(myTargetDir).length == 2));

    // let us disable creation of symlinks in setupWorkDir()
    jConf.set("mapred.create.symlink", "no");

    // Deletion of myWorkDir should not affect contents of myTargetDir.
    // myTargetDir is like $user/jobcache/distcache
    TaskRunner.setupWorkDir(jConf, new File(myWorkDir.toUri().getPath()));

    // Contents of myWorkDir should be cleaned up and a tmp dir should be
    // created under myWorkDir
    assertTrue(myWorkDir + " is not cleaned up properly.",
        fs.exists(myWorkDir) && (fs.listStatus(myWorkDir).length == 1));

    // Make sure that the dir under myWorkDir is tmp
    assertTrue(fs.listStatus(myWorkDir)[0].getPath().toUri().getPath()
               .toString().equals(myWorkDir.toString() + "/tmp"));

    // Make sure that myTargetDir is not changed/deleted
    assertTrue("Dir " + myTargetDir + " seem to be modified.",
        fs.exists(myTargetDir) && (fs.listStatus(myTargetDir).length == 2));

    // cleanup
    fs.delete(rootDir, true);
  }
}
