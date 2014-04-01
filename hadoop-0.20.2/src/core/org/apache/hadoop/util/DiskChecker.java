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

package org.apache.hadoop.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Class that provides utility functions for checking disk problem
 */

public class DiskChecker {
  private static final Log LOG = LogFactory.getLog(DiskChecker.class);

  public static class DiskErrorException extends IOException {
    public DiskErrorException(String msg) {
      super(msg);
    }
  }
    
  public static class DiskOutOfSpaceException extends IOException {
    public DiskOutOfSpaceException(String msg) {
      super(msg);
    }
  }
      
  /** 
   * The semantics of mkdirsWithExistsCheck method is different from the mkdirs
   * method provided in the Sun's java.io.File class in the following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly 
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   * @param dir
   * @return true on success, false on failure
   */
  public static boolean mkdirsWithExistsCheck(File dir) {
    if (dir.mkdir() || dir.exists()) {
      return true;
    }
    File canonDir = null;
    try {
      canonDir = dir.getCanonicalFile();
    } catch (IOException e) {
      return false;
    }
    String parent = canonDir.getParent();
    return (parent != null) && 
           (mkdirsWithExistsCheck(new File(parent)) &&
                                      (canonDir.mkdir() || canonDir.exists()));
  }
  
  /**
   * Create the directory if it doesn't exist and 
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDir(File dir) throws DiskErrorException {
    if (!mkdirsWithExistsCheck(dir))
      throw new DiskErrorException("can not create directory: " 
                                   + dir.toString());
        
    if (!dir.isDirectory())
      throw new DiskErrorException("not a directory: " 
                                   + dir.toString());
            
    if (!dir.canRead())
      throw new DiskErrorException("directory is not readable: " 
                                   + dir.toString());
            
    if (!dir.canWrite())
      throw new DiskErrorException("directory is not writable: " 
                                   + dir.toString());
  }

  /** 
   * Create the directory or check permissions if it already exists.
   * 
   * The semantics of mkdirsWithExistsAndPermissionCheck method is different 
   * from the mkdirs method provided in the Sun's java.io.File class in the 
   * following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly 
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   * @param localFS local filesystem
   * @param dir directory to be created or checked
   * @param expected expected permission
   * @return true on success, false on failure
   */
  public static boolean mkdirsWithExistsAndPermissionCheck(
      LocalFileSystem localFS, Path dir, FsPermission expected) 
  throws IOException {
    File directory = new File(dir.makeQualified(localFS).toUri().getPath());
    if (!directory.exists()) {
      boolean created = mkdirsWithExistsCheck(directory);
      if (created) {
        localFS.setPermission(dir, expected);
        return true;
      } else {
        return false;
      }
    }

    FsPermission actual = localFS.getFileStatus(dir).getPermission();
    if (!actual.equals(expected)) {
      LOG.warn("Incorrect permissions were set on " + dir +
               ", expected: " + expected + ", while actual: " +
               actual + ". Fixing...");
      localFS.setPermission(dir, expected);
    }
    return true;
  }
  
  /**
   * Create the local directory if necessary, check permissions and also ensure 
   * it can be read from and written into.
   * @param localFS local filesystem
   * @param dir directory
   * @param expected permission
   * @throws DiskErrorException
   * @throws IOException
   */
  public static void checkDir(LocalFileSystem localFS, Path dir, 
                              FsPermission expected) 
  throws DiskErrorException, IOException {
    if (!mkdirsWithExistsAndPermissionCheck(localFS, dir, expected))
      throw new DiskErrorException("can not create directory: " 
                                   + dir.toString());

    FileStatus stat = localFS.getFileStatus(dir);
    FsPermission actual = stat.getPermission();
    
    if (!stat.isDir())
      throw new DiskErrorException("not a directory: " 
                                   + dir.toString());
            
    FsAction user = actual.getUserAction();
    if (!user.implies(FsAction.READ))
      throw new DiskErrorException("directory is not readable: " 
                                   + dir.toString());
            
    if (!user.implies(FsAction.WRITE))
      throw new DiskErrorException("directory is not writable: " 
                                   + dir.toString());
  }

}

