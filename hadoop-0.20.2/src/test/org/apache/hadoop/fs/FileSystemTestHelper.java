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
package org.apache.hadoop.fs;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;


/**
 * Helper class for unit tests.
 */
public final class FileSystemTestHelper {
  // The test root is relative to the <wd>/build/test/data by default
  public static final String TEST_ROOT_DIR = 
    System.getProperty("test.build.data", "build/test/data") + "/test";
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private static final int DEFAULT_NUM_BLOCKS = 2;
  private static String absTestRootDir = null;

  /** Hidden constructor */
  private FileSystemTestHelper() {}
  
  public static int getDefaultBlockSize() {
    return DEFAULT_BLOCK_SIZE;
  }
  
  public static byte[] getFileData(int numOfBlocks, long blockSize) {
    byte[] data = new byte[(int) (numOfBlocks * blockSize)];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
    return data;
  }
  
  public static Path getTestRootPath(FileSystem fSys) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR));
  }

  public static Path getTestRootPath(FileSystem fSys, String pathString) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }
  
  
  // the getAbsolutexxx method is needed because the root test dir
  // can be messed up by changing the working dir.

  public static String getAbsoluteTestRootDir(FileSystem fSys)
      throws IOException {
    if (absTestRootDir == null) {
      if (TEST_ROOT_DIR.startsWith("/")) {
        absTestRootDir = TEST_ROOT_DIR;
      } else {
        absTestRootDir = fSys.getWorkingDirectory().toString() + "/"
            + TEST_ROOT_DIR;
      }
    }
    return absTestRootDir;
  }
  
  public static Path getAbsoluteTestRootPath(FileSystem fSys) throws IOException {
    return fSys.makeQualified(new Path(getAbsoluteTestRootDir(fSys)));
  }

  public static Path getDefaultWorkingDirectory(FileSystem fSys)
      throws IOException {
    return getTestRootPath(fSys, "/user/" + System.getProperty("user.name"))
        .makeQualified(fSys.getUri(),
            fSys.getWorkingDirectory());
  }

  /*
   * Create files with numBlocks blocks each with block size blockSize.
   */
  public static void createFile(FileSystem fSys, Path path, int numBlocks,
      int blockSize, boolean createParent) throws IOException {
    FSDataOutputStream out = 
      fSys.create(path, false, 4096, fSys.getDefaultReplication(), blockSize );

    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
  }

  public static void createFile(FileSystem fSys, Path path, int numBlocks,
      int blockSize) throws IOException {
    createFile(fSys, path, numBlocks, blockSize, true);
    }

  public static void createFile(FileSystem fSys, Path path) throws IOException {
    createFile(fSys, path, DEFAULT_NUM_BLOCKS, DEFAULT_BLOCK_SIZE, true);
  }

  public static Path createFile(FileSystem fSys, String name) throws IOException {
    Path path = getTestRootPath(fSys, name);
    createFile(fSys, path);
    return path;
  }

  public static boolean exists(FileSystem fSys, Path p) throws IOException {
    return fSys.exists(p);
  }
  
  public static boolean isFile(FileSystem fSys, Path p) throws IOException {
    try {
      return !fSys.getFileStatus(p).isDir();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public static boolean isDir(FileSystem fSys, Path p) throws IOException {
    try {
      return fSys.getFileStatus(p).isDir();
    } catch (FileNotFoundException e) {
      return false;
    }
  }
  
  
  public static void writeFile(FileSystem fSys, Path path,byte b[])
    throws Exception {
    FSDataOutputStream out = 
      fSys.create(path);
    out.write(b);
    out.close();
  }
  
  public static byte[] readFile(FileSystem fSys, Path path, int len )
    throws Exception {
    DataInputStream dis = fSys.open(path);
    byte[] buffer = new byte[len];
    IOUtils.readFully(dis, buffer, 0, len);
    dis.close();
    return buffer;
  }
  public static FileStatus containsPath(FileSystem fSys, Path path,
      FileStatus[] dirList)
    throws IOException {
    for(int i = 0; i < dirList.length; i ++) { 
      if (getTestRootPath(fSys, path.toString()).equals(
          dirList[i].getPath()))
        return dirList[i];
      }
    return null;
  }
  
  public static FileStatus containsPath(Path path,
      FileStatus[] dirList)
    throws IOException {
    for(int i = 0; i < dirList.length; i ++) { 
      if (path.equals(dirList[i].getPath()))
        return dirList[i];
      }
    return null;
  }
  
  
  public static FileStatus containsPath(FileSystem fSys, String path, FileStatus[] dirList)
     throws IOException {
    return containsPath(fSys, new Path(path), dirList);
  }
  
  public static enum fileType {isDir, isFile, isSymlink};
  public static void checkFileStatus(FileSystem aFs, String path,
      fileType expectedType) throws IOException {
    FileStatus s = aFs.getFileStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDir());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(!s.isDir());
    }
    Assert.assertEquals(aFs.makeQualified(new Path(path)), s.getPath());
  }
}
