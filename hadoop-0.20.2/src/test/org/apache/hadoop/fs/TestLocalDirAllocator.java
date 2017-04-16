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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import junit.framework.TestCase;

/** This test LocalDirAllocator works correctly;
 * Every test case uses different buffer dirs to 
 * enforce the AllocatorPerContext initialization.
 * This test does not run on Cygwin because under Cygwin
 * a directory can be created in a read-only directory
 * which breaks this test.
 */ 
public class TestLocalDirAllocator extends TestCase {
  final static private Configuration conf = new Configuration();
  final static private String BUFFER_DIR_ROOT = "build/test/temp";
  final static private Path BUFFER_PATH_ROOT = new Path(BUFFER_DIR_ROOT);
  final static private File BUFFER_ROOT = new File(BUFFER_DIR_ROOT);
  final static private String BUFFER_DIR[] = new String[] {
    BUFFER_DIR_ROOT+"/tmp0",  BUFFER_DIR_ROOT+"/tmp1", BUFFER_DIR_ROOT+"/tmp2",
    BUFFER_DIR_ROOT+"/tmp3", BUFFER_DIR_ROOT+"/tmp4", BUFFER_DIR_ROOT+"/tmp5",
    BUFFER_DIR_ROOT+"/tmp6"};
  final static private Path BUFFER_PATH[] = new Path[] {
    new Path(BUFFER_DIR[0]), new Path(BUFFER_DIR[1]), new Path(BUFFER_DIR[2]),
    new Path(BUFFER_DIR[3]), new Path(BUFFER_DIR[4]), new Path(BUFFER_DIR[5]),
    new Path(BUFFER_DIR[6])};
  final static private String CONTEXT = "dfs.client.buffer.dir";
  final static private String FILENAME = "block";
  final static private LocalDirAllocator dirAllocator = 
    new LocalDirAllocator(CONTEXT);
  static LocalFileSystem localFs;
  final static private boolean isWindows =
    System.getProperty("os.name").startsWith("Windows");
  final static int SMALL_FILE_SIZE = 100;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
      rmBufferDirs();
    } catch(IOException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private static void rmBufferDirs() throws IOException {
    assertTrue(!localFs.exists(BUFFER_PATH_ROOT) ||
        localFs.delete(BUFFER_PATH_ROOT));
  }
  
  private void validateTempDirCreation(int i) throws IOException {
    File result = createTempFile(SMALL_FILE_SIZE);
    assertTrue("Checking for " + BUFFER_DIR[i] + " in " + result + " - FAILED!", 
        result.getPath().startsWith(new File(BUFFER_DIR[i], FILENAME).getPath()));
  }
  
  private File createTempFile() throws IOException {
    File result = dirAllocator.createTmpFileForWrite(FILENAME, -1, conf);
    result.delete();
    return result;
  }
  
  private File createTempFile(long size) throws IOException {
    File result = dirAllocator.createTmpFileForWrite(FILENAME, size, conf);
    result.delete();
    return result;
  }
  
  /** Two buffer dirs. The first dir does not exist & is on a read-only disk; 
   * The second dir exists & is RW
   * @throws Exception
   */
  public void test0() throws Exception {
    if (isWindows) return;
    try {
      conf.set(CONTEXT, BUFFER_DIR[0]+","+BUFFER_DIR[1]);
      assertTrue(localFs.mkdirs(BUFFER_PATH[1]));
      BUFFER_ROOT.setReadOnly();
      validateTempDirCreation(1);
      validateTempDirCreation(1);
    } finally {
      Shell.execCommand(new String[]{"chmod", "u+w", BUFFER_DIR_ROOT});
      rmBufferDirs();
    }
  }
    
  /** Two buffer dirs. The first dir exists & is on a read-only disk; 
   * The second dir exists & is RW
   * @throws Exception
   */
  public void test1() throws Exception {
    if (isWindows) return;
    try {
      conf.set(CONTEXT, BUFFER_DIR[1]+","+BUFFER_DIR[2]);
      assertTrue(localFs.mkdirs(BUFFER_PATH[2]));
      BUFFER_ROOT.setReadOnly();
      validateTempDirCreation(2);
      validateTempDirCreation(2);
    } finally {
      Shell.execCommand(new String[]{"chmod", "u+w", BUFFER_DIR_ROOT});
      rmBufferDirs();
    }
  }
  /** Two buffer dirs. Both do not exist but on a RW disk.
   * Check if tmp dirs are allocated in a round-robin 
   */
  public void test2() throws Exception {
    if (isWindows) return;
    try {
      conf.set(CONTEXT, BUFFER_DIR[2]+","+BUFFER_DIR[3]);

      // create the first file, and then figure the round-robin sequence
      createTempFile(SMALL_FILE_SIZE);
      int firstDirIdx = (dirAllocator.getCurrentDirectoryIndex() == 0) ? 2 : 3;
      int secondDirIdx = (firstDirIdx == 2) ? 3 : 2;
      
      // check if tmp dirs are allocated in a round-robin manner
      validateTempDirCreation(firstDirIdx);
      validateTempDirCreation(secondDirIdx);
      validateTempDirCreation(firstDirIdx);
    } finally {
      rmBufferDirs();
    }
  }

  /** Two buffer dirs. Both exists and on a R/W disk. 
   * Later disk1 becomes read-only.
   * @throws Exception
   */
  public void test3() throws Exception {
    if (isWindows) return;
    try {
      conf.set(CONTEXT, BUFFER_DIR[3]+","+BUFFER_DIR[4]);
      assertTrue(localFs.mkdirs(BUFFER_PATH[3]));
      assertTrue(localFs.mkdirs(BUFFER_PATH[4]));
      
      // create the first file with size, and then figure the round-robin sequence
      createTempFile(SMALL_FILE_SIZE);

      int nextDirIdx = (dirAllocator.getCurrentDirectoryIndex() == 0) ? 3 : 4;
      validateTempDirCreation(nextDirIdx);

      // change buffer directory 2 to be read only
      new File(BUFFER_DIR[4]).setReadOnly();
      validateTempDirCreation(3);
      validateTempDirCreation(3);
    } finally {
      rmBufferDirs();
    }
  }
  
  /**
   * Two buffer dirs, on read-write disk.
   * 
   * Try to create a whole bunch of files.
   *  Verify that they do indeed all get created where they should.
   *  
   *  Would ideally check statistical properties of distribution, but
   *  we don't have the nerve to risk false-positives here.
   * 
   * @throws Exception
   */
  static final int TRIALS = 100;
  public void test4() throws Exception {
    if (isWindows) return;
    try {

      conf.set(CONTEXT, BUFFER_DIR[5]+","+BUFFER_DIR[6]);
      assertTrue(localFs.mkdirs(BUFFER_PATH[5]));
      assertTrue(localFs.mkdirs(BUFFER_PATH[6]));
        
      int inDir5=0, inDir6=0;
      for(int i = 0; i < TRIALS; ++i) {
        File result = createTempFile();
        if(result.getPath().startsWith(new File(BUFFER_DIR[5], FILENAME).getPath())) {
          inDir5++;
        } else  if(result.getPath().startsWith(new File(BUFFER_DIR[6], FILENAME).getPath())) {
          inDir6++;
        }
        result.delete();
      }
      
      assertTrue( inDir5 + inDir6 == TRIALS);
        
    } finally {
      rmBufferDirs();
    }
  }
  
}
