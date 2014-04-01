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

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * A collection of tests for the contract of the {@link FileSystem}.
 * This test should be used for general-purpose implementations of
 * {@link FileSystem}, that is, implementations that provide implementations 
 * of all of the functionality of {@link FileSystem}.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fs</code> 
 * {@link FileSystem} instance variable.
 * </p>
 */
public abstract class FileSystemContractBaseTest extends TestCase {
  
  protected FileSystem fs;
  private byte[] data = new byte[getBlockSize() * 2]; // two blocks of data
  {
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
  }
  
  @Override
  protected void tearDown() throws Exception {
    fs.delete(path("/test"), true);
  }
  
  protected int getBlockSize() {
    return 1024;
  }
  
  protected String getDefaultWorkingDirectory() {
    return "/user/" + System.getProperty("user.name");
  }

  protected boolean renameSupported() {
    return true;
  }

  public void testFsStatus() throws Exception {
    FsStatus fsStatus = fs.getStatus();
    assertNotNull(fsStatus);
    //used, free and capacity are non-negative longs
    assertTrue(fsStatus.getUsed() >= 0);
    assertTrue(fsStatus.getRemaining() >= 0);
    assertTrue(fsStatus.getCapacity() >= 0);
  }
  
  public void testWorkingDirectory() throws Exception {

    Path workDir = path(getDefaultWorkingDirectory());
    assertEquals(workDir, fs.getWorkingDirectory());

    fs.setWorkingDirectory(path("."));
    assertEquals(workDir, fs.getWorkingDirectory());

    fs.setWorkingDirectory(path(".."));
    assertEquals(workDir.getParent(), fs.getWorkingDirectory());

    Path relativeDir = path("hadoop");
    fs.setWorkingDirectory(relativeDir);
    assertEquals(relativeDir, fs.getWorkingDirectory());
    
    Path absoluteDir = path("/test/hadoop");
    fs.setWorkingDirectory(absoluteDir);
    assertEquals(absoluteDir, fs.getWorkingDirectory());

  }
  
  public void testMkdirs() throws Exception {
    Path testDir = path("/test/hadoop");
    assertFalse(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));

    assertTrue(fs.mkdirs(testDir));

    assertTrue(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));
    
    assertTrue(fs.mkdirs(testDir));

    assertTrue(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));

    Path parentDir = testDir.getParent();
    assertTrue(fs.exists(parentDir));
    assertFalse(fs.isFile(parentDir));

    Path grandparentDir = parentDir.getParent();
    assertTrue(fs.exists(grandparentDir));
    assertFalse(fs.isFile(grandparentDir));
    
  }
  
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = path("/test/hadoop");
    assertFalse(fs.exists(testDir));
    assertTrue(fs.mkdirs(testDir));
    assertTrue(fs.exists(testDir));
    
    createFile(path("/test/hadoop/file"));
    
    Path testSubDir = path("/test/hadoop/file/subdir");
    try {
      fs.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(fs.exists(testSubDir));
    
    Path testDeepSubDir = path("/test/hadoop/file/deep/sub/dir");
    try {
      fs.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(fs.exists(testDeepSubDir));
    
  }
  
  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fs.getFileStatus(path("/test/hadoop/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  }
  
  public void testListStatusReturnsNullForNonExistentFile() throws Exception {
    assertNull(fs.listStatus(path("/test/hadoop/file")));
  }
  
  public void testListStatus() throws Exception {
    Path[] testDirs = { path("/test/hadoop/a"),
                        path("/test/hadoop/b"),
                        path("/test/hadoop/c/1"), };
    assertFalse(fs.exists(testDirs[0]));

    for (Path path : testDirs) {
      assertTrue(fs.mkdirs(path));
    }

    FileStatus[] paths = fs.listStatus(path("/test"));
    assertEquals(1, paths.length);
    assertEquals(path("/test/hadoop"), paths[0].getPath());

    paths = fs.listStatus(path("/test/hadoop"));
    assertEquals(3, paths.length);
    assertEquals(path("/test/hadoop/a"), paths[0].getPath());
    assertEquals(path("/test/hadoop/b"), paths[1].getPath());
    assertEquals(path("/test/hadoop/c"), paths[2].getPath());

    paths = fs.listStatus(path("/test/hadoop/a"));
    assertEquals(0, paths.length);
  }
  
  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    writeReadAndDelete(0);
  }

  public void testWriteReadAndDeleteHalfABlock() throws Exception {
    writeReadAndDelete(getBlockSize() / 2);
  }

  public void testWriteReadAndDeleteOneBlock() throws Exception {
    writeReadAndDelete(getBlockSize());
  }
  
  public void testWriteReadAndDeleteOneAndAHalfBlocks() throws Exception {
    writeReadAndDelete(getBlockSize() + (getBlockSize() / 2));
  }
  
  public void testWriteReadAndDeleteTwoBlocks() throws Exception {
    writeReadAndDelete(getBlockSize() * 2);
  }
  
  private void writeReadAndDelete(int len) throws IOException {
    Path path = path("/test/hadoop/file");
    
    fs.mkdirs(path.getParent());

    FSDataOutputStream out = fs.create(path, false,
        fs.getConf().getInt("io.file.buffer.size", 4096), 
        (short) 1, getBlockSize());
    out.write(data, 0, len);
    out.close();

    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", len, fs.getFileStatus(path).getLen());

    FSDataInputStream in = fs.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      assertEquals("Position " + i, data[i], buf[i]);
    }
    
    assertTrue("Deleted", fs.delete(path, false));
    
    assertFalse("No longer exists", fs.exists(path));

  }
  
  public void testOverwrite() throws IOException {
    Path path = path("/test/hadoop/file");
    
    fs.mkdirs(path.getParent());

    createFile(path);
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    
    try {
      fs.create(path, false);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = fs.create(path, true);
    out.write(data, 0, data.length);
    out.close();
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    
  }
  
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = path("/test/hadoop/file");
    assertFalse("Parent doesn't exist", fs.exists(path.getParent()));
    createFile(path);
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    assertTrue("Parent exists", fs.exists(path.getParent()));
  }

  public void testDeleteNonExistentFile() throws IOException {
    Path path = path("/test/hadoop/file");    
    assertFalse("Doesn't exist", fs.exists(path));
    assertFalse("No deletion", fs.delete(path, true));
  }
  
  public void testDeleteRecursively() throws IOException {
    Path dir = path("/test/hadoop");
    Path file = path("/test/hadoop/file");
    Path subdir = path("/test/hadoop/subdir");
    
    createFile(file);
    assertTrue("Created subdir", fs.mkdirs(subdir));
    
    assertTrue("File exists", fs.exists(file));
    assertTrue("Dir exists", fs.exists(dir));
    assertTrue("Subdir exists", fs.exists(subdir));
    
    try {
      fs.delete(dir, false);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertTrue("File still exists", fs.exists(file));
    assertTrue("Dir still exists", fs.exists(dir));
    assertTrue("Subdir still exists", fs.exists(subdir));
    
    assertTrue("Deleted", fs.delete(dir, true));
    assertFalse("File doesn't exist", fs.exists(file));
    assertFalse("Dir doesn't exist", fs.exists(dir));
    assertFalse("Subdir doesn't exist", fs.exists(subdir));
  }
  
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = path("/test/hadoop");
    assertTrue(fs.mkdirs(dir));
    assertTrue("Dir exists", fs.exists(dir));
    assertTrue("Deleted", fs.delete(dir, false));
    assertFalse("Dir doesn't exist", fs.exists(dir));
  }
  
  public void testRenameNonExistentPath() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/path");
    Path dst = path("/test/new/newpath");
    rename(src, dst, false, false, false);
  }

  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newfile");
    rename(src, dst, false, true, false);
  }

  public void testRenameFileMoveToExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newfile");
    fs.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
  }

  public void testRenameFileAsExistingFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newfile");
    createFile(dst);
    rename(src, dst, false, true, true);
  }

  public void testRenameFileAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertTrue("Destination changed",
        fs.exists(path("/test/new/newdir/file")));
  }
  
  public void testRenameDirectoryMoveToNonExistentDirectory() 
    throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    Path dst = path("/test/new/newdir");
    rename(src, dst, false, true, false);
  }
  
  public void testRenameDirectoryMoveToExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));
    
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
    
    assertFalse("Nested file1 exists",
        fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(path("/test/new/newdir/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(path("/test/new/newdir/subdir/file2")));
  }
  
  public void testRenameDirectoryAsExistingFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    Path dst = path("/test/new/newfile");
    createFile(dst);
    rename(src, dst, false, true, true);
  }
  
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));
    
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertTrue("Destination changed",
        fs.exists(path("/test/new/newdir/dir")));    
    assertFalse("Nested file1 exists",
        fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(path("/test/new/newdir/dir/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(path("/test/new/newdir/dir/subdir/file2")));
  }

  public void testInputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = path("/test/hadoop/file");
    createFile(src);
    FSDataInputStream in = fs.open(src);
    in.close();
    in.close();
  }
  
  public void testOutputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = path("/test/hadoop/file");
    FSDataOutputStream out = fs.create(src);
    out.writeChar('H'); //write some data
    out.close();
    out.close();
  }
  
  protected Path path(String pathString) {
    return new Path(pathString).makeQualified(fs);
  }
  
  protected void createFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.write(data, 0, data.length);
    out.close();
  }
  
  private void rename(Path src, Path dst, boolean renameSucceeded,
      boolean srcExists, boolean dstExists) throws IOException {
    assertEquals("Rename result", renameSucceeded, fs.rename(src, dst));
    assertEquals("Source exists", srcExists, fs.exists(src));
    assertEquals("Destination exists", dstExists, fs.exists(dst));
  }
}
