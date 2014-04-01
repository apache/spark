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



import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.FileSystemTestHelper.*;

/**
 * <p>
 * A collection of tests for the {@link FileSystem}.
 * This test should be used for testing an instance of FileSystem
 *  that has been initialized to a specific default FileSystem such a
 *  LocalFileSystem, HDFS,S3, etc.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc</code> 
 * {@link FileSystem} instance variable.
 * 
 * Since this a junit 4 you can also do a single setup before 
 * the start of any tests.
 * E.g.
 *     @BeforeClass   public static void clusterSetupAtBegining()
 *     @AfterClass    public static void ClusterShutdownAtEnd()
 * </p>
 */
public abstract class FSMainOperationsBaseTest  {
  
  private static String TEST_DIR_AAA2 = "test/hadoop2/aaa";
  private static String TEST_DIR_AAA = "test/hadoop/aaa";
  private static String TEST_DIR_AXA = "test/hadoop/axa";
  private static String TEST_DIR_AXX = "test/hadoop/axx";
  private static int numBlocks = 2;
  
  static  final String LOCAL_FS_ROOT_URI = "file:///tmp/test";
  
  
  protected static FileSystem fSys;
  
  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
    public boolean accept(final Path file) {
      return true;
    }
  };

  //A test filter with returns any path containing a "b" 
  final private static PathFilter TEST_X_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      if(file.getName().contains("x") || file.toString().contains("X"))
        return true;
      else
        return false;
    }     
  };
  
  private static byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());
  
  @Before
  public void setUp() throws Exception {
    fSys.mkdirs(getTestRootPath(fSys, "test"));
  }
  
  @After
  public void tearDown() throws Exception {
    fSys.delete(new Path(getAbsoluteTestRootPath(fSys), new Path("test")), true);
    fSys.delete(new Path(LOCAL_FS_ROOT_URI), true);
  }
  
  
  protected Path getDefaultWorkingDirectory() throws IOException {
    return getTestRootPath(fSys,
        "/user/" + System.getProperty("user.name")).makeQualified(
        fSys.getUri(), fSys.getWorkingDirectory());
  }

  protected boolean renameSupported() {
    return true;
  }

  
  protected IOException unwrapException(IOException e) {
    return e;
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    // First we cd to our test root
    Path workDir = new Path(getAbsoluteTestRootPath(fSys), new Path("test"));
    fSys.setWorkingDirectory(workDir);
    Assert.assertEquals(workDir, fSys.getWorkingDirectory());

    fSys.setWorkingDirectory(new Path("."));
    Assert.assertEquals(workDir, fSys.getWorkingDirectory());

    fSys.setWorkingDirectory(new Path(".."));
    Assert.assertEquals(workDir.getParent(), fSys.getWorkingDirectory());
    
    // cd using a relative path

    // Go back to our test root
    workDir = new Path(getAbsoluteTestRootPath(fSys), new Path("test"));
    fSys.setWorkingDirectory(workDir);
    Assert.assertEquals(workDir, fSys.getWorkingDirectory());
    
    Path relativeDir = new Path("existingDir1");
    Path absoluteDir = new Path(workDir,"existingDir1");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(relativeDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());
    // cd using a absolute path
    absoluteDir = getTestRootPath(fSys, "test/existingDir2");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());
    
    // Now open a file relative to the wd we just set above.
    Path absolutePath = new Path(absoluteDir, "foo");
    FileSystemTestHelper.createFile(fSys, absolutePath);
    fSys.open(new Path("foo")).close();
    
    
    // Now mkdir relative to the dir we cd'ed to
    fSys.mkdirs(new Path("newDir"));
    Assert.assertTrue(isDir(fSys, new Path(absoluteDir, "newDir")));

    /**
     * We cannot test this because FileSystem has never checked for
     * existence of working dir - fixing  this would break compatibility,
     * 
    absoluteDir = getTestRootPath(fc, "nonexistingPath");
    try {
      fc.setWorkingDirectory(absoluteDir);
      Assert.fail("cd to non existing dir should have failed");
    } catch (Exception e) {
      // Exception as expected
    }
    */
    
    // Try a URI

    absoluteDir = new Path(LOCAL_FS_ROOT_URI + "/existingDir");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());

  }
  
  @Test
  public void testMkdirs() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assert.assertFalse(exists(fSys, testDir));
    Assert.assertFalse(isFile(fSys, testDir));

    fSys.mkdirs(testDir);

    Assert.assertTrue(exists(fSys, testDir));
    Assert.assertFalse(isFile(fSys, testDir));
    
    fSys.mkdirs(testDir);

    Assert.assertTrue(exists(fSys, testDir));
    Assert.assertFalse(isFile(fSys, testDir));

    Path parentDir = testDir.getParent();
    Assert.assertTrue(exists(fSys, parentDir));
    Assert.assertFalse(isFile(fSys, parentDir));

    Path grandparentDir = parentDir.getParent();
    Assert.assertTrue(exists(fSys, grandparentDir));
    Assert.assertFalse(isFile(fSys, grandparentDir));
    
  }
  
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assert.assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    Assert.assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");
    Assert.assertFalse(fSys.mkdirs(testSubDir));
    Assert.assertFalse(exists(fSys, testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    Assert.assertFalse(exists(fSys, testSubDir));
    Assert.assertFalse(fSys.mkdirs(testDeepSubDir));
    Assert.assertFalse(exists(fSys, testDeepSubDir));
    
  }
  
  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fSys.getFileStatus(getTestRootPath(fSys, "test/hadoop/file"));
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  } 
  
  public void testListStatusThrowsExceptionForNonExistentFile()
                                                    throws Exception {
    try {
      fSys.listStatus(getTestRootPath(fSys, "test/hadoop/file"));
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }
  
  @Test
  public void testListStatus() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, "test/hadoop/a"),
        getTestRootPath(fSys, "test/hadoop/b"),
        getTestRootPath(fSys, "test/hadoop/c/1"), };
    Assert.assertFalse(exists(fSys, testDirs[0]));

    for (Path path : testDirs) {
      fSys.mkdirs(path);
    }

    // test listStatus that returns an array
    FileStatus[] paths = fSys.listStatus(getTestRootPath(fSys, "test"));
    Assert.assertEquals(1, paths.length);
    Assert.assertEquals(getTestRootPath(fSys, "test/hadoop"), paths[0].getPath());

    paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop"));
    Assert.assertEquals(3, paths.length);

    Assert.assertTrue(containsPath(getTestRootPath(fSys, "test/hadoop/a"),
        paths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, "test/hadoop/b"),
        paths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, "test/hadoop/c"),
        paths));

    paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop/a"));
    Assert.assertEquals(0, paths.length);
    
  }
  
  @Test
  public void testListStatusFilterWithNoMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA2),
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX), };
    
   if (exists(fSys, testDirs[0]) == false) {
     for (Path path : testDirs) {
       fSys.mkdirs(path);
     }
   }

    // listStatus with filters returns empty correctly
    FileStatus[] filteredPaths = fSys.listStatus(
        getTestRootPath(fSys, "test"), TEST_X_FILTER);
    Assert.assertEquals(0,filteredPaths.length);
    
  }
  
  public void testListStatusFilterWithSomeMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }

    // should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
    FileStatus[] filteredPaths = fSys.listStatus(
        getTestRootPath(fSys, "test/hadoop"), TEST_X_FILTER);
    Assert.assertEquals(2,filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusWithNoMatchesInPath() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }

    // should return nothing
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/?"));
    Assert.assertEquals(0, paths.length);
  }
  
  @Test
  public void testGlobStatusSomeMatchesInDirectories() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    // Should return two items ("/test/hadoop" and "/test/hadoop2")
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop*"));
    Assert.assertEquals(2, paths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        "test/hadoop"), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        "test/hadoop2"), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleWildCardMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }

    //Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
    //"/test/hadoop/axx", and "/test/hadoop2/axx")
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop*/*"));
    Assert.assertEquals(4, paths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AAA), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AXA), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AXX), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AAA2), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleMatchesOfSingleChar() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/ax?"));
    Assert.assertEquals(2, paths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXA), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXX), paths));
  }
  
  @Test
  public void testGlobStatusFilterWithEmptyPathResults() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/?"),
        DEFAULT_FILTER);
    Assert.assertEquals(0,filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/*"),
        DEFAULT_FILTER);
    Assert.assertEquals(3, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AAA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/a??"),
        DEFAULT_FILTER);
    Assert.assertEquals(3, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AAA),
        filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AXA),
        filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AXX),
        filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/*"),
        TEST_X_FILTER);
    Assert.assertEquals(2, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/?"),
        TEST_X_FILTER);
    Assert.assertEquals(0,filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/a??"),
        TEST_X_FILTER);
    Assert.assertEquals(2, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AXA),
        filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fSys, TEST_DIR_AXX),
        filteredPaths));
  }
  
  @Test
  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    writeReadAndDelete(0);
  }

  @Test
  public void testWriteReadAndDeleteHalfABlock() throws Exception {
    writeReadAndDelete(getDefaultBlockSize() / 2);
  }

  @Test
  public void testWriteReadAndDeleteOneBlock() throws Exception {
    writeReadAndDelete(getDefaultBlockSize());
  }
  
  @Test
  public void testWriteReadAndDeleteOneAndAHalfBlocks() throws Exception {
    int blockSize = getDefaultBlockSize();
    writeReadAndDelete(blockSize + (blockSize / 2));
  }
  
  @Test
  public void testWriteReadAndDeleteTwoBlocks() throws Exception {
    writeReadAndDelete(getDefaultBlockSize() * 2);
  }
  
  private void writeReadAndDelete(int len) throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    
    fSys.mkdirs(path.getParent());

   
    FSDataOutputStream out = 
      fSys.create(path, false, 4096, (short) 1, getDefaultBlockSize() );
    out.write(data, 0, len);
    out.close();

    Assert.assertTrue("Exists", exists(fSys, path));
    Assert.assertEquals("Length", len, fSys.getFileStatus(path).getLen());

    FSDataInputStream in = fSys.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    Assert.assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      Assert.assertEquals("Position " + i, data[i], buf[i]);
    }
    
    Assert.assertTrue("Deleted", fSys.delete(path, false));
    
    Assert.assertFalse("No longer exists", exists(fSys, path));

  }
  
  @Test
  public void testOverwrite() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    
    fSys.mkdirs(path.getParent());

    createFile(path);
    
    Assert.assertTrue("Exists", exists(fSys, path));
    Assert.assertEquals("Length", data.length, fSys.getFileStatus(path).getLen());
    
    try {
      createFile(path);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = fSys.create(path, true, 4096);
    out.write(data, 0, data.length);
    out.close();
    
    Assert.assertTrue("Exists", exists(fSys, path));
    Assert.assertEquals("Length", data.length, fSys.getFileStatus(path).getLen());
    
  }
  
  @Test
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    Assert.assertFalse("Parent doesn't exist", exists(fSys, path.getParent()));
    createFile(path);
    
    Assert.assertTrue("Exists", exists(fSys, path));
    Assert.assertEquals("Length", data.length, fSys.getFileStatus(path).getLen());
    Assert.assertTrue("Parent exists", exists(fSys, path.getParent()));
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");    
    Assert.assertFalse("Doesn't exist", exists(fSys, path));
    Assert.assertFalse("No deletion", fSys.delete(path, true));
  }
  
  @Test
  public void testDeleteRecursively() throws IOException {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    Path file = getTestRootPath(fSys, "test/hadoop/file");
    Path subdir = getTestRootPath(fSys, "test/hadoop/subdir");
    
    createFile(file);
    fSys.mkdirs(subdir);
    
    Assert.assertTrue("File exists", exists(fSys, file));
    Assert.assertTrue("Dir exists", exists(fSys, dir));
    Assert.assertTrue("Subdir exists", exists(fSys, subdir));
    
    try {
      fSys.delete(dir, false);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertTrue("File still exists", exists(fSys, file));
    Assert.assertTrue("Dir still exists", exists(fSys, dir));
    Assert.assertTrue("Subdir still exists", exists(fSys, subdir));
    
    Assert.assertTrue("Deleted", fSys.delete(dir, true));
    Assert.assertFalse("File doesn't exist", exists(fSys, file));
    Assert.assertFalse("Dir doesn't exist", exists(fSys, dir));
    Assert.assertFalse("Subdir doesn't exist", exists(fSys, subdir));
  }
  
  @Test
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    fSys.mkdirs(dir);
    Assert.assertTrue("Dir exists", exists(fSys, dir));
    Assert.assertTrue("Deleted", fSys.delete(dir, false));
    Assert.assertFalse("Dir doesn't exist", exists(fSys, dir));
  }
  
  @Test
  public void testInputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    FSDataInputStream in = fSys.open(src);
    in.close();
    in.close();
  }
  
  @Test
  public void testOutputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    FSDataOutputStream out = fSys.create(src);
    
    out.writeChar('H'); //write some data
    out.close();
    out.close();
  }

  
  protected void createFile(Path path) throws IOException {
    FileSystemTestHelper.createFile(fSys, path);
  }

  private boolean containsPath(Path path, FileStatus[] filteredPaths)
    throws IOException {
    for(int i = 0; i < filteredPaths.length; i ++) { 
      if (getTestRootPath(fSys, path.toString()).equals(
          filteredPaths[i].getPath()))
        return true;
      }
    return false;
 }
}
