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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestFileUtil {
  private static final Log LOG = LogFactory.getLog(TestFileUtil.class);

  final static private File TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "fu");
  private static String FILE = "x";
  private static String LINK = "y";
  private static String DIR = "dir";
  private File del = new File(TEST_DIR, "del");
  private File tmp = new File(TEST_DIR, "tmp");

  /**
   * Creates directories del and tmp for testing.
   * 
   * Contents of them are
   * dir:tmp: 
   *   file: x
   * dir:del:
   *   file: x
   *   dir: dir1 : file:x
   *   dir: dir2 : file:x
   *   link: y to tmp/x
   *   link: tmpDir to tmp
   */
  private void setupDirs() throws IOException {
    Assert.assertFalse(del.exists());
    Assert.assertFalse(tmp.exists());
    del.mkdirs();
    tmp.mkdirs();
    new File(del, FILE).createNewFile();
    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();

    // create directories 
    File one = new File(del, DIR + "1");
    one.mkdirs();
    File two = new File(del, DIR + "2");
    two.mkdirs();
    new File(one, FILE).createNewFile();
    new File(two, FILE).createNewFile();

    // create a symlink to file
    File link = new File(del, LINK);
    FileUtil.symLink(tmpFile.toString(), link.toString());

    // create a symlink to dir
    File linkDir = new File(del, "tmpDir");
    FileUtil.symLink(tmp.toString(), linkDir.toString());
    Assert.assertEquals(5, del.listFiles().length);
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(del);
    FileUtil.fullyDelete(tmp);
  }

  @Test
  public void testFullyDelete() throws IOException {
    setupDirs();
    boolean ret = FileUtil.fullyDelete(del);
    Assert.assertTrue(ret);
    Assert.assertFalse(del.exists());
    validateTmpDir();
  }

  @Test
  public void testFullyDeleteContents() throws IOException {
    setupDirs();
    boolean ret = FileUtil.fullyDeleteContents(del);
    Assert.assertTrue(ret);
    Assert.assertTrue(del.exists());
    Assert.assertEquals(0, del.listFiles().length);
    validateTmpDir();
  }

  private void validateTmpDir() {
    Assert.assertTrue(tmp.exists());
    Assert.assertEquals(1, tmp.listFiles().length);
    Assert.assertTrue(new File(tmp, FILE).exists());
  }

  private File xSubDir = new File(del, "xsubdir");
  private File ySubDir = new File(del, "ysubdir");
  static String file1Name = "file1";
  private File file2 = new File(xSubDir, "file2");
  private File file3 = new File(ySubDir, "file3");
  private File zlink = new File(del, "zlink");
  
  /**
   * Creates a directory which can not be deleted completely.
   * 
   * Directory structure. The naming is important in that {@link MyFile}
   * is used to return them in alphabetical order when listed.
   * 
   *                     del(+w)
   *                       |
   *    .---------------------------------------,
   *    |            |              |           |
   *  file1(!w)   xsubdir(-w)   ysubdir(+w)   zlink
   *                 |              |
   *               file2          file3
   *
   * @throws IOException
   */
  private void setupDirsAndNonWritablePermissions() throws IOException {
    Assert.assertFalse("The directory del should not have existed!",
        del.exists());
    del.mkdirs();
    new MyFile(del, file1Name).createNewFile();

    // "file1" is non-deletable by default, see MyFile.delete().

    xSubDir.mkdirs();
    file2.createNewFile();
    xSubDir.setWritable(false);
    ySubDir.mkdirs();
    file3.createNewFile();

    Assert.assertFalse("The directory tmp should not have existed!",
        tmp.exists());
    tmp.mkdirs();
    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();
    FileUtil.symLink(tmpFile.toString(), zlink.toString());
  }
  
  // Validates the return value.
  // Validates the existence of directory "xsubdir" and the file "file1"
  // Sets writable permissions for the non-deleted dir "xsubdir" so that it can
  // be deleted in tearDown().
  private void validateAndSetWritablePermissions(boolean ret) {
    xSubDir.setWritable(true);
    Assert.assertFalse("The return value should have been false!", ret);
    Assert.assertTrue("The file file1 should not have been deleted!",
        new File(del, file1Name).exists());
    Assert.assertTrue(
        "The directory xsubdir should not have been deleted!",
        xSubDir.exists());
    Assert.assertTrue("The file file2 should not have been deleted!",
        file2.exists());
    Assert.assertFalse("The directory ysubdir should have been deleted!",
        ySubDir.exists());
    Assert.assertFalse("The link zlink should have been deleted!",
        zlink.exists());
  }

  @Test
  public void testFailFullyDelete() throws IOException {
    LOG.info("Running test to verify failure of fullyDelete()");
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDelete(new MyFile(del));
    validateAndSetWritablePermissions(ret);
  }

  /**
   * Extend {@link File}. Same as {@link File} except for two things: (1) This
   * treats file1Name as a very special file which is not delete-able
   * irrespective of it's parent-dir's permissions, a peculiar file instance for
   * testing. (2) It returns the files in alphabetically sorted order when
   * listed.
   * 
   */
  public static class MyFile extends File {

    private static final long serialVersionUID = 1L;

    public MyFile(File f) {
      super(f.getAbsolutePath());
    }

    public MyFile(File parent, String child) {
      super(parent, child);
    }

    /**
     * Same as {@link File#delete()} except for file1Name which will never be
     * deleted (hard-coded)
     */
    @Override
    public boolean delete() {
      LOG.info("Trying to delete myFile " + getAbsolutePath());
      boolean bool = false;
      if (getName().equals(file1Name)) {
        bool = false;
      } else {
        bool = super.delete();
      }
      if (bool) {
        LOG.info("Deleted " + getAbsolutePath() + " successfully");
      } else {
        LOG.info("Cannot delete " + getAbsolutePath());
      }
      return bool;
    }

    /**
     * Return the list of files in an alphabetically sorted order
     */
    @Override
    public File[] listFiles() {
      File[] files = super.listFiles();
      List<File> filesList = Arrays.asList(files);
      Collections.sort(filesList);
      File[] myFiles = new MyFile[files.length];
      int i=0;
      for(File f : filesList) {
        myFiles[i++] = new MyFile(f);
      }
      return myFiles;
    }
  }

  @Test
  public void testFailFullyDeleteContents() throws IOException {
    LOG.info("Running test to verify failure of fullyDeleteContents()");
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDeleteContents(new MyFile(del));
    validateAndSetWritablePermissions(ret);
  }
  
  @Test
  public void testListFiles() throws IOException {
    setupDirs();
    //Test existing files case 
    File[] files = FileUtil.listFiles(tmp);
    Assert.assertEquals(1, files.length);

    //Test existing directory with no files case 
    File newDir = new File(tmp.getPath(),"test");
    newDir.mkdir();
    Assert.assertTrue("Failed to create test dir", newDir.exists());
    files = FileUtil.listFiles(newDir);
    Assert.assertEquals(0, files.length);
    newDir.delete();
    Assert.assertFalse("Failed to delete test dir", newDir.exists());
    
    //Test non-existing directory case, this throws 
    //IOException
    try {
      files = FileUtil.listFiles(newDir);
      Assert.fail("IOException expected on listFiles() for non-existent dir "
          + newDir.toString());
    } catch(IOException ioe) {
      //Expected an IOException
    }
  }

  @Test
  public void testListAPI() throws IOException {
    setupDirs();
    //Test existing files case 
    String[] files = FileUtil.list(tmp);
    Assert.assertEquals(1, files.length);

    //Test existing directory with no files case 
    File newDir = new File(tmp.getPath(),"test");
    newDir.mkdir();
    Assert.assertTrue("Failed to create test dir", newDir.exists());
    files = FileUtil.list(newDir);
    Assert.assertEquals(0, files.length);
    newDir.delete();
    Assert.assertFalse("Failed to delete test dir", newDir.exists());
    
    //Test non-existing directory case, this throws 
    //IOException
    try {
      files = FileUtil.list(newDir);
      Assert.fail("IOException expected on list() for non-existent dir "
          + newDir.toString());
    } catch(IOException ioe) {
      //Expected an IOException
    }
  }
}
