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
package org.apache.hadoop.mapreduce.util;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.MRAsyncDiskService;

/**
 * A test for MRAsyncDiskService.
 */
public class TestMRAsyncDiskService extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestMRAsyncDiskService.class);
  
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString();
  
  @Override
  protected void setUp() throws Exception {
    FileUtil.fullyDelete(new File(TEST_ROOT_DIR));
  }

  /**
   * Given 'pathname', compute an equivalent path relative to the cwd.
   * @param pathname the path to a directory.
   * @return the path to that same directory, relative to ${user.dir}.
   */
  private String relativeToWorking(String pathname) {
    String cwd = System.getProperty("user.dir", "/");

    // normalize pathname and cwd into full directory paths.
    pathname = (new Path(pathname)).toUri().getPath();
    cwd = (new Path(cwd)).toUri().getPath();

    String [] cwdParts = cwd.split(File.separator);
    String [] pathParts = pathname.split(File.separator);

    // There are three possible cases:
    // 1) pathname and cwd are equal. Return '.'
    // 2) pathname is under cwd. Return the components that are under it.
    //     e.g., cwd = /a/b, path = /a/b/c, return 'c'
    // 3) pathname is outside of cwd. Find the common components, if any,
    //    and subtract them from the returned path, then return enough '..'
    //    components to "undo" the non-common components of cwd, then all 
    //    the remaining parts of pathname.
    //    e.g., cwd = /a/b, path = /a/c, return '../c'

    if (cwd.equals(pathname)) {
      LOG.info("relative to working: " + pathname + " -> .");
      return "."; // They match exactly.
    }

    // Determine how many path components are in common between cwd and path.
    int common = 0;
    for (int i = 0; i < Math.min(cwdParts.length, pathParts.length); i++) {
      if (cwdParts[i].equals(pathParts[i])) {
        common++;
      } else {
        break;
      }
    }

    // output path stringbuilder.
    StringBuilder sb = new StringBuilder();

    // For everything in cwd that isn't in pathname, add a '..' to undo it.
    int parentDirsRequired = cwdParts.length - common;
    for (int i = 0; i < parentDirsRequired; i++) {
      sb.append("..");
      sb.append(File.separator);
    }

    // Then append all non-common parts of 'pathname' itself.
    for (int i = common; i < pathParts.length; i++) {
      sb.append(pathParts[i]);
      sb.append(File.separator);
    }

    // Don't end with a '/'.
    String s = sb.toString();
    if (s.endsWith(File.separator)) {
      s = s.substring(0, s.length() - 1);
    }

    LOG.info("relative to working: " + pathname + " -> " + s);
    return s;
  }

  /** Test that the relativeToWorking() method above does what we expect. */
  public void testRelativeToWorking() {
    assertEquals(".", relativeToWorking(System.getProperty("user.dir", ".")));

    String cwd = System.getProperty("user.dir", ".");
    Path cwdPath = new Path(cwd);

    Path subdir = new Path(cwdPath, "foo");
    assertEquals("foo", relativeToWorking(subdir.toUri().getPath()));

    Path subsubdir = new Path(subdir, "bar");
    assertEquals("foo/bar", relativeToWorking(subsubdir.toUri().getPath()));

    Path parent = new Path(cwdPath, "..");
    assertEquals("..", relativeToWorking(parent.toUri().getPath()));

    Path sideways = new Path(parent, "baz");
    assertEquals("../baz", relativeToWorking(sideways.toUri().getPath()));
  }


  /** Test that volumes specified as relative paths are handled properly
   * by MRAsyncDiskService (MAPREDUCE-1887).
   */
  public void testVolumeNormalization() throws Throwable {
    LOG.info("TEST_ROOT_DIR is " + TEST_ROOT_DIR);

    String relativeTestRoot = relativeToWorking(TEST_ROOT_DIR);

    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String [] vols = new String[] { relativeTestRoot + "/0",
        relativeTestRoot + "/1" };

    // Put a file in one of the volumes to be cleared on startup.
    Path delDir = new Path(vols[0], MRAsyncDiskService.TOBEDELETED);
    localFileSystem.mkdirs(delDir);
    localFileSystem.create(new Path(delDir, "foo")).close();

    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);
    makeSureCleanedUp(vols, service);
  }

  /**
   * This test creates some directories and then removes them through 
   * MRAsyncDiskService. 
   */
  public void testMRAsyncDiskService() throws Throwable {
  
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};
    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);
    
    String a = "a";
    String b = "b";
    String c = "b/c";
    String d = "d";
    
    File fa = new File(vols[0], a);
    File fb = new File(vols[1], b);
    File fc = new File(vols[1], c);
    File fd = new File(vols[1], d);
    
    // Create the directories
    fa.mkdirs();
    fb.mkdirs();
    fc.mkdirs();
    fd.mkdirs();
    
    assertTrue(fa.exists());
    assertTrue(fb.exists());
    assertTrue(fc.exists());
    assertTrue(fd.exists());
    
    // Move and delete them
    service.moveAndDeleteRelativePath(vols[0], a);
    assertFalse(fa.exists());
    service.moveAndDeleteRelativePath(vols[1], b);
    assertFalse(fb.exists());
    assertFalse(fc.exists());
    
    assertFalse(service.moveAndDeleteRelativePath(vols[1], "not_exists"));
    
    // asyncDiskService is NOT able to delete files outside all volumes.
    IOException ee = null;
    try {
      service.moveAndDeleteAbsolutePath(TEST_ROOT_DIR + "/2");
    } catch (IOException e) {
      ee = e;
    }
    assertNotNull("asyncDiskService should not be able to delete files "
        + "outside all volumes", ee);
    // asyncDiskService is able to automatically find the file in one
    // of the volumes.
    assertTrue(service.moveAndDeleteAbsolutePath(vols[1] + Path.SEPARATOR_CHAR + d));
    
    // Make sure everything is cleaned up
    makeSureCleanedUp(vols, service);
  }

  /**
   * This test creates some directories inside the volume roots, and then 
   * call asyncDiskService.MoveAndDeleteAllVolumes.
   * We should be able to delete all files/dirs inside the volumes except
   * the toBeDeleted directory.
   */
  public void testMRAsyncDiskServiceMoveAndDeleteAllVolumes() throws Throwable {
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};
    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);

    String a = "a";
    String b = "b";
    String c = "b/c";
    String d = "d";
    
    File fa = new File(vols[0], a);
    File fb = new File(vols[1], b);
    File fc = new File(vols[1], c);
    File fd = new File(vols[1], d);
    
    // Create the directories
    fa.mkdirs();
    fb.mkdirs();
    fc.mkdirs();
    fd.mkdirs();

    assertTrue(fa.exists());
    assertTrue(fb.exists());
    assertTrue(fc.exists());
    assertTrue(fd.exists());
    
    // Delete all of them
    service.cleanupAllVolumes();
    
    assertFalse(fa.exists());
    assertFalse(fb.exists());
    assertFalse(fc.exists());
    assertFalse(fd.exists());
    
    // Make sure everything is cleaned up
    makeSureCleanedUp(vols, service);
  }
  
  /**
   * This test creates some directories inside the toBeDeleted directory and
   * then start the asyncDiskService.
   * AsyncDiskService will create tasks to delete the content inside the
   * toBeDeleted directories.
   */
  public void testMRAsyncDiskServiceStartupCleaning() throws Throwable {
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};

    String a = "a";
    String b = "b";
    String c = "b/c";
    String d = "d";
    
    // Create directories inside SUBDIR
    String suffix = Path.SEPARATOR_CHAR + MRAsyncDiskService.TOBEDELETED;
    File fa = new File(vols[0] + suffix, a);
    File fb = new File(vols[1] + suffix, b);
    File fc = new File(vols[1] + suffix, c);
    File fd = new File(vols[1] + suffix, d);
    
    // Create the directories
    fa.mkdirs();
    fb.mkdirs();
    fc.mkdirs();
    fd.mkdirs();

    assertTrue(fa.exists());
    assertTrue(fb.exists());
    assertTrue(fc.exists());
    assertTrue(fd.exists());
    
    // Create the asyncDiskService which will delete all contents inside SUBDIR
    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);
    
    // Make sure everything is cleaned up
    makeSureCleanedUp(vols, service);
  }
  
  private void makeSureCleanedUp(String[] vols, MRAsyncDiskService service)
      throws Throwable {
    // Sleep at most 5 seconds to make sure the deleted items are all gone.
    service.shutdown();
    if (!service.awaitTermination(5000)) {
      fail("MRAsyncDiskService is still not shutdown in 5 seconds!");
    }
    
    // All contents should be gone by now.
    for (int i = 0; i < vols.length; i++) {
      File subDir = new File(vols[i]);
      String[] subDirContent = subDir.list();
      // 0.20 version of MRAsyncDiskService deletes toBeDeleted/ after
      // shutdown, so we should not see any children in any volume.
      assertEquals("Volume should contain no children: "
          + MRAsyncDiskService.TOBEDELETED, 0, subDirContent.length);
    }
  }
  
  public void testToleratesSomeUnwritableVolumes() throws Throwable {
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};
    
    assertTrue(new File(vols[0]).mkdirs());
    assertEquals(0, FileUtil.chmod(vols[0], "400")); // read only
    try {
      new MRAsyncDiskService(localFileSystem, vols);
    } finally {
      FileUtil.chmod(vols[0], "755"); // make writable again
    }
  }
  
}
