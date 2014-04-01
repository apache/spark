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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;

import org.junit.Test;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;

/**
 * This test validates that chmod, chown, chgrp returning correct exit codes
 * 
 */
public class TestFsShellReturnCode {
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.fs.TestFsShellReturnCode");

  private static final Configuration conf = new Configuration();
  private FileSystem fs;

  private static String TEST_ROOT_DIR = System.getProperty("test.build.data",
      "build/test/data/testCHReturnCode");

  static void writeFile(FileSystem fs, Path name) throws Exception {
    FSDataOutputStream stm = fs.create(name);
    stm.writeBytes("42\n");
    stm.close();
  }

  public void verify(FileSystem fs, String cmd, String argv[], int cmdIndex,
      FsShell fsShell, int exitCode) throws Exception {
    int ec;
    ec = FsShellPermissions.changePermissions(fs, cmd, argv, cmdIndex, fsShell);
    Assert.assertEquals(ec, exitCode);
  }

  /**
   * Test Chmod 1. Create and write file on FS 2. Verify that exit code for
   * chmod on existing file is 0 3. Verify that exit code for chmod on
   * non-existing file is 1 4. Verify that exit code for chmod with glob input
   * on non-existing file is 1 5. Verify that exit code for chmod with glob
   * input on existing file in 0
   * 
   * @throws Exception
   */
  @Test
  public void testChmod() throws Exception {

    FsShell fsShell = new FsShell(conf);
    if (this.fs == null) {
      this.fs = FileSystem.get(fsShell.getConf());
    }

    final String f1 = TEST_ROOT_DIR + "/" + "testChmod/fileExists";
    final String f2 = TEST_ROOT_DIR + "/" + "testChmod/fileDoesNotExist";
    final String f3 = TEST_ROOT_DIR + "/" + "testChmod/nonExistingfiles*";

    Path p1 = new Path(f1);

    final Path p4 = new Path(TEST_ROOT_DIR + "/" + "testChmod/file1");
    final Path p5 = new Path(TEST_ROOT_DIR + "/" + "testChmod/file2");
    final Path p6 = new Path(TEST_ROOT_DIR + "/" + "testChmod/file3");

    final String f7 = TEST_ROOT_DIR + "/" + "testChmod/file*";

    FileSystem fileSys = FileSystem.getLocal(conf);

    // create and write test file
    writeFile(fileSys, p1);
    assertTrue(fileSys.exists(p1));

    // Test 1: Test 1: exit code for chmod on existing is 0
    String argv[] = { "-chmod", "777", f1 };
    verify(fs, "-chmod", argv, 1, fsShell, 0);

    // Test 2: exit code for chmod on non-existing path is 1
    String argv2[] = { "-chmod", "777", f2 };
    verify(fs, "-chmod", argv2, 1, fsShell, 1);

    // Test 3: exit code for chmod on non-existing path with globbed input is 1
    String argv3[] = { "-chmod", "777", f3 };
    verify(fs, "-chmod", argv3, 1, fsShell, 1);

    // create required files
    writeFile(fileSys, p4);
    assertTrue(fileSys.exists(p4));
    writeFile(fileSys, p5);
    assertTrue(fileSys.exists(p5));
    writeFile(fileSys, p6);
    assertTrue(fileSys.exists(p6));

    // Test 4: exit code for chmod on existing path with globbed input is 0
    String argv4[] = { "-chmod", "777", f7 };
    verify(fs, "-chmod", argv4, 1, fsShell, 0);

  }

  /**
   * Test Chown 1. Create and write file on FS 2. Verify that exit code for
   * Chown on existing file is 0 3. Verify that exit code for Chown on
   * non-existing file is 1 4. Verify that exit code for Chown with glob input
   * on non-existing file is 1 5. Verify that exit code for Chown with glob
   * input on existing file in 0
   * 
   * @throws Exception
   */
  @Test
  public void testChown() throws Exception {

    FsShell fsShell = new FsShell(conf);
    if (this.fs == null) {
      this.fs = FileSystem.get(fsShell.getConf());
    }

    final String f1 = TEST_ROOT_DIR + "/" + "testChown/fileExists";
    final String f2 = TEST_ROOT_DIR + "/" + "testChown/fileDoesNotExist";
    final String f3 = TEST_ROOT_DIR + "/" + "testChown/nonExistingfiles*";

    Path p1 = new Path(f1);

    final Path p4 = new Path(TEST_ROOT_DIR + "/" + "testChown/file1");
    final Path p5 = new Path(TEST_ROOT_DIR + "/" + "testChown/file2");
    final Path p6 = new Path(TEST_ROOT_DIR + "/" + "testChown/file3");

    final String f7 = TEST_ROOT_DIR + "/" + "testChown/file*";

    FileSystem fileSys = FileSystem.getLocal(conf);

    // create and write test file
    writeFile(fileSys, p1);
    assertTrue(fileSys.exists(p1));

    // Test 1: exit code for chown on existing file is 0
    String argv[] = { "-chown", "admin", f1 };
    verify(fs, "-chown", argv, 1, fsShell, 0);

    // Test 2: exit code for chown on non-existing path is 1
    String argv2[] = { "-chown", "admin", f2 };
    verify(fs, "-chown", argv2, 1, fsShell, 1);

    // Test 3: exit code for chown on non-existing path with globbed input is 1
    String argv3[] = { "-chown", "admin", f3 };
    verify(fs, "-chown", argv3, 1, fsShell, 1);

    // create required files
    writeFile(fileSys, p4);
    assertTrue(fileSys.exists(p4));
    writeFile(fileSys, p5);
    assertTrue(fileSys.exists(p5));
    writeFile(fileSys, p6);
    assertTrue(fileSys.exists(p6));

    // Test 4: exit code for chown on existing path with globbed input is 0
    String argv4[] = { "-chown", "admin", f7 };
    verify(fs, "-chown", argv4, 1, fsShell, 0);

  }

  /**
   * Test Chgrp 1. Create and write file on FS 2. Verify that exit code for
   * chgrp on existing file is 0 3. Verify that exit code for chgrp on
   * non-existing file is 1 4. Verify that exit code for chgrp with glob input
   * on non-existing file is 1 5. Verify that exit code for chgrp with glob
   * input on existing file in 0
   * 
   * @throws Exception
   */
  @Test
  public void testChgrp() throws Exception {

    FsShell fsShell = new FsShell(conf);
    if (this.fs == null) {
      this.fs = FileSystem.get(fsShell.getConf());
    }

    final String f1 = TEST_ROOT_DIR + "/" + "testChgrp/fileExists";
    final String f2 = TEST_ROOT_DIR + "/" + "testChgrp/fileDoesNotExist";
    final String f3 = TEST_ROOT_DIR + "/" + "testChgrp/nonExistingfiles*";

    Path p1 = new Path(f1);

    final Path p4 = new Path(TEST_ROOT_DIR + "/" + "testChgrp/file1");
    final Path p5 = new Path(TEST_ROOT_DIR + "/" + "testChgrp/file2");
    final Path p6 = new Path(TEST_ROOT_DIR + "/" + "testChgrp/file3");

    final String f7 = TEST_ROOT_DIR + "/" + "testChgrp/file*";

    FileSystem fileSys = FileSystem.getLocal(conf);

    // create and write test file
    writeFile(fileSys, p1);
    assertTrue(fileSys.exists(p1));

    // Test 1: exit code for chgrp on existing file is 0
    String argv[] = { "-chgrp", "admin", f1 };
    verify(fs, "-chgrp", argv, 1, fsShell, 0);

    // Test 2: exit code for chgrp on non existing path is 1
    String argv2[] = { "-chgrp", "admin", f2 };
    verify(fs, "-chgrp", argv2, 1, fsShell, 1);

    // Test 3: exit code for chgrp on non-existing path with globbed input is 1
    String argv3[] = { "-chgrp", "admin", f3 };
    verify(fs, "-chgrp", argv3, 1, fsShell, 1);

    // create required files
    writeFile(fileSys, p4);
    assertTrue(fileSys.exists(p4));
    writeFile(fileSys, p5);
    assertTrue(fileSys.exists(p5));
    writeFile(fileSys, p6);
    assertTrue(fileSys.exists(p6));

    // Test 4: exit code for chgrp on existing path with globbed input is 0
    String argv4[] = { "-chgrp", "admin", f7 };
    verify(fs, "-chgrp", argv4, 1, fsShell, 0);

  }
}