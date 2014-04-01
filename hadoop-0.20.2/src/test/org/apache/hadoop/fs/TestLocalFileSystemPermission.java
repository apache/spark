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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell;

import java.io.*;
import java.util.*;

import junit.framework.*;

/**
 * This class tests the local file system via the FileSystem abstraction.
 */
public class TestLocalFileSystemPermission extends TestCase {
  static final String TEST_PATH_PREFIX = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString().replace(' ', '_')
      + "/" + TestLocalFileSystemPermission.class.getSimpleName() + "_";

  {
    try {
      ((org.apache.commons.logging.impl.Log4JLogger)FileSystem.LOG).getLogger()
      .setLevel(org.apache.log4j.Level.DEBUG);
    }
    catch(Exception e) {
      System.out.println("Cannot change log level\n"
          + StringUtils.stringifyException(e));
    }
  }

  private Path writeFile(FileSystem fs, String name) throws IOException {
    Path f = new Path(TEST_PATH_PREFIX + name);
    FSDataOutputStream stm = fs.create(f);
    stm.writeBytes("42\n");
    stm.close();
    return f;
  }

  private void cleanupFile(FileSystem fs, Path name) throws IOException {
    assertTrue(fs.exists(name));
    fs.delete(name, true);
    assertTrue(!fs.exists(name));
  }

  /** Test LocalFileSystem.setPermission */
  public void testLocalFSsetPermission() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
      return;
    }
    Configuration conf = new Configuration();
    LocalFileSystem localfs = FileSystem.getLocal(conf);
    String filename = "foo";
    Path f = writeFile(localfs, filename);
    try {
      System.out.println(filename + ": " + getPermission(localfs, f));
    }
    catch(Exception e) {
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
      return;
    }

    try {
      // create files and manipulate them.
      FsPermission all = new FsPermission((short)0777);
      FsPermission none = new FsPermission((short)0);

      localfs.setPermission(f, none);
      assertEquals(none, getPermission(localfs, f));

      localfs.setPermission(f, all);
      assertEquals(all, getPermission(localfs, f));
    }
    finally {cleanupFile(localfs, f);}
  }

  FsPermission getPermission(LocalFileSystem fs, Path p) throws IOException {
    return fs.getFileStatus(p).getPermission();
  }

  /** Test LocalFileSystem.setOwner */
  public void testLocalFSsetOwner() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
      return;
    }

    Configuration conf = new Configuration();
    LocalFileSystem localfs = FileSystem.getLocal(conf);
    String filename = "bar";
    Path f = writeFile(localfs, filename);
    List<String> groups = null;
    try {
      groups = getGroups();
      System.out.println(filename + ": " + getPermission(localfs, f));
    }
    catch(IOException e) {
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
      return;
    }
    if (groups == null || groups.size() < 1) {
      System.out.println("Cannot run test: need at least one group.  groups="
                         + groups);
      return;
    }

    // create files and manipulate them.
    try {
      String g0 = groups.get(0);
      localfs.setOwner(f, null, g0);
      assertEquals(g0, getGroup(localfs, f));

      if (groups.size() > 1) {
        String g1 = groups.get(1);
        localfs.setOwner(f, null, g1);
        assertEquals(g1, getGroup(localfs, f));
      } else {
        System.out.println("Not testing changing the group since user " +
                           "belongs to only one group.");
      }
    } 
    finally {cleanupFile(localfs, f);}
  }

  static List<String> getGroups() throws IOException {
    List<String> a = new ArrayList<String>();
    String s = Shell.execCommand(Shell.getGroupsCommand());
    for(StringTokenizer t = new StringTokenizer(s); t.hasMoreTokens(); ) {
      a.add(t.nextToken());
    }
    return a;
  }

  String getGroup(LocalFileSystem fs, Path p) throws IOException {
    return fs.getFileStatus(p).getGroup();
  }
}
