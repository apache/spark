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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.ListPathsServlet;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link ListPathsServlet} that serves the URL
 * http://<namenodeaddress:httpport?/listPaths
 * 
 * This test does not use the servlet directly. Instead it is based on
 * {@link HftpFileSystem}, which uses this servlet to implement
 * {@link HftpFileSystem#listStatus(Path)} method.
 */
public class TestListPathServlet {
  private static final Configuration CONF = new Configuration();
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static URI hftpURI;
  private static HftpFileSystem hftpFs;
  private Random r = new Random();
  private List<String> filelist = new ArrayList<String>();

  @BeforeClass
  public static void setup() throws Exception {
    // start a cluster with single datanode
    cluster = new MiniDFSCluster(CONF, 1, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();

    final String str = "hftp://"
        + CONF.get("dfs.http.address");
    hftpURI = new URI(str);
    hftpFs = (HftpFileSystem) FileSystem.get(hftpURI, CONF);
  }

  @AfterClass
  public static void teardown() {
    cluster.shutdown();
  }

  /** create a file with a length of <code>fileLen</code> */
  private void createFile(String fileName, long fileLen) throws IOException {
    filelist.add(hftpURI + fileName);
    final Path filePath = new Path(fileName);
    DFSTestUtil.createFile(fs, filePath, fileLen, (short) 1, r.nextLong());
  }

  private void mkdirs(String dirName) throws IOException {
    filelist.add(hftpURI + dirName);
    fs.mkdirs(new Path(dirName));
  }

  @Test
  public void testListStatus() throws Exception {
    // Empty root directory
    checkStatus("/");

    // Root directory with files and directories
    createFile("/a", 1);
    createFile("/b", 1);
    mkdirs("/dir");
    checkStatus("/");

    // A directory with files and directories
    createFile("/dir/a", 1);
    createFile("/dir/b", 1);
    mkdirs("/dir/dir1");
    checkStatus("/dir");

    // Non existent path
    checkStatus("/nonexistent");
    checkStatus("/nonexistent/a");

    final String username = UserGroupInformation.getCurrentUser().getShortUserName() + "1";
    final HftpFileSystem hftp2 = cluster.getHftpFileSystemAs(username, CONF, "somegroup");
    { //test file not found on hftp 
      final Path nonexistent = new Path("/nonexistent");
      try {
        hftp2.getFileStatus(nonexistent);
        Assert.fail();
      } catch(IOException ioe) {
        FileSystem.LOG.info("GOOD: getting an exception", ioe);
      }
    }

    { //test permission error on hftp
      final Path dir = new Path("/dir");
      fs.setPermission(dir, new FsPermission((short)0));
      try {
        hftp2.getFileStatus(new Path(dir, "a"));
        Assert.fail();
      } catch(IOException ioe) {
        FileSystem.LOG.info("GOOD: getting an exception", ioe);
      }
    }
  }

  private void checkStatus(String listdir) throws IOException {
    final Path listpath = hftpFs.makeQualified(new Path(listdir));
    listdir = listpath.toString();
    final FileStatus[] statuslist = hftpFs.listStatus(listpath);
    for (String directory : filelist) {
      System.out.println("dir:" + directory);
    }
    for (String file : filelist) {
      System.out.println("file:" + file);
    }
    for (FileStatus status : statuslist) {
      System.out.println("status:" + status.getPath().toString() + " type "
          + (status.isDir() ? "directory" : "file"));
    }
    for (String file : filelist) {
      boolean found = false;
      // Consider only file under the list path
      if (!file.startsWith(listpath.toString()) ||
          file.equals(listpath.toString())) {
        continue;
      }
      for (FileStatus status : statuslist) {
        if (status.getPath().toString().equals(file)) {
          found = true;
          break;
        }
      }
      Assert.assertTrue("Directory/file not returned in list status " + file,
          found);
    }
  }
}
