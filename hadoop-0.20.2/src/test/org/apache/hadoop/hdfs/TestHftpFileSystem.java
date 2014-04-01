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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.Random;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.ServletUtil;
import org.apache.log4j.Level;

public class TestHftpFileSystem {
  private static final Random RAN = new Random();
  
  private static Configuration config = null;
  private static MiniDFSCluster cluster = null;
  private static FileSystem hdfs = null;
  private static HftpFileSystem hftpFs = null;

  private static Path[] TEST_PATHS = new Path[] {
      // URI does not encode, Request#getPathInfo returns /foo
      new Path("/foo;bar"),

      // URI does not encode, Request#getPathInfo returns verbatim
      new Path("/foo+"),
      new Path("/foo+bar/foo+bar"),
      new Path("/foo=bar/foo=bar"),
      new Path("/foo,bar/foo,bar"),
      new Path("/foo@bar/foo@bar"),
      new Path("/foo&bar/foo&bar"),
      new Path("/foo$bar/foo$bar"),
      new Path("/foo_bar/foo_bar"),
      new Path("/foo~bar/foo~bar"),
      new Path("/foo.bar/foo.bar"),
      new Path("/foo../bar/foo../bar"),
      new Path("/foo.../bar/foo.../bar"),
      new Path("/foo'bar/foo'bar"),
      new Path("/foo#bar/foo#bar"),
      new Path("/foo!bar/foo!bar"),
      // HDFS file names may not contain ":"

      // URI percent encodes, Request#getPathInfo decodes
      new Path("/foo bar/foo bar"),
      new Path("/foo?bar/foo?bar"),
      new Path("/foo\">bar/foo\">bar"),
    };

  @BeforeClass
  public static void setUp() throws IOException {
    ((Log4JLogger)HftpFileSystem.LOG).getLogger().setLevel(Level.ALL);

    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    config = new Configuration();
    config.set("slave.host.name", "localhost");

    cluster = new MiniDFSCluster(config, 2, true, null);
    hdfs = cluster.getFileSystem();
    final String hftpuri = "hftp://" + config.get("dfs.http.address"); 
    hftpFs = (HftpFileSystem) new Path(hftpuri).getFileSystem(config);
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    hdfs.close();
    hftpFs.close();
    cluster.shutdown();
  }

  /**
   * Test file creation and access with file names that need encoding. 
   */
  @Test
  public void testFileNameEncoding() throws IOException, URISyntaxException {
    for (Path p : TEST_PATHS) {
      // Create and access the path (data and streamFile servlets)
      FSDataOutputStream out = hdfs.create(p, true);
      out.writeBytes("0123456789");
      out.close();
      FSDataInputStream in = hftpFs.open(p);
      assertEquals('0', in.read());

      // Check the file status matches the path. Hftp returns a FileStatus
      // with the entire URI, extract the path part.
      assertEquals(p, new Path(hftpFs.getFileStatus(p).getPath().toUri().getPath()));

      // Test list status (listPath servlet)
      assertEquals(1, hftpFs.listStatus(p).length);

      // Test content summary (contentSummary servlet)
      assertNotNull("No content summary", hftpFs.getContentSummary(p));

      // Test checksums (fileChecksum and getFileChecksum servlets)
      assertNotNull("No file checksum", hftpFs.getFileChecksum(p));
    }
  }

  private void testDataNodeRedirect(Path path) throws IOException {
    // Create the file
    if (hdfs.exists(path)) {
      hdfs.delete(path, true);
    }
    FSDataOutputStream out = hdfs.create(path, (short)1);
    out.writeBytes("0123456789");
    out.close();

    // Get the path's block location so we can determine
    // if we were redirected to the right DN.
    FileStatus status = hdfs.getFileStatus(path);
    BlockLocation[] locations =
        hdfs.getFileBlockLocations(status, 0, 10);
    String locationName = locations[0].getNames()[0];

    // Connect to the NN to get redirected
    URL u = hftpFs.getNamenodeURL(
        "/data" + ServletUtil.encodePath(path.toUri().getPath()), 
        "ugi=userx,groupy");
    HttpURLConnection conn = (HttpURLConnection)u.openConnection();
    HttpURLConnection.setFollowRedirects(true);
    conn.connect();
    conn.getInputStream();

    boolean checked = false;
    // Find the datanode that has the block according to locations
    // and check that the URL was redirected to this DN's info port
    for (DataNode node : cluster.getDataNodes()) {
      DatanodeRegistration dnR = node.dnRegistration;
      if (dnR.getName().equals(locationName)) {
        checked = true;
        assertEquals(dnR.getInfoPort(), conn.getURL().getPort());
      }
    }
    assertTrue("The test never checked that location of " +
               "the block and hftp desitnation are the same", checked);
  }

  /**
   * Test that clients are redirected to the appropriate DN.
   */
  @Test
  public void testDataNodeRedirect() throws IOException {
    for (Path p : TEST_PATHS) {
      testDataNodeRedirect(p);
    }
  }

  /**
   * Tests getPos() functionality.
   */
  @Test
  public void testGetPos() throws IOException {
    final Path testFile = new Path("/testfile+1");
    // Write a test file.
    FSDataOutputStream out = hdfs.create(testFile, true);
    out.writeBytes("0123456789");
    out.close();
    
    FSDataInputStream in = hftpFs.open(testFile);
    
    // Test read().
    for (int i = 0; i < 5; ++i) {
      assertEquals(i, in.getPos());
      in.read();
    }
    
    // Test read(b, off, len).
    assertEquals(5, in.getPos());
    byte[] buffer = new byte[10];
    assertEquals(2, in.read(buffer, 0, 2));
    assertEquals(7, in.getPos());
    
    // Test read(b).
    int bytesRead = in.read(buffer);
    assertEquals(7 + bytesRead, in.getPos());
    
    // Test EOF.
    for (int i = 0; i < 100; ++i) {
      in.read();
    }
    assertEquals(10, in.getPos());
    in.close();
  }

  /**
   * Tests seek().
   */
  @Test
  public void testSeek() throws IOException {
    final Path testFile = new Path("/testfile+1");
    FSDataOutputStream out = hdfs.create(testFile, true);
    out.writeBytes("0123456789");
    out.close();
    FSDataInputStream in = hftpFs.open(testFile);
    in.seek(7);
    assertEquals('7', in.read());
  }
}
