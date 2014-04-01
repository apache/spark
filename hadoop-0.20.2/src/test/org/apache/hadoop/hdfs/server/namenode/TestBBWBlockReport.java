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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class TestBBWBlockReport {

  private final Path src = new Path(System.getProperty("test.build.data",
      "/tmp"), "testfile");

  private Configuration conf = null;

  private final String fileContent = "PartialBlockReadTest";

  @Before
  public void setUp() {
    conf = new Configuration();
    conf.setInt("ipc.client.connection.maxidletime", 1000);
  }

  @Test(timeout = 60000)
  // timeout is mainly for safe mode
  public void testDNShouldSendBBWReport() throws Exception {
    FileSystem fileSystem = null;
    FSDataOutputStream outStream = null;
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    try {
      fileSystem = cluster.getFileSystem();
      // Keep open stream
      outStream = writeFileAndSync(fileSystem, src, fileContent);
      // Parameter true will ensure that NN came out of safemode
      cluster.restartNameNode();
      assertEquals(
          "Not able to read the synced block content after NameNode restart (with append support)",
          fileContent, getFileContentFromDFS(fileSystem));
    } finally {
      if (null != fileSystem)
        fileSystem.close();
      if (null != outStream)
        outStream.close();
      cluster.shutdown();
    }
  }

  private String getFileContentFromDFS(FileSystem fs) throws IOException {
    ByteArrayOutputStream bio = new ByteArrayOutputStream();
    IOUtils.copyBytes(fs.open(src), bio, conf, true);
    return new String(bio.toByteArray());
  }

  private FSDataOutputStream writeFileAndSync(FileSystem fs, Path src,
      String fileContent) throws IOException {
    FSDataOutputStream fo = fs.create(src);
    fo.writeBytes(fileContent);
    fo.sync();
    return fo;
  }
}
