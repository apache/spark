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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

/**
 * Test that the NN stays up as long as it has a valid storage directory and
 * exits when there are no more valid storage directories.
 */
public class TestStorageDirectoryFailure {

  MiniDFSCluster cluster = null;
  FileSystem fs;
  SecondaryNameNode secondaryNN;
  ArrayList<String> nameDirs;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();

    String baseDir = System.getProperty("test.build.data", "/tmp");
    File dfsDir = new File(baseDir, "dfs");
    nameDirs = new ArrayList<String>();
    nameDirs.add(new File(dfsDir, "name1").getPath());
    nameDirs.add(new File(dfsDir, "name2").getPath());
    nameDirs.add(new File(dfsDir, "name3").getPath());

    conf.set("dfs.name.dir", StringUtils.join(nameDirs, ","));
    conf.set("dfs.data.dir", new File(dfsDir, "data").getPath());
    conf.set("fs.checkpoint.dir", new File(dfsDir, "secondary").getPath());
    conf.set("fs.default.name", "hdfs://localhost:0");
    conf.set("dfs.http.address", "0.0.0.0:0");
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    cluster = new MiniDFSCluster(0, conf, 1, true, false, true, null, null,
        null, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    secondaryNN = new SecondaryNameNode(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (secondaryNN != null) {
      secondaryNN.shutdown();
    }
  }

  private List<StorageDirectory> getRemovedDirs() {
    return cluster.getNameNode().getFSImage().getRemovedStorageDirs();
  }

  private int numRemovedDirs() {
    return getRemovedDirs().size();
  }

  private void writeFile(String name, byte[] buff) throws IOException {
    FSDataOutputStream writeStream = fs.create(new Path(name));
    writeStream.write(buff, 0, buff.length);
    writeStream.close();
  }

  private byte[] readFile(String name, int len) throws IOException {
    FSDataInputStream readStream = fs.open(new Path(name));
    byte[] buff = new byte[len];
    readStream.readFully(buff);
    readStream.close();
    return buff;
  }

  /** Assert that we can create and read a file */
  private void checkFileCreation(String name) throws IOException {
    byte[] buff = "some bytes".getBytes();
    writeFile(name, buff);
    assertTrue(Arrays.equals(buff, readFile(name, buff.length)));
  }

  /** Assert that we can read a file we created */
  private void checkFileContents(String name) throws IOException {
    byte[] buff = "some bytes".getBytes();
    assertTrue(Arrays.equals(buff, readFile(name, buff.length)));
  }

  @Test
  /** Remove storage dirs and checkpoint to trigger detection */
  public void testCheckpointAfterFailingFirstNamedir() throws IOException {
    assertEquals(0, numRemovedDirs());

    checkFileCreation("file0");

    // Remove the 1st storage dir
    FileUtil.fullyDelete(new File(nameDirs.get(0)));
    secondaryNN.doCheckpoint();
    assertEquals(1, numRemovedDirs());
    assertEquals(nameDirs.get(0), getRemovedDirs().get(0).getRoot().getPath());

    checkFileCreation("file1");

    // Remove the 2nd
    FileUtil.fullyDelete(new File(nameDirs.get(1)));
    secondaryNN.doCheckpoint();
    assertEquals(2, numRemovedDirs());
    assertEquals(nameDirs.get(1), getRemovedDirs().get(1).getRoot().getPath());

    checkFileCreation("file2");

    // Remove the last one. Prevent the NN from exiting the process when
    // it notices this via the checkpoint.
    FSEditLog spyLog = spy(cluster.getNameNode().getFSImage().getEditLog());
    doNothing().when(spyLog).fatalExit(anyString());
    cluster.getNameNode().getFSImage().setEditLog(spyLog);

    // After the checkpoint, we should be dead. Verify fatalExit was
    // called and that eg a checkpoint fails.
    FileUtil.fullyDelete(new File(nameDirs.get(2)));
    try {
      secondaryNN.doCheckpoint();
      fail("There's no storage to retrieve an image from");
    } catch (FileNotFoundException fnf) {
      // Expected
    }
    verify(spyLog, atLeastOnce()).fatalExit(anyString());

    // Check that we can't mutate state without any edit streams
    try {
      checkFileCreation("file3");
      fail("Created a file w/o edit streams");
    } catch (IOException ioe) {
      // Expected
      assertTrue(ioe.getMessage().contains(
          "java.lang.AssertionError: No edit streams to log to"));
    }
  }

  @Test
  /** Test that we can restart OK after removing a failed dir */
  public void testRestartAfterFailingStorageDir() throws IOException {
    assertEquals(0, numRemovedDirs());

    checkFileCreation("file0");

    FileUtil.fullyDelete(new File(nameDirs.get(0)));
    secondaryNN.doCheckpoint();
    assertEquals(1, numRemovedDirs());
    assertEquals(nameDirs.get(0), getRemovedDirs().get(0).getRoot().getPath());
    
    checkFileCreation("file1");

    new File(nameDirs.get(0)).mkdir();
    cluster.restartNameNode();

    // The dir was restored, is no longer considered removed
    assertEquals(0, numRemovedDirs());
    checkFileContents("file0");
    checkFileContents("file1");
  }

  @Test
  /** Test that we abort when there are no valid edit log directories
   * remaining. */
  public void testAbortOnNoValidEditDirs() throws IOException {
    cluster.restartNameNode();
    assertEquals(0, numRemovedDirs());
    checkFileCreation("file9");
    cluster.getNameNode().getFSImage().
      removeStorageDir(new File(nameDirs.get(0)));
    cluster.getNameNode().getFSImage().
      removeStorageDir(new File(nameDirs.get(1)));
    FSEditLog spyLog = spy(cluster.getNameNode().getFSImage().getEditLog());
    doNothing().when(spyLog).fatalExit(anyString());
    cluster.getNameNode().getFSImage().setEditLog(spyLog);
    cluster.getNameNode().getFSImage().
      removeStorageDir(new File(nameDirs.get(2)));
    verify(spyLog, atLeastOnce()).fatalExit(anyString());
  }
}
