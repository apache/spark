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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestEditLogLoading {
  
  private static final int NUM_DATA_NODES = 0;
  
  @Test
  public void testDisplayRecentEditLogOpCodes() throws IOException {
    // start a cluster 
    Configuration conf = new Configuration();
    conf.set("dfs.name.dir", new File(MiniDFSCluster.getBaseDir(), "name").getPath());
    
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    cluster = new MiniDFSCluster(0, conf, NUM_DATA_NODES, true, false, null, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();

    FSImage fsimage = namesystem.getFSImage();
    final FSEditLog editLog = fsimage.getEditLog();
    for (int i = 0; i < 20; i++) {
      fileSys.mkdirs(new Path("/tmp/tmp" + i));
    }
    File editFile = editLog.getFsEditName();
    System.out.println("edit log file: " + editFile);
    editLog.close();
    cluster.shutdown();
    
    // Corrupt the edits file.
    long fileLen = editFile.length();
    RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
    rwf.seek(fileLen - 40);
    for (int i = 0; i < 20; i++) {
      rwf.write((byte) 2); // FSEditLog.DELETE
    }
    rwf.close();
    
    String expectedErrorMessage = "^Error replaying edit log at offset \\d+\n";
    expectedErrorMessage += "Recent opcode offsets: (\\d+\\s*){4}$";
    try {
      cluster = new MiniDFSCluster(0, conf, NUM_DATA_NODES, false, false, null, null);
      cluster.waitActive();
      fail("should not be able to start");
    } catch (IOException e) {
      assertTrue("error message contains opcodes message",
          e.getMessage().matches(expectedErrorMessage));
    }
  }
}