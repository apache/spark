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

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the DFS class via the FileSystem interface in a single node
 * mini-cluster.
 */
public class TestLocalDFS extends TestCase {

  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    DataOutputStream stm = fileSys.create(name);
    stm.writeBytes("oom");
    stm.close();
  }
  
  private void readFile(FileSystem fileSys, Path name) throws IOException {
    DataInputStream stm = fileSys.open(name);
    byte[] buffer = new byte[4];
    int bytesRead = stm.read(buffer, 0 , 4);
    assertEquals("oom", new String(buffer, 0 , bytesRead));
    stm.close();
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  static String getUserName(FileSystem fs) {
    if (fs instanceof DistributedFileSystem) {
      return ((DistributedFileSystem)fs).dfs.ugi.getShortUserName();
    }
    return System.getProperty("user.name");
  }

  /**
   * Tests get/set working directory in DFS.
   */
  public void testWorkingDirectory() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path orig_path = fileSys.getWorkingDirectory();
      assertTrue(orig_path.isAbsolute());
      Path file1 = new Path("somewhat/random.txt");
      writeFile(fileSys, file1);
      assertTrue(fileSys.exists(new Path(orig_path, file1.toString())));
      fileSys.delete(file1, true);
      Path subdir1 = new Path("/somewhere");
      fileSys.setWorkingDirectory(subdir1);
      writeFile(fileSys, file1);
      cleanupFile(fileSys, new Path(subdir1, file1.toString()));
      Path subdir2 = new Path("else");
      fileSys.setWorkingDirectory(subdir2);
      writeFile(fileSys, file1);
      readFile(fileSys, file1);
      cleanupFile(fileSys, new Path(new Path(subdir1, subdir2.toString()),
                                    file1.toString()));

      // test home directory
      Path home = new Path("/user/" + getUserName(fileSys))
        .makeQualified(fileSys);
      Path fsHome = fileSys.getHomeDirectory();
      assertEquals(home, fsHome);

    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
