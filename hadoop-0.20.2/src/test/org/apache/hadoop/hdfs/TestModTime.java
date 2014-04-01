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
import java.util.Random;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

/**
 * This class tests the decommissioning of nodes.
 * @author Dhruba Borthakur
 */
public class TestModTime extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int numDatanodes = 6;


  Random myrand = new Random();
  Path hostsFile;
  Path excludeFile;

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, 
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  private void printDatanodeReport(DatanodeInfo[] info) {
    System.out.println("-------------------------------------------------");
    for (int i = 0; i < info.length; i++) {
      System.out.println(info[i].getDatanodeReport());
      System.out.println();
    }
  }

  /**
   * Tests modification time in DFS.
   */
  public void testModTime() throws IOException {
    Configuration conf = new Configuration();

    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost", 
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    int replicas = numDatanodes - 1;
    assertTrue(fileSys instanceof DistributedFileSystem);

    try {

     //
     // create file and record ctime and mtime of test file
     //
     System.out.println("Creating testdir1 and testdir1/test1.dat.");
     Path dir1 = new Path("testdir1");
     Path file1 = new Path(dir1, "test1.dat");
     writeFile(fileSys, file1, replicas);
     FileStatus stat = fileSys.getFileStatus(file1);
     long mtime1 = stat.getModificationTime();
     assertTrue(mtime1 != 0);
     //
     // record dir times
     //
     stat = fileSys.getFileStatus(dir1);
     long mdir1 = stat.getModificationTime();

     //
     // create second test file
     //
     System.out.println("Creating testdir1/test2.dat.");
     Path file2 = new Path(dir1, "test2.dat");
     writeFile(fileSys, file2, replicas);
     stat = fileSys.getFileStatus(file2);

     //
     // verify that mod time of dir remains the same
     // as before. modification time of directory has increased.
     //
     stat = fileSys.getFileStatus(dir1);
     assertTrue(stat.getModificationTime() >= mdir1);
     mdir1 = stat.getModificationTime();
     //
     // create another directory
     //
     Path dir2 = (new Path("testdir2/")).makeQualified(fileSys);
     System.out.println("Creating testdir2 " + dir2);
     assertTrue(fileSys.mkdirs(dir2));
     stat = fileSys.getFileStatus(dir2);
     long mdir2 = stat.getModificationTime();
     //
     // rename file1 from testdir into testdir2
     //
     Path newfile = new Path(dir2, "testnew.dat");
     System.out.println("Moving " + file1 + " to " + newfile);
     fileSys.rename(file1, newfile);
     //
     // verify that modification time of file1 did not change.
     //
     stat = fileSys.getFileStatus(newfile);
     assertTrue(stat.getModificationTime() == mtime1);
     //
     // verify that modification time of  testdir1 and testdir2
     // were changed. 
     //
     stat = fileSys.getFileStatus(dir1);
     assertTrue(stat.getModificationTime() != mdir1);
     mdir1 = stat.getModificationTime();

     stat = fileSys.getFileStatus(dir2);
     assertTrue(stat.getModificationTime() != mdir2);
     mdir2 = stat.getModificationTime();
     //
     // delete newfile
     //
     System.out.println("Deleting testdir2/testnew.dat.");
     assertTrue(fileSys.delete(newfile, true));
     //
     // verify that modification time of testdir1 has not changed.
     //
     stat = fileSys.getFileStatus(dir1);
     assertTrue(stat.getModificationTime() == mdir1);
     //
     // verify that modification time of testdir2 has changed.
     //
     stat = fileSys.getFileStatus(dir2);
     assertTrue(stat.getModificationTime() != mdir2);
     mdir2 = stat.getModificationTime();

     cleanupFile(fileSys, file2);
     cleanupFile(fileSys, dir1);
     cleanupFile(fileSys, dir2);
    } catch (IOException e) {
      info = client.datanodeReport(DatanodeReportType.ALL);
      printDatanodeReport(info);
      throw e;
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    new TestModTime().testModTime();
  }
}
