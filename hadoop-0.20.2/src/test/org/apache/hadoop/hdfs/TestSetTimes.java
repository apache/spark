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
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class tests the access time on files.
 *
 */
public class TestSetTimes extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int numDatanodes = 1;

  static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");

  Random myrand = new Random();
  Path hostsFile;
  Path excludeFile;

  private FSDataOutputStream writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, 
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    return stm;
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
   * Tests mod & access time in DFS.
   */
  public void testTimes() throws IOException {
    Configuration conf = new Configuration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);


    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    final int nnport = cluster.getNameNodePort();
    InetSocketAddress addr = new InetSocketAddress("localhost", 
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    int replicas = 1;
    assertTrue(fileSys instanceof DistributedFileSystem);

    try {
      //
      // create file and record atime/mtime
      //
      System.out.println("Creating testdir1 and testdir1/test1.dat.");
      Path dir1 = new Path("testdir1");
      Path file1 = new Path(dir1, "test1.dat");
      FSDataOutputStream stm = writeFile(fileSys, file1, replicas);
      FileStatus stat = fileSys.getFileStatus(file1);
      long atimeBeforeClose = stat.getAccessTime();
      String adate = dateForm.format(new Date(atimeBeforeClose));
      System.out.println("atime on " + file1 + " before close is " + 
                         adate + " (" + atimeBeforeClose + ")");
      assertTrue(atimeBeforeClose != 0);
      stm.close();

      stat = fileSys.getFileStatus(file1);
      long atime1 = stat.getAccessTime();
      long mtime1 = stat.getModificationTime();
      adate = dateForm.format(new Date(atime1));
      String mdate = dateForm.format(new Date(mtime1));
      System.out.println("atime on " + file1 + " is " + adate + 
                         " (" + atime1 + ")");
      System.out.println("mtime on " + file1 + " is " + mdate + 
                         " (" + mtime1 + ")");
      assertTrue(atime1 != 0);

      //
      // record dir times
      //
      stat = fileSys.getFileStatus(dir1);
      long mdir1 = stat.getAccessTime();
      assertTrue(mdir1 == 0);

      // set the access time to be one day in the past
      long atime2 = atime1 - (24L * 3600L * 1000L);
      fileSys.setTimes(file1, -1, atime2);

      // check new access time on file
      stat = fileSys.getFileStatus(file1);
      long atime3 = stat.getAccessTime();
      String adate3 = dateForm.format(new Date(atime3));
      System.out.println("new atime on " + file1 + " is " + 
                         adate3 + " (" + atime3 + ")");
      assertTrue(atime2 == atime3);
      assertTrue(mtime1 == stat.getModificationTime());

      // set the modification time to be 1 hour in the past
      long mtime2 = mtime1 - (3600L * 1000L);
      fileSys.setTimes(file1, mtime2, -1);

      // check new modification time on file
      stat = fileSys.getFileStatus(file1);
      long mtime3 = stat.getModificationTime();
      String mdate3 = dateForm.format(new Date(mtime3));
      System.out.println("new mtime on " + file1 + " is " + 
                         mdate3 + " (" + mtime3 + ")");
      assertTrue(atime2 == stat.getAccessTime());
      assertTrue(mtime2 == mtime3);

      // shutdown cluster and restart
      cluster.shutdown();
      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster(nnport, conf, 1, false, true,
                                   null, null, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();

      // verify that access times and modification times persist after a
      // cluster restart.
      System.out.println("Verifying times after cluster restart");
      stat = fileSys.getFileStatus(file1);
      assertTrue(atime2 == stat.getAccessTime());
      assertTrue(mtime3 == stat.getModificationTime());
    
      cleanupFile(fileSys, file1);
      cleanupFile(fileSys, dir1);
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
    new TestSetTimes().testTimes();
  }
}
