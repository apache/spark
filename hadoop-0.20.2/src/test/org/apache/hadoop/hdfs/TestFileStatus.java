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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * This class tests the FileStatus API.
 */
public class TestFileStatus {
  {
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;

  private void writeFile(FileSystem fileSys, Path name, int repl,
                         int fileSize, int blockSize)
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

  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    DFSTestUtil.waitReplication(fileSys, name, (short) repl);
  }


  /**
   * Tests various options of DFSShell.
   */
  @Test
  public void testFileStatus() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 2);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    final HftpFileSystem hftpfs = cluster.getHftpFileSystem();
    final DFSClient dfsClient = new DFSClient(NameNode.getAddress(conf), conf);
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);
      
      // make sure getFileInfo returns null for files which do not exist
      HdfsFileStatus fileInfo = dfsClient.getFileInfo("/noSuchFile");
      assertTrue(fileInfo == null);

      // create a file in home directory
      //
      Path file1 = new Path("filestatus.dat");
      writeFile(fs, file1, 1, fileSize, blockSize);
      System.out.println("Created file filestatus.dat with one "
                         + " replicas.");
      checkFile(fs, file1, 1);
      System.out.println("Path : \"" + file1 + "\"");

      // test getFileStatus on a file
      FileStatus status = fs.getFileStatus(file1);
      assertTrue(file1 + " should be a file", 
          status.isDir() == false);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertTrue(status.getLen() == fileSize);
      assertEquals(fs.makeQualified(file1).toString(), 
          status.getPath().toString());

      // test getVisbileLen
      DFSDataInputStream fin = (DFSDataInputStream)fs.open(file1);
      assertEquals(status.getLen(), fin.getVisibleLength());
      
      // test listStatus on a file
      FileStatus[] stats = fs.listStatus(file1);
      assertEquals(1, stats.length);
      status = stats[0];
      assertTrue(file1 + " should be a file", 
          status.isDir() == false);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertTrue(status.getLen() == fileSize);
      assertEquals(fs.makeQualified(file1).toString(), 
          status.getPath().toString());

      // test file status on a directory
      Path dir = new Path("/test/mkdirs");

      // test listStatus on a non-existent file/directory
      stats = fs.listStatus(dir);
      assertTrue(null == stats);
      try {
        status = fs.getFileStatus(dir);
        fail("getFileStatus of non-existent path should fail");
      } catch (FileNotFoundException fe) {
        assertTrue(fe.getMessage().startsWith("File does not exist"));
      }
      
      // create the directory
      assertTrue(fs.mkdirs(dir));
      assertTrue(fs.exists(dir));
      System.out.println("Dir : \"" + dir + "\"");

      // test getFileStatus on an empty directory
      status = fs.getFileStatus(dir);
      assertTrue(dir + " should be a directory", status.isDir());
      assertTrue(dir + " should be zero size ", status.getLen() == 0);
      assertEquals(fs.makeQualified(dir).toString(), 
          status.getPath().toString());

      // test listStatus on an empty directory
      stats = fs.listStatus(dir);
      assertEquals(dir + " should be empty", 0, stats.length);
      assertEquals(dir + " should be zero size ",
          0, fs.getContentSummary(dir).getLength());
      assertEquals(dir + " should be zero size using hftp",
          0, hftpfs.getContentSummary(dir).getLength());
      assertTrue(dir + " should be zero size ",
                 fs.getFileStatus(dir).getLen() == 0);
      System.out.println("Dir : \"" + dir + "\"");

      // create another file that is smaller than a block.
      //
      Path file2 = new Path(dir, "filestatus2.dat");
      writeFile(fs, file2, 1, blockSize/4, blockSize);
      System.out.println("Created file filestatus2.dat with one "
                         + " replicas.");
      checkFile(fs, file2, 1);
      System.out.println("Path : \"" + file2 + "\"");

      // verify file attributes
      status = fs.getFileStatus(file2);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      file2 = fs.makeQualified(file2);
      assertEquals(file2.toString(), status.getPath().toString());

      // create another file in the same directory
      Path file3 = new Path(dir, "filestatus3.dat");
      writeFile(fs, file3, 1, blockSize/4, blockSize);
      System.out.println("Created file filestatus3.dat with one "
                         + " replicas.");
      checkFile(fs, file3, 1);
      file3 = fs.makeQualified(file3);
      
      // verify that the size of the directory increased by the size 
      // of the two files
      final int expected = blockSize/2;  
      assertEquals(dir + " size should be " + expected, 
          expected, fs.getContentSummary(dir).getLength());
      assertEquals(dir + " size should be " + expected + " using hftp", 
          expected, hftpfs.getContentSummary(dir).getLength());
       
       // test listStatus on a non-empty directory
       stats = fs.listStatus(dir);
       assertEquals(dir + " should have two entries", 2, stats.length);
       assertEquals(file2.toString(), stats[0].getPath().toString());
       assertEquals(file3.toString(), stats[1].getPath().toString());

      // test iterative listing
      // now dir has 2 entries, create one more
      Path dir3 = fs.makeQualified(new Path(dir, "dir3"));
      fs.mkdirs(dir3);
      dir3 = fs.makeQualified(dir3);
      stats = fs.listStatus(dir);
      assertEquals(dir + " should have three entries", 3, stats.length);
      assertEquals(dir3.toString(), stats[0].getPath().toString());
      assertEquals(file2.toString(), stats[1].getPath().toString());
      assertEquals(file3.toString(), stats[2].getPath().toString());

      // now dir has 3 entries, create two more
      Path dir4 = fs.makeQualified(new Path(dir, "dir4"));
      fs.mkdirs(dir4);
      dir4 = fs.makeQualified(dir4);
      Path dir5 = fs.makeQualified(new Path(dir, "dir5"));
      fs.mkdirs(dir5);
      dir5 = fs.makeQualified(dir5);
      stats = fs.listStatus(dir);
      assertEquals(dir + " should have five entries", 5, stats.length);
      assertEquals(dir3.toString(), stats[0].getPath().toString());
      assertEquals(dir4.toString(), stats[1].getPath().toString());
      assertEquals(dir5.toString(), stats[2].getPath().toString());
      assertEquals(file2.toString(), stats[3].getPath().toString());
      assertEquals(file3.toString(), stats[4].getPath().toString());

      { //test permission error on hftp 
        fs.setPermission(dir, new FsPermission((short)0));
        try {
          final String username = UserGroupInformation.getCurrentUser().getShortUserName() + "1";
          final HftpFileSystem hftp2 = cluster.getHftpFileSystemAs(username, conf, "somegroup");
          hftp2.getContentSummary(dir);
          fail();
        } catch(IOException ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }
      }
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
