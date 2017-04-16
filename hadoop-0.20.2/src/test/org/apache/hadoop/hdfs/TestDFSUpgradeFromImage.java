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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This tests data transfer protocol handling in the Datanode. It sends
 * various forms of wrong data and verifies that Datanode handles it well.
 * 
 * This test uses the following two file from src/test/.../dfs directory :
 *   1) hadoop-version-dfs-dir.tgz : contains DFS directories.
 *   2) hadoop-dfs-dir.txt : checksums that are compared in this test.
 * Please read hadoop-dfs-dir.txt for more information.  
 */
public class TestDFSUpgradeFromImage extends TestCase {
  
  private static final Log LOG = LogFactory.getLog(
                    "org.apache.hadoop.hdfs.TestDFSUpgradeFromImage");
  
  public int numDataNodes = 4;
  
  private static class ReferenceFileInfo {
    String path;
    long checksum;
  }
  
  LinkedList<ReferenceFileInfo> refList = new LinkedList<ReferenceFileInfo>();
  Iterator<ReferenceFileInfo> refIter;
  
  boolean printChecksum = false;
  
  protected void setUp() throws IOException {
    unpackStorage();
  }

  public void unpackStorage() throws IOException {
    String tarFile = System.getProperty("test.cache.data", "build/test/cache") +
                     "/hadoop-14-dfs-dir.tgz";
    String dataDir = System.getProperty("test.build.data", "build/test/data");
    File dfsDir = new File(dataDir, "dfs");
    if ( dfsDir.exists() && !FileUtil.fullyDelete(dfsDir) ) {
      throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
    }
    FileUtil.unTar(new File(tarFile), new File(dataDir));
    //Now read the reference info
    
    BufferedReader reader = new BufferedReader( 
                        new FileReader(System.getProperty("test.cache.data", "build/test/cache") +
                                       "/hadoop-dfs-dir.txt"));
    String line;
    while ( (line = reader.readLine()) != null ) {
      
      line = line.trim();
      if (line.length() <= 0 || line.startsWith("#")) {
        continue;
      }
      String[] arr = line.split("\\s+\t\\s+");
      if (arr.length < 1) {
        continue;
      }
      if (arr[0].equals("printChecksums")) {
        printChecksum = true;
        break;
      }
      if (arr.length < 2) {
        continue;
      }
      ReferenceFileInfo info = new ReferenceFileInfo();
      info.path = arr[0];
      info.checksum = Long.parseLong(arr[1]);
      refList.add(info);
    }
    reader.close();
  }

  private void verifyChecksum(String path, long checksum) throws IOException {
    if ( refIter == null ) {
      refIter = refList.iterator();
    }
    
    if ( printChecksum ) {
      LOG.info("CRC info for reference file : " + path + " \t " + checksum);
    } else {
      if ( !refIter.hasNext() ) {
        throw new IOException("Checking checksum for " + path +
                              "Not enough elements in the refList");
      }
      ReferenceFileInfo info = refIter.next();
      // The paths are expected to be listed in the same order 
      // as they are traversed here.
      assertEquals(info.path, path);
      assertEquals("Checking checksum for " + path, info.checksum, checksum);
    }
  }
  
  CRC32 overallChecksum = new CRC32();
  
  private void verifyDir(DistributedFileSystem dfs, Path dir) 
                                           throws IOException {
    
    FileStatus[] fileArr = dfs.listStatus(dir);
    TreeMap<Path, Boolean> fileMap = new TreeMap<Path, Boolean>();
    
    for(FileStatus file : fileArr) {
      fileMap.put(file.getPath(), Boolean.valueOf(file.isDir()));
    }
    
    for(Iterator<Path> it = fileMap.keySet().iterator(); it.hasNext();) {
      Path path = it.next();
      boolean isDir = fileMap.get(path);
      
      String pathName = path.toUri().getPath();
      overallChecksum.update(pathName.getBytes());
      
      if ( isDir ) {
        verifyDir(dfs, path);
      } else {
        // this is not a directory. Checksum the file data.
        CRC32 fileCRC = new CRC32();
        FSInputStream in = dfs.dfs.open(pathName);
        byte[] buf = new byte[4096];
        int nRead = 0;
        while ( (nRead = in.read(buf, 0, buf.length)) > 0 ) {
          fileCRC.update(buf, 0, nRead);
        }
        
        verifyChecksum(pathName, fileCRC.getValue());
      }
    }
  }
  
  private void verifyFileSystem(DistributedFileSystem dfs) throws IOException {
  
    verifyDir(dfs, new Path("/"));
    
    verifyChecksum("overallCRC", overallChecksum.getValue());
    
    if ( printChecksum ) {
      throw new IOException("Checksums are written to log as requested. " +
                            "Throwing this exception to force an error " +
                            "for this test.");
    }
  }
  
  public void testUpgradeFromImage() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      if (System.getProperty("test.build.data") == null) { // to allow test to be run outside of Ant
        System.setProperty("test.build.data", "build/test/data");
      }
      conf.setInt("dfs.datanode.scan.period.hours", -1); // block scanning off
      cluster = new MiniDFSCluster(0, conf, numDataNodes, false, true,
                                   StartupOption.UPGRADE, null);
      cluster.waitActive();
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      DFSClient dfsClient = dfs.dfs;
      //Safemode will be off only after upgrade is complete. Wait for it.
      while ( dfsClient.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_GET) ) {
        LOG.info("Waiting for SafeMode to be OFF.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
      }

      verifyFileSystem(dfs);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
