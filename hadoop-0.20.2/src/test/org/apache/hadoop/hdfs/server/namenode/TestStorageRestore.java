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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test restoring failed storage directories on checkpoint.
 */
public class TestStorageRestore {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";
  private Configuration config;
  private File hdfsDir=null;
  static final long seed = 0xAAAAEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  private File path1, path2, path3;
  private MiniDFSCluster cluster;

  private void writeFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
        fileSys.getConf().getInt("io.file.buffer.size", 4096),
        (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  @Before
  public void setUpNameDirs() throws Exception {
    config = new Configuration();
    String baseDir = System.getProperty("test.build.data", "/tmp");
    
    hdfsDir = new File(baseDir, "dfs");
    if (hdfsDir.exists()) {
      FileUtil.fullyDelete(hdfsDir);
    }
    
    hdfsDir.mkdir();
    path1 = new File(hdfsDir, "name1");
    path2 = new File(hdfsDir, "name2");
    path3 = new File(hdfsDir, "name3");
    
    path1.mkdir();
    path2.mkdir();
    path3.mkdir();

    String nameDir = new String(path1.getPath() + "," + path2.getPath());
    config.set("dfs.name.dir", nameDir);
    config.set("dfs.name.edits.dir", nameDir + "," + path3.getPath());
    config.set("fs.checkpoint.dir",new File(hdfsDir, "secondary").getPath());
 
    FileSystem.setDefaultUri(config, "hdfs://"+NAME_NODE_HOST + "0");
    config.set("dfs.secondary.http.address", "0.0.0.0:0");
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, true);
  }

  @After
  public void cleanUpNameDirs() throws Exception {
    if (hdfsDir.exists()) {
      FileUtil.fullyDelete(hdfsDir);
    }
  }
  
  /**
   * Remove edits and storage directories.
   */
  public void invalidateStorage(FSImage fi) throws IOException {
    fi.getEditLog().removeEditsAndStorageDir(2); // name3
    fi.getEditLog().removeEditsAndStorageDir(1); // name2
  }
  
  /**
   * Check the lengths of the image and edits files.
   */
  public void checkFiles(boolean expectValid) {
    final String imgName = 
      Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.IMAGE.getName();
    final String editsName = 
      Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.EDITS.getName();

    File fsImg1 = new File(path1, imgName);
    File fsImg2 = new File(path2, imgName);
    File fsImg3 = new File(path3, imgName);
    File fsEdits1 = new File(path1, editsName);
    File fsEdits2 = new File(path2, editsName);
    File fsEdits3 = new File(path3, editsName);

    if (expectValid) {
      assertTrue(fsImg1.length() == fsImg2.length());
      assertTrue(0 == fsImg3.length()); // Shouldn't be created
      assertTrue(fsEdits1.length() == fsEdits2.length());
      assertTrue(fsEdits1.length() == fsEdits3.length());
    } else {
      assertTrue(fsEdits1.length() != fsEdits2.length());
      assertTrue(fsEdits1.length() != fsEdits3.length());
    }
  }
  
  /**
   * test 
   * 1. create DFS cluster with 3 storage directories - 2 EDITS_IMAGE, 1 EDITS
   * 2. create a cluster and write a file
   * 3. corrupt/disable one storage (or two) by removing
   * 4. run doCheckpoint - it will fail on removed dirs (which
   * will invalidate the storages)
   * 5. write another file
   * 6. check that edits and fsimage differ 
   * 7. run doCheckpoint
   * 8. verify that all the image and edits files are the same.
   */
  @Test
  public void testStorageRestore() throws Exception {
    int numDatanodes = 2;
    cluster = new MiniDFSCluster(0, config, numDatanodes, true, 
        false, true,  null, null, null, null);
    cluster.waitActive();
    
    SecondaryNameNode secondary = new SecondaryNameNode(config);
    
    FileSystem fs = cluster.getFileSystem();
    Path path = new Path("/", "test");
    writeFile(fs, path, 2);
    
    invalidateStorage(cluster.getNameNode().getFSImage());

    path = new Path("/", "test1");
    writeFile(fs, path, 2);
    
    checkFiles(false);
    
    secondary.doCheckpoint();
    
    checkFiles(true);
    secondary.shutdown();
    cluster.shutdown();
  }
  
  /**
   * Test to simulate interleaved checkpointing by 2 2NNs after a storage
   * directory has been taken offline. The first will cause the directory to
   * come back online, but it won't have any valid contents. The second 2NN will
   * then try to perform a checkpoint. The NN should not serve up the image or
   * edits from the restored (empty) dir.
   */
  @Test
  public void testCheckpointWithRestoredDirectory() throws IOException {
    SecondaryNameNode secondary = null;
    try {
      cluster = new MiniDFSCluster(0, config, 1, true, false, true,
          null, null, null, null);
      cluster.waitActive();
      
      secondary = new SecondaryNameNode(config);
      FSImage fsImage = cluster.getNameNode().getFSImage();

      FileSystem fs = cluster.getFileSystem();
      Path path1 = new Path("/", "test");
      writeFile(fs, path1, 2);
  
      // Take name3 offline
      fsImage.getEditLog().removeEditsAndStorageDir(2);
      
      // Simulate a 2NN beginning a checkpoint, but not finishing. This will
      // cause name3 to be restored.
      cluster.getNameNode().rollEditLog();
      
      // Now another 2NN comes along to do a full checkpoint.
      secondary.doCheckpoint();
      
      // The created file should still exist in the in-memory FS state after the
      // checkpoint.
      assertTrue("File missing after checkpoint", fs.exists(path1));
      
      secondary.shutdown();
      
      // Restart the NN so it reloads the edits from on-disk.
      cluster.restartNameNode();
  
      // The created file should still exist after the restart.
      assertTrue("path should still exist after restart", fs.exists(path1));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (secondary != null) {
        secondary.shutdown();
      }
    }
  }
}