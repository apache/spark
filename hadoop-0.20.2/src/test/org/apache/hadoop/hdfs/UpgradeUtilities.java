/*
 * UpgradeUtilities.java
 *
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;

import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * This class defines a number of static helper methods used by the
 * DFS Upgrade unit tests.  By default, a singleton master populated storage
 * directory is created for a Namenode (contains edits, fsimage,
 * version, and time files) and a Datanode (contains version and
 * block files).  The master directories are lazily created.  They are then
 * copied by the createStorageDirs() method to create new storage
 * directories of the appropriate type (Namenode or Datanode).
 */
public class UpgradeUtilities {

  // Root scratch directory on local filesystem 
  private static File TEST_ROOT_DIR = new File(
      System.getProperty("test.build.data","/tmp").replace(' ', '+'));
  // The singleton master storage directory for Namenode
  private static File namenodeStorage = new File(TEST_ROOT_DIR, "namenodeMaster");
  // A checksum of the contents in namenodeStorage directory
  private static long namenodeStorageChecksum;
  // The namespaceId of the namenodeStorage directory
  private static int namenodeStorageNamespaceID;
  // The fsscTime of the namenodeStorage directory
  private static long namenodeStorageFsscTime;
  // The singleton master storage directory for Datanode
  private static File datanodeStorage = new File(TEST_ROOT_DIR, "datanodeMaster");
  // A checksum of the contents in datanodeStorage directory
  private static long datanodeStorageChecksum;
  
  /**
   * Initialize the data structures used by this class.  
   * IMPORTANT NOTE: This method must be called once before calling 
   *                 any other public method on this class.  
   * <p>
   * Creates a singleton master populated storage
   * directory for a Namenode (contains edits, fsimage,
   * version, and time files) and a Datanode (contains version and
   * block files).  This can be a lengthy operation.
   */
  public static void initialize() throws Exception {
    createEmptyDirs(new String[] {TEST_ROOT_DIR.toString()});
    Configuration config = new Configuration();
    config.set("dfs.name.dir", namenodeStorage.toString());
    config.set("dfs.data.dir", datanodeStorage.toString());
    MiniDFSCluster cluster = null;
    try {
      // format data-node
      createEmptyDirs(new String[] {datanodeStorage.toString()});
      
      // format and start NameNode and start DataNode
      NameNode.format(config); 
      cluster = new MiniDFSCluster(config, 1, StartupOption.REGULAR);
        
      NameNode namenode = cluster.getNameNode();
      namenodeStorageNamespaceID = namenode.versionRequest().getNamespaceID();
      namenodeStorageFsscTime = namenode.versionRequest().getCTime();
      
      FileSystem fs = FileSystem.get(config);
      Path baseDir = new Path("/TestUpgrade");
      fs.mkdirs(baseDir);
      
      // write some files
      int bufferSize = 4096;
      byte[] buffer = new byte[bufferSize];
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
      writeFile(fs, new Path(baseDir, "file1"), buffer, bufferSize);
      writeFile(fs, new Path(baseDir, "file2"), buffer, bufferSize);
      
      // save image
      namenode.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      namenode.saveNamespace();
      namenode.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      
      // write more files
      writeFile(fs, new Path(baseDir, "file3"), buffer, bufferSize);
      writeFile(fs, new Path(baseDir, "file4"), buffer, bufferSize);
    } finally {
      // shutdown
      if (cluster != null) cluster.shutdown();
      FileUtil.fullyDelete(new File(namenodeStorage,"in_use.lock"));
      FileUtil.fullyDelete(new File(datanodeStorage,"in_use.lock"));
    }
    namenodeStorageChecksum = checksumContents(
                                               NAME_NODE, new File(namenodeStorage,"current"));
    datanodeStorageChecksum = checksumContents(
                                               DATA_NODE, new File(datanodeStorage,"current"));
  }
  
  // Private helper method that writes a file to the given file system.
  private static void writeFile(FileSystem fs, Path path, byte[] buffer,
                                int bufferSize) throws IOException 
  {
    OutputStream out;
    out = fs.create(path, true, bufferSize, (short) 1, 1024);
    out.write(buffer, 0, bufferSize);
    out.close();
  }
  
  /**
   * Initialize dfs.name.dir and dfs.data.dir with the specified number of
   * directory entries. Also initialize dfs.blockreport.intervalMsec.
   */
  public static Configuration initializeStorageStateConf(int numDirs,
                                                         Configuration conf) {
    StringBuffer nameNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, "name1").toString());
    StringBuffer dataNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, "data1").toString());
    for (int i = 2; i <= numDirs; i++) {
      nameNodeDirs.append("," + new File(TEST_ROOT_DIR, "name"+i));
      dataNodeDirs.append("," + new File(TEST_ROOT_DIR, "data"+i));
    }
    if (conf == null) {
      conf = new Configuration();
    }
    conf.set("dfs.name.dir", nameNodeDirs.toString());
    conf.set("dfs.data.dir", dataNodeDirs.toString());
    conf.setInt("dfs.blockreport.intervalMsec", 10000);
    return conf;
  }
  
  /**
   * Create empty directories.  If a specified directory already exists
   * then it is first removed.
   */
  public static void createEmptyDirs(String[] dirs) throws IOException {
    for (String d : dirs) {
      File dir = new File(d);
      if (dir.exists()) {
        FileUtil.fullyDelete(dir);
      }
      dir.mkdirs();
    }
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * of the given node type.
   */
  public static long checksumMasterContents(NodeType nodeType) throws IOException {
    if (nodeType == NAME_NODE) {
      return namenodeStorageChecksum;
    } else {
      return datanodeStorageChecksum;
    }
  }
  
  /**
   * Compute the checksum of all the files in the specified directory.
   * The contents of subdirectories are not included. This method provides
   * an easy way to ensure equality between the contents of two directories.
   *
   * @param nodeType if DATA_NODE then any file named "VERSION" is ignored.
   *    This is because this file file is changed every time
   *    the Datanode is started.
   * @param dir must be a directory. Subdirectories are ignored.
   *
   * @throws IllegalArgumentException if specified directory is not a directory
   * @throws IOException if an IOException occurs while reading the files
   * @return the computed checksum value
   */
  public static long checksumContents(NodeType nodeType, File dir) throws IOException {
    if (!dir.isDirectory()) {
      throw new IllegalArgumentException(
                                         "Given argument is not a directory:" + dir);
    }
    File[] list = dir.listFiles();
    Arrays.sort(list);
    CRC32 checksum = new CRC32();
    for (int i = 0; i < list.length; i++) {
      if (!list[i].isFile()) {
        continue;
      }
      // skip VERSION file for DataNodes
      if (nodeType == DATA_NODE && list[i].getName().equals("VERSION")) {
        continue; 
      }
      FileInputStream fis = null;
      try {
        fis = new FileInputStream(list[i]);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
          checksum.update(buffer, 0, bytesRead);
        }
      } finally {
        if(fis != null) {
          fis.close();
        }
      }
    }
    return checksum.getValue();
  }
  
  /**
   * Simulate the <code>dfs.name.dir</code> or <code>dfs.data.dir</code>
   * of a populated DFS filesystem.
   *
   * This method creates and populates the directory specified by
   *  <code>parent/dirName</code>, for each parent directory.
   * The contents of the new directories will be
   * appropriate for the given node type.  If the directory does not
   * exist, it will be created.  If the directory already exists, it
   * will first be deleted.
   *
   * By default, a singleton master populated storage
   * directory is created for a Namenode (contains edits, fsimage,
   * version, and time files) and a Datanode (contains version and
   * block files).  These directories are then
   * copied by this method to create new storage
   * directories of the appropriate type (Namenode or Datanode).
   *
   * @return the array of created directories
   */
  public static File[] createStorageDirs(NodeType nodeType, String[] parents, String dirName) throws Exception {
    File[] retVal = new File[parents.length];
    for (int i = 0; i < parents.length; i++) {
      File newDir = new File(parents[i], dirName);
      createEmptyDirs(new String[] {newDir.toString()});
      LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
      switch (nodeType) {
      case NAME_NODE:
        localFS.copyToLocalFile(new Path(namenodeStorage.toString(), "current"),
                                new Path(newDir.toString()),
                                false);
        Path newImgDir = new Path(newDir.getParent(), "image");
        if (!localFS.exists(newImgDir))
          localFS.copyToLocalFile(
              new Path(namenodeStorage.toString(), "image"),
              newImgDir,
              false);
        break;
      case DATA_NODE:
        localFS.copyToLocalFile(new Path(datanodeStorage.toString(), "current"),
                                new Path(newDir.toString()),
                                false);
        Path newStorageFile = new Path(newDir.getParent(), "storage");
        if (!localFS.exists(newStorageFile))
          localFS.copyToLocalFile(
              new Path(datanodeStorage.toString(), "storage"),
              newStorageFile,
              false);
        break;
      }
      retVal[i] = newDir;
    }
    return retVal;
  }
  
  /**
   * Create a <code>version</code> file inside the specified parent
   * directory.  If such a file already exists, it will be overwritten.
   * The given version string will be written to the file as the layout
   * version. None of the parameters may be null.
   *
   * @param version
   *
   * @return the created version file
   */
  public static File[] createVersionFile(NodeType nodeType, File[] parent,
                                         StorageInfo version) throws IOException 
  {
    Storage storage = null;
    File[] versionFiles = new File[parent.length];
    for (int i = 0; i < parent.length; i++) {
      File versionFile = new File(parent[i], "VERSION");
      FileUtil.fullyDelete(versionFile);
      switch (nodeType) {
      case NAME_NODE:
        storage = new FSImage(version);
        break;
      case DATA_NODE:
        storage = new DataStorage(version, "doNotCare");
        break;
      }
      StorageDirectory sd = storage.new StorageDirectory(parent[i].getParentFile());
      sd.write(versionFile);
      versionFiles[i] = versionFile;
    }
    return versionFiles;
  }
  
  /**
   * Corrupt the specified file.  Some random bytes within the file
   * will be changed to some random values.
   *
   * @throws IllegalArgumentException if the given file is not a file
   * @throws IOException if an IOException occurs while reading or writing the file
   */
  public static void corruptFile(File file) throws IOException {
    if (!file.isFile()) {
      throw new IllegalArgumentException(
                                         "Given argument is not a file:" + file);
    }
    RandomAccessFile raf = new RandomAccessFile(file,"rws");
    Random random = new Random();
    for (long i = 0; i < raf.length(); i++) {
      raf.seek(i);
      if (random.nextBoolean()) {
        raf.writeByte(random.nextInt());
      }
    }
    raf.close();
  }
  
  /**
   * Return the layout version inherent in the current version
   * of the Namenode, whether it is running or not.
   */
  public static int getCurrentLayoutVersion() {
    return FSConstants.LAYOUT_VERSION;
  }
  
  /**
   * Return the namespace ID inherent in the currently running
   * Namenode.  If no Namenode is running, return the namespace ID of
   * the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static int getCurrentNamespaceID(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNode().versionRequest().getNamespaceID();
    }
    return namenodeStorageNamespaceID;
  }
  
  /**
   * Return the File System State Creation Timestamp (FSSCTime) inherent
   * in the currently running Namenode.  If no Namenode is running,
   * return the FSSCTime of the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static long getCurrentFsscTime(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNode().versionRequest().getCTime();
    }
    return namenodeStorageFsscTime;
  }
}

