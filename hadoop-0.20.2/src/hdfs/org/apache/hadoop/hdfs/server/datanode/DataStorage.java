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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileUtil.HardLink;
import org.apache.hadoop.io.IOUtils;

/** 
 * Data storage information file.
 * <p>
 * @see Storage
 */
public class DataStorage extends Storage {
  // Constants
  final static String BLOCK_SUBDIR_PREFIX = "subdir";
  final static String BLOCK_FILE_PREFIX = "blk_";
  final static String COPY_FILE_PREFIX = "dncp_";
  
  private String storageID;

  DataStorage() {
    super(NodeType.DATA_NODE);
    storageID = "";
  }
  
  DataStorage(int nsID, long cT, String strgID) {
    super(NodeType.DATA_NODE, nsID, cT);
    this.storageID = strgID;
  }
  
  public DataStorage(StorageInfo storageInfo, String strgID) {
    super(NodeType.DATA_NODE, storageInfo);
    this.storageID = strgID;
  }

  public String getStorageID() {
    return storageID;
  }
  
  void setStorageID(String newStorageID) {
    this.storageID = newStorageID;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @param nsInfo namespace information
   * @param dataDirs array of data storage directories
   * @param startOpt startup option
   * @throws IOException
   */
  void recoverTransitionRead(NamespaceInfo nsInfo,
                             Collection<File> dataDirs,
                             StartupOption startOpt
                             ) throws IOException {
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Data-node and name-node layout versions must be the same.";
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    // Format and recover.
    this.storageID = "";
    this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
    ArrayList<StorageState> dataDirStates = new ArrayList<StorageState>(dataDirs.size());
    for(Iterator<File> it = dataDirs.iterator(); it.hasNext();) {
      File dataDir = it.next();
      StorageDirectory sd = new StorageDirectory(dataDir);
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        // sd is locked but not opened
        switch(curState) {
        case NORMAL:
          break;
        case NON_EXISTENT:
          // ignore this storage
          LOG.info("Storage directory " + dataDir + " does not exist.");
          it.remove();
          continue;
        case NOT_FORMATTED: // format
          LOG.info("Storage directory " + dataDir + " is not formatted.");
          LOG.info("Formatting ...");
          format(sd, nsInfo);
          break;
        default:  // recovery part is common
          sd.doRecover(curState);
        }
      } catch (IOException ioe) {
        sd.unlock();
        LOG.warn("Ignoring storage directory " + dataDir +
            " due to exception: " + StringUtils.stringifyException(ioe));
        // Continue with other good dirs
        continue;
      }
      // add to the storage list
      addStorageDir(sd);
      dataDirStates.add(curState);
    }

    if (dataDirs.size() == 0 || dataDirStates.size() == 0) {
      throw new IOException("All specified directories are not "
        +"accessible or do not exist.");
    }

    // 2. Do transitions
    // Each storage directory is treated individually.
    // During sturtup some of them can upgrade or rollback 
    // while others could be uptodate for the regular startup.
    for(int idx = 0; idx < getNumStorageDirs(); idx++) {
      doTransition(getStorageDir(idx), nsInfo, startOpt);
      assert this.getLayoutVersion() == nsInfo.getLayoutVersion() :
        "Data-node and name-node layout versions must be the same.";
      assert this.getCTime() == nsInfo.getCTime() :
        "Data-node and name-node CTimes must be the same.";
    }
    
    // 3. Update all storages. Some of them might have just been formatted.
    this.writeAll();
  }

  void format(StorageDirectory sd, NamespaceInfo nsInfo) throws IOException {
    sd.clearDirectory(); // create directory
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = nsInfo.getNamespaceID();
    this.cTime = 0;
    // store storageID as it currently is
    sd.write();
  }

  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.setFields(props, sd);
    props.setProperty("storageID", storageID);
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.getFields(props, sd);
    String ssid = props.getProperty("storageID");
    if (ssid == null ||
        !("".equals(storageID) || "".equals(ssid) ||
          storageID.equals(ssid)))
      throw new InconsistentFSStateException(sd.getRoot(),
                                             "has incompatible storage Id.");
    if ("".equals(storageID)) // update id only if it was empty
      storageID = ssid;
  }

  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    File oldF = new File(sd.getRoot(), "storage");
    if (!oldF.exists())
      return false;
    // check the layout version inside the storage file
    // Lock and Read old storage file
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    FileLock oldLock = oldFile.getChannel().tryLock();
    try {
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      oldLock.release();
      oldFile.close();
    }
    return true;
  }
  
  /**
   * Analize which and whether a transition of the fs state is required
   * and perform it if necessary.
   * 
   * Rollback if previousLV >= LAYOUT_VERSION && prevCTime <= namenode.cTime
   * Upgrade if this.LV > LAYOUT_VERSION || this.cTime < namenode.cTime
   * Regular startup if this.LV = LAYOUT_VERSION && this.cTime = namenode.cTime
   * 
   * @param sd  storage directory
   * @param nsInfo  namespace info
   * @param startOpt  startup option
   * @throws IOException
   */
  private void doTransition( StorageDirectory sd, 
                             NamespaceInfo nsInfo, 
                             StartupOption startOpt
                             ) throws IOException {
    if (startOpt == StartupOption.ROLLBACK)
      doRollback(sd, nsInfo); // rollback if applicable
    sd.read();
    checkVersionUpgradable(this.layoutVersion);
    assert this.layoutVersion >= FSConstants.LAYOUT_VERSION :
      "Future version is not allowed";
    if (getNamespaceID() != nsInfo.getNamespaceID())
      throw new IOException(
                            "Incompatible namespaceIDs in " + sd.getRoot().getCanonicalPath()
                            + ": namenode namespaceID = " + nsInfo.getNamespaceID() 
                            + "; datanode namespaceID = " + getNamespaceID());
    if (this.layoutVersion == FSConstants.LAYOUT_VERSION 
        && this.cTime == nsInfo.getCTime())
      return; // regular startup
    // verify necessity of a distributed upgrade
    verifyDistributedUpgradeProgress(nsInfo);
    if (this.layoutVersion > FSConstants.LAYOUT_VERSION
        || this.cTime < nsInfo.getCTime()) {
      doUpgrade(sd, nsInfo);  // upgrade
      return;
    }
    // layoutVersion == LAYOUT_VERSION && this.cTime > nsInfo.cTime
    // must shutdown
    throw new IOException("Datanode state: LV = " + this.getLayoutVersion() 
                          + " CTime = " + this.getCTime() 
                          + " is newer than the namespace state: LV = "
                          + nsInfo.getLayoutVersion() 
                          + " CTime = " + nsInfo.getCTime());
  }

  /**
   * Move current storage into a backup directory,
   * and hardlink all its blocks into the new current directory.
   * 
   * @param sd  storage directory
   * @throws IOException
   */
  void doUpgrade(StorageDirectory sd,
                 NamespaceInfo nsInfo
                 ) throws IOException {
    LOG.info("Upgrading storage directory " + sd.getRoot()
             + ".\n   old LV = " + this.getLayoutVersion()
             + "; old CTime = " + this.getCTime()
             + ".\n   new LV = " + nsInfo.getLayoutVersion()
             + "; new CTime = " + nsInfo.getCTime());
    File curDir = sd.getCurrentDir();
    File prevDir = sd.getPreviousDir();
    assert curDir.exists() : "Current directory must exist.";
    // delete previous dir before upgrading
    if (prevDir.exists())
      deleteDir(prevDir);
    File tmpDir = sd.getPreviousTmp();
    assert !tmpDir.exists() : "previous.tmp directory must not exist.";
    // rename current to tmp
    rename(curDir, tmpDir);
    // hardlink blocks
    linkBlocks(tmpDir, curDir, this.getLayoutVersion());
    // write version file
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    assert this.namespaceID == nsInfo.getNamespaceID() :
      "Data-node and name-node layout versions must be the same.";
    this.cTime = nsInfo.getCTime();
    sd.write();
    // rename tmp to previous
    rename(tmpDir, prevDir);
    LOG.info("Upgrade of " + sd.getRoot()+ " is complete.");
  }

  void doRollback( StorageDirectory sd,
                   NamespaceInfo nsInfo
                   ) throws IOException {
    File prevDir = sd.getPreviousDir();
    // regular startup if previous dir does not exist
    if (!prevDir.exists())
      return;
    DataStorage prevInfo = new DataStorage();
    StorageDirectory prevSD = prevInfo.new StorageDirectory(sd.getRoot());
    prevSD.read(prevSD.getPreviousVersionFile());

    // We allow rollback to a state, which is either consistent with
    // the namespace state or can be further upgraded to it.
    if (!(prevInfo.getLayoutVersion() >= FSConstants.LAYOUT_VERSION
          && prevInfo.getCTime() <= nsInfo.getCTime()))  // cannot rollback
      throw new InconsistentFSStateException(prevSD.getRoot(),
                                             "Cannot rollback to a newer state.\nDatanode previous state: LV = " 
                                             + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime() 
                                             + " is newer than the namespace state: LV = "
                                             + nsInfo.getLayoutVersion() + " CTime = " + nsInfo.getCTime());
    LOG.info("Rolling back storage directory " + sd.getRoot()
             + ".\n   target LV = " + nsInfo.getLayoutVersion()
             + "; target CTime = " + nsInfo.getCTime());
    File tmpDir = sd.getRemovedTmp();
    assert !tmpDir.exists() : "removed.tmp directory must not exist.";
    // rename current to tmp
    File curDir = sd.getCurrentDir();
    assert curDir.exists() : "Current directory must exist.";
    rename(curDir, tmpDir);
    // rename previous to current
    rename(prevDir, curDir);
    // delete tmp dir
    deleteDir(tmpDir);
    LOG.info("Rollback of " + sd.getRoot() + " is complete.");
  }

  void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists())
      return; // already discarded
    final String dataDirPath = sd.getRoot().getCanonicalPath();
    LOG.info("Finalizing upgrade for storage directory " 
             + dataDirPath 
             + ".\n   cur LV = " + this.getLayoutVersion()
             + "; cur CTime = " + this.getCTime());
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp
    rename(prevDir, tmpDir);

    // delete tmp dir in a separate thread
    new Daemon(new Runnable() {
        public void run() {
          try {
            deleteDir(tmpDir);
          } catch(IOException ex) {
            LOG.error("Finalize upgrade for " + dataDirPath + " failed.", ex);
          }
          LOG.info("Finalize upgrade for " + dataDirPath + " is complete.");
        }
        public String toString() { return "Finalize " + dataDirPath; }
      }).start();
  }
  
  void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      doFinalize(it.next());
    }
  }
  
  static void linkBlocks(File from, File to, int oldLV) throws IOException {
    if (!from.isDirectory()) {
      if (from.getName().startsWith(COPY_FILE_PREFIX)) {
        IOUtils.copyBytes(new FileInputStream(from), 
                          new FileOutputStream(to), 16*1024, true);
      } else {
        
        //check if we are upgrading from pre-generation stamp version.
        if (oldLV >= PRE_GENERATIONSTAMP_LAYOUT_VERSION) {
          // Link to the new file name.
          to = new File(convertMetatadataFileName(to.getAbsolutePath()));
        }
        
        HardLink.createHardLink(from, to);
      }
      return;
    }
    // from is a directory
    if (!to.mkdir())
      throw new IOException("Cannot create directory " + to);
    String[] blockNames = from.list(new java.io.FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.startsWith(BLOCK_SUBDIR_PREFIX) 
            || name.startsWith(BLOCK_FILE_PREFIX)
            || name.startsWith(COPY_FILE_PREFIX);
        }
      });
    
    for(int i = 0; i < blockNames.length; i++)
      linkBlocks(new File(from, blockNames[i]), 
                 new File(to, blockNames[i]), oldLV);
  }

  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    File oldF = new File(rootDir, "storage");
    if (oldF.exists())
      return;
    // recreate old storage file to let pre-upgrade versions fail
    if (!oldF.createNewFile())
      throw new IOException("Cannot create file " + oldF);
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    // write new version into old storage file
    try {
      writeCorruptedData(oldFile);
    } finally {
      oldFile.close();
    }
  }

  private void verifyDistributedUpgradeProgress(
                  NamespaceInfo nsInfo
                ) throws IOException {
    UpgradeManagerDatanode um = DataNode.getDataNode().upgradeManager;
    assert um != null : "DataNode.upgradeManager is null.";
    um.setUpgradeState(false, getLayoutVersion());
    um.initializeUpgrade(nsInfo);
  }
  
  private static final Pattern PRE_GENSTAMP_META_FILE_PATTERN = 
    Pattern.compile("(.*blk_[-]*\\d+)\\.meta$");
  /**
   * This is invoked on target file names when upgrading from pre generation 
   * stamp version (version -13) to correct the metatadata file name.
   * @param oldFileName
   * @return the new metadata file name with the default generation stamp.
   */
  private static String convertMetatadataFileName(String oldFileName) {
    Matcher matcher = PRE_GENSTAMP_META_FILE_PATTERN.matcher(oldFileName); 
    if (matcher.matches()) {
      //return the current metadata file name
      return FSDataset.getMetaFileName(matcher.group(1),
                                       Block.GRANDFATHER_GENERATION_STAMP); 
    }
    return oldFileName;
  }
}
