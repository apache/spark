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

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.mortbay.log.Log;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 *************************************************/
class FSDirectory implements FSConstants, Closeable {

  final FSNamesystem namesystem;
  final INodeDirectoryWithQuota rootDir;
  FSImage fsImage;  
  private boolean ready = false;
  // Metrics record
  private MetricsRecord directoryMetrics = null;
  private final int lsLimit;  // max list limit

  /** Access an existing dfs name directory. */
  FSDirectory(FSNamesystem ns, Configuration conf) {
    this(new FSImage(), ns, conf);
    fsImage.setCheckpointDirectories(FSImage.getCheckpointDirs(conf, null),
                                FSImage.getCheckpointEditsDirs(conf, null));
  }

  FSDirectory(FSImage fsImage, FSNamesystem ns, Configuration conf) {
    rootDir = new INodeDirectoryWithQuota(INodeDirectory.ROOT_NAME,
        ns.createFsOwnerPermissions(new FsPermission((short)0755)),
        Integer.MAX_VALUE, -1);
    this.fsImage = fsImage;
    namesystem = ns;
    int configuredLimit = conf.getInt(
        DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit>0 ? 
        configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
    boolean restore = conf.getBoolean
        (DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY,
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT);
    if (conf.getBoolean
        (DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY_DEPRECATED,
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      NameNode.LOG.warn(
          DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY_DEPRECATED + " is " +
          "deprecated; please use " +
          DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY + " instead.");
      restore = true;
    }
    if (restore) {
      fsImage.setRestoreFailedStorage(true);
    }
    initialize(conf);
  }
    
  private void initialize(Configuration conf) {
    MetricsContext metricsContext = MetricsUtil.getContext("dfs");
    directoryMetrics = MetricsUtil.createRecord(metricsContext, "FSDirectory");
    directoryMetrics.setTag("sessionId", conf.get("session.id"));
  }

  void loadFSImage(Collection<File> dataDirs,
                   Collection<File> editsDirs,
                   StartupOption startOpt) throws IOException {
    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      fsImage.setStorageDirectories(dataDirs, editsDirs);
      fsImage.format();
      startOpt = StartupOption.REGULAR;
    }
    try {
      if (fsImage.recoverTransitionRead(dataDirs, editsDirs, startOpt)) {
        fsImage.saveNamespace(true);
      }
      FSEditLog editLog = fsImage.getEditLog();
      assert editLog != null : "editLog must be initialized";
      if (!editLog.isOpen())
        editLog.open();
      fsImage.setCheckpointDirectories(null, null);
    } catch(IOException e) {
      fsImage.close();
      throw e;
    }
    synchronized (this) {
      this.ready = true;
      this.notifyAll();
    }
  }

  private void incrDeletedFileCount(int count) {
    directoryMetrics.incrMetric("files_deleted", count);
    directoryMetrics.update();
  }
    
  /**
   * Shutdown the filestore
   */
  public void close() throws IOException {
    fsImage.close();
  }

  /**
   * Block until the object is ready to be used.
   */
  void waitForReady() {
    if (!ready) {
      synchronized (this) {
        while (!ready) {
          try {
            this.wait(5000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
  }

  /**
   * Add the given filename to the fs.
   */
  INodeFileUnderConstruction addFile(String path, 
                PermissionStatus permissions,
                short replication,
                long preferredBlockSize,
                String clientName,
                String clientMachine,
                DatanodeDescriptor clientNode,
                long generationStamp) 
                throws IOException {
    waitForReady();

    // Always do an implicit mkdirs for parent directory tree.
    long modTime = FSNamesystem.now();
    if (!mkdirs(new Path(path).getParent().toString(), permissions, true,
        modTime)) {
      return null;
    }
    INodeFileUnderConstruction newNode = new INodeFileUnderConstruction(
                                 permissions,replication,
                                 preferredBlockSize, modTime, clientName, 
                                 clientMachine, clientNode);
    synchronized (rootDir) {
      newNode = addNode(path, newNode, -1, false);
    }
    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
                                   +"failed to add "+path
                                   +" to the file system");
      return null;
    }
    // add create file record to log, record new generation stamp
    fsImage.getEditLog().logOpenFile(path, newNode);

    NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                  +path+" is added to the file system");
    return newNode;
  }

  /**
   */
  INode unprotectedAddFile( String path, 
                            PermissionStatus permissions,
                            Block[] blocks, 
                            short replication,
                            long modificationTime,
                            long atime,
                            long preferredBlockSize) {
    INode newNode;
    long diskspace = -1; // unknown
    if (blocks == null)
      newNode = new INodeDirectory(permissions, modificationTime);
    else {
      newNode = new INodeFile(permissions, blocks.length, replication,
                              modificationTime, atime, preferredBlockSize);
      diskspace = ((INodeFile)newNode).diskspaceConsumed(blocks);
    }
    synchronized (rootDir) {
      try {
        newNode = addNode(path, newNode, diskspace, false);
        if(newNode != null && blocks != null) {
          int nrBlocks = blocks.length;
          // Add file->block mapping
          INodeFile newF = (INodeFile)newNode;
          for (int i = 0; i < nrBlocks; i++) {
            newF.setBlock(i, namesystem.blocksMap.addINode(blocks[i], newF));
          }
        }
      } catch (IOException e) {
        return null;
      }
      return newNode;
    }
  }

  INodeDirectory addToParent( String src,
                              INodeDirectory parentINode,
                              PermissionStatus permissions,
                              Block[] blocks, 
                              short replication,
                              long modificationTime,
                              long atime,
                              long nsQuota,
                              long dsQuota,
                              long preferredBlockSize) {
    // NOTE: This does not update space counts for parents
    // create new inode
    INode newNode;
    if (blocks == null) {
      if (nsQuota >= 0 || dsQuota >= 0) {
        newNode = new INodeDirectoryWithQuota(
            permissions, modificationTime, nsQuota, dsQuota);
      } else {
        newNode = new INodeDirectory(permissions, modificationTime);
      }
    } else 
      newNode = new INodeFile(permissions, blocks.length, replication,
                              modificationTime, atime, preferredBlockSize);
    // add new node to the parent
    INodeDirectory newParent = null;
    synchronized (rootDir) {
      try {
        newParent = rootDir.addToParent(src, newNode, parentINode, false);
      } catch (FileNotFoundException e) {
        return null;
      }
      if(newParent == null)
        return null;
      if(blocks != null) {
        int nrBlocks = blocks.length;
        // Add file->block mapping
        INodeFile newF = (INodeFile)newNode;
        for (int i = 0; i < nrBlocks; i++) {
          newF.setBlock(i, namesystem.blocksMap.addINode(blocks[i], newF));
        }
      }
    }
    return newParent;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  Block addBlock(String path, INode[] inodes, Block block) throws IOException {
    waitForReady();

    synchronized (rootDir) {
      INodeFile fileNode = (INodeFile) inodes[inodes.length-1];

      // check quota limits and updated space consumed
      updateCount(inodes, inodes.length-1, 0,
          fileNode.getPreferredBlockSize()*fileNode.getReplication(), true);
      
      // associate the new list of blocks with this file
      namesystem.blocksMap.addINode(block, fileNode);
      BlockInfo blockInfo = namesystem.blocksMap.getStoredBlock(block);
      fileNode.addBlock(blockInfo);

      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    + path + " with " + block
                                    + " block is added to the in-memory "
                                    + "file system");
    }
    return block;
  }

  /**
   * Persist the block list for the inode.
   */
  void persistBlocks(String path, INodeFileUnderConstruction file) 
                     throws IOException {
    waitForReady();

    synchronized (rootDir) {
      fsImage.getEditLog().logOpenFile(path, file);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.persistBlocks: "
                                    +path+" with "+ file.getBlocks().length 
                                    +" blocks is persisted to the file system");
    }
  }

  /**
   * Close file.
   */
  void closeFile(String path, INodeFile file) throws IOException {
    waitForReady();
    synchronized (rootDir) {
      // file is closed
      fsImage.getEditLog().logCloseFile(path, file);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.closeFile: "
                                    +path+" with "+ file.getBlocks().length 
                                    +" blocks is persisted to the file system");
      }
    }
  }

  /**
   * Remove a block to the file.
   */
  boolean removeBlock(String path, INodeFileUnderConstruction fileNode, 
                      Block block) throws IOException {
    waitForReady();

    synchronized (rootDir) {
      // modify file-> block and blocksMap
      fileNode.removeBlock(block);
      namesystem.blocksMap.removeINode(block);
      // If block is removed from blocksMap remove it from corruptReplicasMap
      namesystem.corruptReplicas.removeFromCorruptReplicasMap(block);

      // write modified block locations to log
      fsImage.getEditLog().logOpenFile(path, fileNode);
      NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                                    +path+" with "+block
                                    +" block is added to the file system");
    }
    return true;
  }

  /**
   * @see #unprotectedRenameTo(String, String, long)
   */
  boolean renameTo(String src, String dst) throws QuotaExceededException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
                                  +src+" to "+dst);
    }
    waitForReady();
    long now = FSNamesystem.now();
    if (!unprotectedRenameTo(src, dst, now))
      return false;
    fsImage.getEditLog().logRename(src, dst, now);
    return true;
  }

  /** Change a path name
   * 
   * @param src source path
   * @param dst destination path
   * @return true if rename succeeds; false otherwise
   * @throws QuotaExceededException if the operation violates any quota limit
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp) 
  throws QuotaExceededException {
    synchronized (rootDir) {
      INode[] srcInodes = rootDir.getExistingPathINodes(src);

      // check the validation of the source
      if (srcInodes[srcInodes.length-1] == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst
            + " because source does not exist");
        return false;
      } 
      if (srcInodes.length == 1) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst+ " because source is the root");
        return false;
      }
      if (isDir(dst)) {
        dst += Path.SEPARATOR + new Path(src).getName();
      }
      
      // check the validity of the destination
      if (dst.equals(src)) {
        return true;
      }
      // dst cannot be directory or a file under src
      if (dst.startsWith(src) && 
          dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst
            + " because destination starts with src");
        return false;
      }
      
      byte[][] dstComponents = INode.getPathComponents(dst);
      INode[] dstInodes = new INode[dstComponents.length];
      rootDir.getExistingPathINodes(dstComponents, dstInodes);
      if (dstInodes[dstInodes.length-1] != null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                     +"failed to rename "+src+" to "+dst+ 
                                     " because destination exists");
        return false;
      }
      if (dstInodes[dstInodes.length-2] == null) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            +"failed to rename "+src+" to "+dst+ 
            " because destination's parent does not exist");
        return false;
      }
      
      // Ensure dst has quota to accommodate rename
      verifyQuotaForRename(srcInodes,dstInodes);
      
      INode dstChild = null;
      INode srcChild = null;
      String srcChildName = null;
      try {
        // remove src
        srcChild = removeChild(srcInodes, srcInodes.length-1);
        if (srcChild == null) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
              + "failed to rename " + src + " to " + dst
              + " because the source can not be removed");
          return false;
        }
        srcChildName = srcChild.getLocalName();
        srcChild.setLocalName(dstComponents[dstInodes.length-1]);
        
        // add src to the destination
        dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1,
            srcChild, -1, false);
        if (dstChild != null) {
          srcChild = null;
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: " + src
                    + " is renamed to " + dst);
          }
          // update modification time of dst and the parent of src
          srcInodes[srcInodes.length-2].setModificationTime(timestamp);
          dstInodes[dstInodes.length-2].setModificationTime(timestamp);
          return true;
        }
      } finally {
        if (dstChild == null && srcChild != null) {
          // put it back
          srcChild.setLocalName(srcChildName);
          addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, srcChild, -1,
              false);
        }
      }
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          +"failed to rename "+src+" to "+dst);
      return false;
    }
  }

  /**
   * Set file replication
   * 
   * @param src file name
   * @param replication new replication
   * @param oldReplication old replication - output parameter
   * @return array of file blocks
   * @throws IOException
   */
  Block[] setReplication(String src, 
                         short replication,
                         int[] oldReplication
                         ) throws IOException {
    waitForReady();
    Block[] fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
    if (fileBlocks != null)  // log replication change
      fsImage.getEditLog().logSetReplication(src, replication);
    return fileBlocks;
  }

  Block[] unprotectedSetReplication( String src, 
                                     short replication,
                                     int[] oldReplication
                                     ) throws IOException {
    if (oldReplication == null)
      oldReplication = new int[1];
    oldReplication[0] = -1;
    Block[] fileBlocks = null;
    synchronized(rootDir) {
      INode[] inodes = rootDir.getExistingPathINodes(src);
      INode inode = inodes[inodes.length - 1];
      if (inode == null)
        return null;
      if (inode.isDirectory())
        return null;
      INodeFile fileNode = (INodeFile)inode;
      oldReplication[0] = fileNode.getReplication();

      // check disk quota
      long dsDelta = (replication - oldReplication[0]) *
           (fileNode.diskspaceConsumed()/oldReplication[0]);
      updateCount(inodes, inodes.length-1, 0, dsDelta, true);

      fileNode.setReplication(replication);
      fileBlocks = fileNode.getBlocks();
    }
    return fileBlocks;
  }

  /**
   * Get the blocksize of a file
   * @param filename the filename
   * @return the number of bytes 
   * @throws IOException if it is a directory or does not exist.
   */
  long getPreferredBlockSize(String filename) throws IOException {
    synchronized (rootDir) {
      INode fileNode = rootDir.getNode(filename);
      if (fileNode == null) {
        throw new IOException("Unknown file: " + filename);
      }
      if (fileNode.isDirectory()) {
        throw new IOException("Getting block size of a directory: " + 
                              filename);
      }
      return ((INodeFile)fileNode).getPreferredBlockSize();
    }
  }

  boolean exists(String src) {
    src = normalizePath(src);
    synchronized(rootDir) {
      INode inode = rootDir.getNode(src);
      if (inode == null) {
         return false;
      }
      return inode.isDirectory()? true: ((INodeFile)inode).getBlocks() != null;
    }
  }

  void setPermission(String src, FsPermission permission
      ) throws IOException {
    unprotectedSetPermission(src, permission);
    fsImage.getEditLog().logSetPermissions(src, permission);
  }

  void unprotectedSetPermission(String src, FsPermission permissions) throws FileNotFoundException {
    synchronized(rootDir) {
        INode inode = rootDir.getNode(src);
        if(inode == null)
            throw new FileNotFoundException("File does not exist: " + src);
        inode.setPermission(permissions);
    }
  }

  void setOwner(String src, String username, String groupname
      ) throws IOException {
    unprotectedSetOwner(src, username, groupname);
    fsImage.getEditLog().logSetOwner(src, username, groupname);
  }

  void unprotectedSetOwner(String src, String username, String groupname) throws FileNotFoundException {
    synchronized(rootDir) {
      INode inode = rootDir.getNode(src);
      if(inode == null)
          throw new FileNotFoundException("File does not exist: " + src);
      if (username != null) {
        inode.setUser(username);
      }
      if (groupname != null) {
        inode.setGroup(groupname);
      }
    }
  }
    
  /**
   * Remove the file from management, return blocks
   */
  INode delete(String src) {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: "+src);
    }
    waitForReady();
    long now = FSNamesystem.now();
    INode deletedNode = unprotectedDelete(src, now);
    if (deletedNode != null) {
      fsImage.getEditLog().logDelete(src, now);
    }
    return deletedNode;
  }
  
  /** Return if a directory is empty or not **/
  boolean isDirEmpty(String src) {
	   boolean dirNotEmpty = true;
    if (!isDir(src)) {
      return true;
    }
    synchronized(rootDir) {
      INode targetNode = rootDir.getNode(src);
      assert targetNode != null : "should be taken care in isDir() above";
      if (((INodeDirectory)targetNode).getChildren().size() != 0) {
        dirNotEmpty = false;
      }
    }
    return dirNotEmpty;
  }
  
  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param src a string representation of a path to an inode
   * @param modificationTime the time the inode is removed
   * @param deletedBlocks the place holder for the blocks to be removed
   * @return if the deletion succeeds
   */ 
  INode unprotectedDelete(String src, long modificationTime) {
    src = normalizePath(src);

    synchronized (rootDir) {
      INode[] inodes =  rootDir.getExistingPathINodes(src);
      INode targetNode = inodes[inodes.length-1];

      if (targetNode == null) { // non-existent src
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
            +"failed to remove "+src+" because it does not exist");
        return null;
      } else if (inodes.length == 1) { // src is the root
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
            "failed to remove " + src +
            " because the root is not allowed to be deleted");
        return null;
      } else {
        try {
          // Remove the node from the namespace
          removeChild(inodes, inodes.length-1);
          // set the parent's modification time
          inodes[inodes.length-2].setModificationTime(modificationTime);
          // GC all the blocks underneath the node.
          ArrayList<Block> v = new ArrayList<Block>();
          int filesRemoved = targetNode.collectSubtreeBlocksAndClear(v);
          incrDeletedFileCount(filesRemoved);
          namesystem.removePathAndBlocks(src, v);
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
              +src+" is removed");
          }
          return targetNode;
        } catch (IOException e) {
          NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
              "failed to remove " + src + " because " + e.getMessage());
          return null;
        }
      }
    }
  }

  /**
   * Replaces the specified inode with the specified one.
   */
  void replaceNode(String path, INodeFile oldnode, INodeFile newnode)
      throws IOException {
    synchronized (rootDir) {
      //
      // Remove the node from the namespace 
      //
      if (!oldnode.removeNode()) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.replaceNode: " +
                                     "failed to remove " + path);
        throw new IOException("FSDirectory.replaceNode: " +
                              "failed to remove " + path);
      } 
      
      /* Currently oldnode and newnode are assumed to contain the same
       * blocks. Otherwise, blocks need to be removed from the blocksMap.
       */
      
      rootDir.addNode(path, newnode); 

      int index = 0;
      for (Block b : newnode.getBlocks()) {
        BlockInfo info = namesystem.blocksMap.addINode(b, newnode);
        newnode.setBlock(index, info); // inode refers to the block in BlocksMap
        index++;
      }
    }
  }

  /**
   * Get a partial listing of the indicated directory
   * 
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @return a partial listing starting after startAfter 
   */
  DirectoryListing getListing(String src, byte[] startAfter) {
    String srcs = normalizePath(src);

    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null)
        return null;
      
      if (!targetNode.isDirectory()) {
        return new DirectoryListing(new HdfsFileStatus[]{createFileStatus(
            HdfsFileStatus.EMPTY_NAME, targetNode)}, 0);
      }
      INodeDirectory dirInode = (INodeDirectory)targetNode; 
      List<INode> contents = dirInode.getChildren();
      int startChild = dirInode.nextChild(startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren-startChild, this.lsLimit);
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      for (int i=0; i<numOfListing; i++) {
        INode cur = contents.get(startChild+i);
        listing[i] = createFileStatus(cur.name, cur);
      }
      return new DirectoryListing(
          listing, totalNumChildren-startChild-numOfListing);
    }
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getFileInfo(String src) {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null) {
        return null;
      }
      else {
        return createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode);
      }
    }
  }

  /**
   * Get the blocks associated with the file.
   */
  Block[] getFileBlocks(String src) {
    waitForReady();
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(src);
      if (targetNode == null)
        return null;
      if(targetNode.isDirectory())
        return null;
      return ((INodeFile)targetNode).getBlocks();
    }
  }

  /**
   * Get {@link INode} associated with the file.
   */
  INodeFile getFileINode(String src) {
    synchronized (rootDir) {
      INode inode = rootDir.getNode(src);
      if (inode == null || inode.isDirectory())
        return null;
      return (INodeFile)inode;
    }
  }

  /**
   * Retrieve the existing INodes along the given path.
   * 
   * @param path the path to explore
   * @return INodes array containing the existing INodes in the order they
   *         appear when following the path from the root INode to the
   *         deepest INodes. The array size will be the number of expected
   *         components in the path, and non existing components will be
   *         filled with null
   *         
   * @see INodeDirectory#getExistingPathINodes(byte[][], INode[])
   */
  INode[] getExistingPathINodes(String path) {
    synchronized (rootDir){
      return rootDir.getExistingPathINodes(path);
    }
  }
  
  /** 
   * Check whether the filepath could be created
   */
  boolean isValidToCreate(String src) {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      if (srcs.startsWith("/") && 
          !srcs.endsWith("/") && 
          rootDir.getNode(srcs) == null) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) {
    synchronized (rootDir) {
      INode node = rootDir.getNode(normalizePath(src));
      return node != null && node.isDirectory();
    }
  }

  /** Updates namespace and diskspace consumed for all
   * directories until the parent directory of file represented by path.
   * 
   * @param path path for the file.
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFound if path does not exist.
   */
  void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
                                         throws QuotaExceededException,
                                                FileNotFoundException {
    synchronized (rootDir) {
      INode[] inodes = rootDir.getExistingPathINodes(path);
      int len = inodes.length;
      if (inodes[len - 1] == null) {
        throw new FileNotFoundException(path + 
                                        " does not exist under rootDir.");
      }
      updateCount(inodes, len-1, nsDelta, dsDelta, true);
    }
  }
  
  /** update count of each inode with quota
   * 
   * @param inodes an array of inodes on a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param nsDelta the delta change of namespace
   * @param dsDelta the delta change of diskspace
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  private void updateCount(INode[] inodes, int numOfINodes, 
                           long nsDelta, long dsDelta, boolean checkQuota)
                           throws QuotaExceededException {
    if (!ready) {
      //still intializing. do not check or update quotas.
      return;
    }
    if (numOfINodes>inodes.length) {
      numOfINodes = inodes.length;
    }
    if (checkQuota) {
      verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
    }
    for(int i = 0; i < numOfINodes; i++) {
      if (inodes[i].isQuotaSet()) { // a directory with quota
        INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
        node.updateNumItemsInTree(nsDelta, dsDelta);
      }
    }
  }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INode[], int, long, long, boolean)}
   */ 
  private void updateCountNoQuotaCheck(INode[] inodes, int numOfINodes, 
                           long nsDelta, long dsDelta) {
    try {
      updateCount(inodes, numOfINodes, nsDelta, dsDelta, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.updateCountNoQuotaCheck - unexpected ", e);
    }
  }
  
  /** Return the name of the path represented by inodes at [0, pos] */
  private static String getFullPathName(INode[] inodes, int pos) {
    StringBuilder fullPathName = new StringBuilder();
    for (int i=1; i<=pos; i++) {
      fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getLocalName());
    }
    return fullPathName.toString();
  }
  
  /** Return the full path name of the specified inode */
  static String getFullPathName(INode inode) {
    // calculate the depth of this inode from root
    int depth = 0;
    for (INode i = inode; i != null; i = i.parent) {
      depth++;
    }
    INode[] inodes = new INode[depth];

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      inodes[depth-i-1] = inode;
      inode = inode.parent;
    }
    return getFullPathName(inodes, depth-1);
  }
  
  /**
   * Create a directory 
   * If ancestor directories do not exist, automatically create them.

   * @param src string representation of the path to the directory
   * @param permissions the permission of the directory
   * @param inheritPermission if the permission of the directory should inherit
   *                          from its parent or not. The automatically created
   *                          ones always inherit its permission from its parent
   * @param now creation time
   * @return true if the operation succeeds false otherwise
   * @throws FileNotFoundException if an ancestor or itself is a file
   * @throws QuotaExceededException if directory creation violates 
   *                                any quota limit
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean inheritPermission, long now)
      throws FileNotFoundException, QuotaExceededException {
    src = normalizePath(src);
    String[] names = INode.getPathNames(src);
    byte[][] components = INode.getPathComponents(names);
    INode[] inodes = new INode[components.length];

    synchronized(rootDir) {
      rootDir.getExistingPathINodes(components, inodes);

      // find the index of the first null in inodes[]
      StringBuilder pathbuilder = new StringBuilder();
      int i = 1;
      for(; i < inodes.length && inodes[i] != null; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        if (!inodes[i].isDirectory()) {
          throw new FileNotFoundException("Parent path is not a directory: "
              + pathbuilder);
        }
      }

      // create directories beginning from the first null index
      for(; i < inodes.length; i++) {
        pathbuilder.append(Path.SEPARATOR + names[i]);
        String cur = pathbuilder.toString();
        unprotectedMkdir(inodes, i, components[i], permissions,
            inheritPermission || i != components.length-1, now);
        if (inodes[i] == null) {
          return false;
        }
        // Directory creation also count towards FilesCreated
        // to match count of files_deleted metric. 
        if (namesystem != null)
          NameNode.getNameNodeMetrics().numFilesCreated.inc();
        fsImage.getEditLog().logMkDir(cur, inodes[i]);
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.mkdirs: created directory " + cur);
      }
    }
    return true;
  }

  /**
   */
  INode unprotectedMkdir(String src, PermissionStatus permissions,
                          long timestamp) throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes);
      unprotectedMkdir(inodes, inodes.length-1, components[inodes.length-1],
          permissions, false, timestamp);
      return inodes[inodes.length-1];
    }
  }

  /** create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  private void unprotectedMkdir(INode[] inodes, int pos,
      byte[] name, PermissionStatus permission, boolean inheritPermission,
      long timestamp) throws QuotaExceededException {
    inodes[pos] = addChild(inodes, pos, 
        new INodeDirectory(name, permission, timestamp),
        -1, inheritPermission );
  }
  
  /** Add a node child to the namespace. The full path name of the node is src.
   * childDiskspace should be -1, if unknown. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addNode(String src, T child, 
        long childDiskspace, boolean inheritPermission) 
  throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(src);
    child.setLocalName(components[components.length-1]);
    INode[] inodes = new INode[components.length];
    synchronized (rootDir) {
      rootDir.getExistingPathINodes(components, inodes);
      return addChild(inodes, inodes.length-1, child, childDiskspace,
                      inheritPermission);
    }
  }

  /**
   * Verify quota for adding or moving a new INode with required 
   * namespace and diskspace to a given position.
   *  
   * @param inodes INodes corresponding to a path
   * @param pos position where a new INode will be added
   * @param nsDelta needed namespace
   * @param dsDelta needed diskspace
   * @param commonAncestor Last node in inodes array that is a common ancestor
   *          for a INode that is being moved from one location to the other.
   *          Pass null if a node is not being moved.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuota(INode[] inodes, int pos, long nsDelta, long dsDelta,
      INode commonAncestor) throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    if (pos>inodes.length) {
      pos = inodes.length;
    }
    int i = pos - 1;
    try {
      // check existing components in the path  
      for(; i >= 0; i--) {
        if (commonAncestor == inodes[i]) {
          // Moving an existing node. Stop checking for quota when common
          // ancestor is reached
          return;
        }
        if (inodes[i].isQuotaSet()) { // a directory with quota
          INodeDirectoryWithQuota node =(INodeDirectoryWithQuota)inodes[i]; 
          node.verifyQuota(nsDelta, dsDelta);
        }
      }
    } catch (QuotaExceededException e) {
      e.setPathName(getFullPathName(inodes, i));
      throw e;
    }
  }
  
  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   * 
   * @param srcInodes directory from where node is being moved.
   * @param dstInodes directory to where node is moved to.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  private void verifyQuotaForRename(INode[] srcInodes, INode[]dstInodes)
      throws QuotaExceededException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode srcInode = srcInodes[srcInodes.length - 1];
    INode commonAncestor = null;
    for(int i =0;srcInodes[i] == dstInodes[i]; i++) {
      commonAncestor = srcInodes[i];
    }
    INode.DirCounts counts = new INode.DirCounts();
    srcInode.spaceConsumedInTree(counts);
    verifyQuota(dstInodes, dstInodes.length - 1, counts.getNsCount(),
            counts.getDsCount(), commonAncestor);
  }
  
  /** Add a node child to the inodes at index pos. 
   * Its ancestors are stored at [0, pos-1]. 
   * QuotaExceededException is thrown if it violates quota limit */
  private <T extends INode> T addChild(INode[] pathComponents, int pos,
      T child, long childDiskspace, boolean inheritPermission,
      boolean checkQuota) throws QuotaExceededException {
    INode.DirCounts counts = new INode.DirCounts();
    child.spaceConsumedInTree(counts);
    if (childDiskspace < 0) {
      childDiskspace = counts.getDsCount();
    }
    updateCount(pathComponents, pos, counts.getNsCount(), childDiskspace,
        checkQuota);
    T addedNode = ((INodeDirectory)pathComponents[pos-1]).addChild(
        child, inheritPermission);
    if (addedNode == null) {
      updateCount(pathComponents, pos, -counts.getNsCount(), 
          -childDiskspace, true);
    }
    return addedNode;
  }

  private <T extends INode> T addChild(INode[] pathComponents, int pos,
      T child, long childDiskspace, boolean inheritPermission)
      throws QuotaExceededException {
    return addChild(pathComponents, pos, child, childDiskspace,
        inheritPermission, true);
  }
  
  private <T extends INode> T addChildNoQuotaCheck(INode[] pathComponents,
      int pos, T child, long childDiskspace, boolean inheritPermission) {
    T inode = null;
    try {
      inode = addChild(pathComponents, pos, child, childDiskspace,
          inheritPermission, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e); 
    }
    return inode;
  }
  
  /** Remove an inode at index pos from the namespace.
   * Its ancestors are stored at [0, pos-1].
   * Count of each ancestor with quota is also updated.
   * Return the removed node; null if the removal fails.
   */
  private INode removeChild(INode[] pathComponents, int pos) {
    INode removedNode = 
      ((INodeDirectory)pathComponents[pos-1]).removeChild(pathComponents[pos]);
    if (removedNode != null) {
      INode.DirCounts counts = new INode.DirCounts();
      removedNode.spaceConsumedInTree(counts);
      updateCountNoQuotaCheck(pathComponents, pos,
                  -counts.getNsCount(), -counts.getDsCount());
    }
    return removedNode;
  }
  
  /**
   */
  String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  ContentSummary getContentSummary(String src) throws IOException {
    String srcs = normalizePath(src);
    synchronized (rootDir) {
      INode targetNode = rootDir.getNode(srcs);
      if (targetNode == null) {
        throw new FileNotFoundException("File does not exist: " + srcs);
      }
      else {
        return targetNode.computeContentSummary();
      }
    }
  }

  /** Update the count of each directory with quota in the namespace
   * A directory's count is defined as the total number inodes in the tree
   * rooted at the directory.
   * 
   * This is an update of existing state of the filesystem and does not
   * throw QuotaExceededException.
   */
  void updateCountForINodeWithQuota() {
    updateCountForINodeWithQuota(rootDir, new INode.DirCounts(), 
                                 new ArrayList<INode>(50));
  }
  
  /** 
   * Update the count of the directory if it has a quota and return the count
   * 
   * This does not throw a QuotaExceededException. This is just an update
   * of of existing state and throwing QuotaExceededException does not help
   * with fixing the state, if there is a problem.
   * 
   * @param dir the root of the tree that represents the directory
   * @param counters counters for name space and disk space
   * @param nodesInPath INodes for the each of components in the path.
   * @return the size of the tree
   */
  private static void updateCountForINodeWithQuota(INodeDirectory dir, 
                                               INode.DirCounts counts,
                                               ArrayList<INode> nodesInPath) {
    long parentNamespace = counts.nsCount;
    long parentDiskspace = counts.dsCount;
    
    counts.nsCount = 1L;//for self. should not call node.spaceConsumedInTree()
    counts.dsCount = 0L;
    
    /* We don't need nodesInPath if we could use 'parent' field in 
     * INode. using 'parent' is not currently recommended. */
    nodesInPath.add(dir);

    for (INode child : dir.getChildren()) {
      if (child.isDirectory()) {
        updateCountForINodeWithQuota((INodeDirectory)child, 
                                     counts, nodesInPath);
      } else { // reduce recursive calls
        counts.nsCount += 1;
        counts.dsCount += ((INodeFile)child).diskspaceConsumed();
      }
    }
      
    if (dir.isQuotaSet()) {
      ((INodeDirectoryWithQuota)dir).setSpaceConsumed(counts.nsCount,
                                                      counts.dsCount);

      // check if quota is violated for some reason.
      if ((dir.getNsQuota() >= 0 && counts.nsCount > dir.getNsQuota()) ||
          (dir.getDsQuota() >= 0 && counts.dsCount > dir.getDsQuota())) {

        // can only happen because of a software bug. the bug should be fixed.
        StringBuilder path = new StringBuilder(512);
        for (INode n : nodesInPath) {
          path.append('/');
          path.append(n.getLocalName());
        }
        
        NameNode.LOG.warn("Quota violation in image for " + path + 
                          " (Namespace quota : " + dir.getNsQuota() +
                          " consumed : " + counts.nsCount + ")" +
                          " (Diskspace quota : " + dir.getDsQuota() +
                          " consumed : " + counts.dsCount + ").");
      }            
    }
      
    // pop 
    nodesInPath.remove(nodesInPath.size()-1);
    
    counts.nsCount += parentNamespace;
    counts.dsCount += parentDiskspace;
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * Sets quota for for a directory.
   * @returns INodeDirectory if any of the quotas have changed. null other wise.
   * @throws FileNotFoundException if the path does not exist or is a file
   * @throws QuotaExceededException if the directory tree size is 
   *                                greater than the given quota
   */
  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota) 
                       throws FileNotFoundException, QuotaExceededException {
    // sanity check
    if ((nsQuota < 0 && nsQuota != FSConstants.QUOTA_DONT_SET && 
         nsQuota < FSConstants.QUOTA_RESET) || 
        (dsQuota < 0 && dsQuota != FSConstants.QUOTA_DONT_SET && 
          dsQuota < FSConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
                                         "dsQuota : " + nsQuota + " and " +
                                         dsQuota);
    }
    
    String srcs = normalizePath(src);
    INode[] inodes = rootDir.getExistingPathINodes(src);
    INode targetNode = inodes[inodes.length-1];
    if (targetNode == null) {
      throw new FileNotFoundException("Directory does not exist: " + srcs);
    } else if (!targetNode.isDirectory()) {
      throw new FileNotFoundException("Cannot set quota on a file: " + srcs);  
    } else if (targetNode.isRoot() && nsQuota == FSConstants.QUOTA_RESET) {
      throw new IllegalArgumentException("Cannot clear namespace quota on root.");
    } else { // a directory inode
      INodeDirectory dirNode = (INodeDirectory)targetNode;
      long oldNsQuota = dirNode.getNsQuota();
      long oldDsQuota = dirNode.getDsQuota();
      if (nsQuota == FSConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (dsQuota == FSConstants.QUOTA_DONT_SET) {
        dsQuota = oldDsQuota;
      }        

      if (dirNode instanceof INodeDirectoryWithQuota) { 
        // a directory with quota; so set the quota to the new value
        ((INodeDirectoryWithQuota)dirNode).setQuota(nsQuota, dsQuota);
	if (!dirNode.isQuotaSet()) {
          // will not come here for root because root's nsQuota is always set
          INodeDirectory newNode = new INodeDirectory(dirNode);
          INodeDirectory parent = (INodeDirectory)inodes[inodes.length-2];
          dirNode = newNode;
          parent.replaceChild(newNode);
        }
      } else {
        // a non-quota directory; so replace it with a directory with quota
        INodeDirectoryWithQuota newNode = 
          new INodeDirectoryWithQuota(nsQuota, dsQuota, dirNode);
        // non-root directory node; parent != null
        INodeDirectory parent = (INodeDirectory)inodes[inodes.length-2];
        dirNode = newNode;
        parent.replaceChild(newNode);
      }
      return (oldNsQuota != nsQuota || oldDsQuota != dsQuota) ? dirNode : null;
    }
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the 
   * contract.
   * @see #unprotectedSetQuota(String, long, long)
   */
  void setQuota(String src, long nsQuota, long dsQuota) 
                throws FileNotFoundException, QuotaExceededException {
    synchronized (rootDir) {    
      INodeDirectory dir = unprotectedSetQuota(src, nsQuota, dsQuota);
      if (dir != null) {
        fsImage.getEditLog().logSetQuota(src, dir.getNsQuota(), 
                                         dir.getDsQuota());
      }
    }
  }
  
  long totalInodes() {
    synchronized (rootDir) {
      return rootDir.numItemsInTree();
    }
  }

  /**
   * Sets the access time on the file. Logs it in the transaction log
   */
  void setTimes(String src, INodeFile inode, long mtime, long atime, boolean force) 
                                                        throws IOException {
    if (unprotectedSetTimes(src, inode, mtime, atime, force)) {
      fsImage.getEditLog().logTimes(src, mtime, atime);
    }
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force) 
                              throws IOException {
    INodeFile inode = getFileINode(src);
    if (inode == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedSetTimes: "
          +"failed to setTimes " + src + " because source does not exist");
      return false;
    }
    return unprotectedSetTimes(src, inode, mtime, atime, force);
  }

  private boolean unprotectedSetTimes(String src, INodeFile inode, long mtime,
                                      long atime, boolean force) throws IOException {
    boolean status = false;
    if (mtime != -1) {
      inode.setModificationTimeForce(mtime);
      status = true;
    }
    if (atime != -1) {
      long inodeTime = inode.getAccessTime();

      // if the last access time update was within the last precision interval, then
      // no need to store access time
      if (atime <= inodeTime + namesystem.getAccessTimePrecision() && !force) {
        status =  false;
      } else {
        inode.setAccessTime(atime);
        status = true;
      }
    } 
    return status;
  }
  
  /**
   * Create FileStatus by file INode 
   */
   private static HdfsFileStatus createFileStatus(byte[] path, INode node) {
    // length is zero for directories
    return new HdfsFileStatus(
        node.isDirectory() ? 0 : ((INodeFile)node).computeContentSummary().getLength(), 
        node.isDirectory(), 
        node.isDirectory() ? 0 : ((INodeFile)node).getReplication(), 
        node.isDirectory() ? 0 : ((INodeFile)node).getPreferredBlockSize(),
        node.getModificationTime(),
        node.getAccessTime(),
        node.getFsPermission(),
        node.getUserName(),
        node.getGroupName(),
        path);
  }
}
