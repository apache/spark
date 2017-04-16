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

import java.io.*;
import java.net.*;
import java.util.ArrayList;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.Progressable;


/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 *****************************************************************/
public class DistributedFileSystem extends FileSystem {
  private Path workingDir;
  private URI uri;

  DFSClient dfs;
  private boolean verifyChecksum = true;
  
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  public DistributedFileSystem() {
  }

  /** @deprecated */
  public DistributedFileSystem(InetSocketAddress namenode,
    Configuration conf) throws IOException {
    initialize(NameNode.getUri(namenode), conf);
  }

  /** @deprecated */
  public String getName() { return uri.getAuthority(); }

  public URI getUri() { return uri; }

  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: "+ uri);
    }

    InetSocketAddress namenode = NameNode.getAddress(uri.getAuthority());
    this.dfs = new DFSClient(namenode, conf, statistics);
    this.uri = NameNode.getUri(namenode);
    this.workingDir = getHomeDirectory();
  }

  /** Permit paths which explicitly specify the default port. */
  protected void checkPath(Path path) {
    URI thisUri = this.getUri();
    URI thatUri = path.toUri();
    String thatAuthority = thatUri.getAuthority();
    if (thatUri.getScheme() != null
        && thatUri.getScheme().equalsIgnoreCase(thisUri.getScheme())
        && thatUri.getPort() == NameNode.DEFAULT_PORT
        && thisUri.getPort() == -1
        && thatAuthority.substring(0,thatAuthority.indexOf(":"))
        .equalsIgnoreCase(thisUri.getAuthority()))
      return;
    super.checkPath(path);
  }

  /** Normalize paths that explicitly specify the default port. */
  public Path makeQualified(Path path) {
    URI thisUri = this.getUri();
    URI thatUri = path.toUri();
    String thatAuthority = thatUri.getAuthority();
    if (thatUri.getScheme() != null
        && thatUri.getScheme().equalsIgnoreCase(thisUri.getScheme())
        && thatUri.getPort() == NameNode.DEFAULT_PORT
        && thisUri.getPort() == -1
        && thatAuthority.substring(0,thatAuthority.indexOf(":"))
        .equalsIgnoreCase(thisUri.getAuthority())) {
      path = new Path(thisUri.getScheme(), thisUri.getAuthority(),
                      thatUri.getPath());
    }
    return super.makeQualified(path);
  }


  public Path getWorkingDirectory() {
    return workingDir;
  }

  public long getDefaultBlockSize() {
    return dfs.getDefaultBlockSize();
  }

  public short getDefaultReplication() {
    return dfs.getDefaultReplication();
  }

  private Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new Path(workingDir, f);
    }
  }

  public void setWorkingDirectory(Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + 
                                         result);
    }
    workingDir = makeAbsolute(dir);
  }

  /** {@inheritDoc} */
  public Path getHomeDirectory() {
    return new Path("/user/" + dfs.ugi.getShortUserName()).makeQualified(this);
  }

  private String getPathName(Path file) {
    checkPath(file);
    String result = makeAbsolute(file).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
                                         file+" is not a valid DFS filename.");
    }
    return result;
  }
  

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) {
      return null;
    }
    statistics.incrementReadOps(1);
    return dfs.getBlockLocations(getPathName(file.getPath()), start, len);
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    statistics.incrementReadOps(1);
    return new DFSClient.DFSDataInputStream(
          dfs.open(getPathName(f), bufferSize, verifyChecksum, statistics));
  }

  /**
   * Start the lease recovery of a file
   *
   * @param f a file
   * @return true if the file is already closed
   * @throws IOException if an error occurs
   * @throws NoSuchMethodException if server does not support this function
   */
  public boolean recoverLease(Path f)
      throws IOException, NoSuchMethodException {
    return dfs.recoverLease(getPathName(f));
  }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {

    statistics.incrementWriteOps(1);
    DFSOutputStream op = (DFSOutputStream)dfs.append(getPathName(f), bufferSize, progress);
    return new FSDataOutputStream(op, statistics, op.getInitialLen());
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite,
    int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {

    statistics.incrementWriteOps(1);
    return new FSDataOutputStream
       (dfs.create(getPathName(f), permission,
                   overwrite, true, replication, blockSize, progress, bufferSize),
        statistics);
  }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   * @see #create(Path, FsPermission, boolean, int, short, long, Progressable)
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize, 
      Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    return new FSDataOutputStream
        (dfs.create(getPathName(f), permission, 
                    overwrite, false, replication, blockSize, progress, bufferSize), 
         statistics);
  }

  public boolean setReplication(Path src, 
                                short replication
                               ) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.setReplication(getPathName(src), replication);
  }

  /**
   * Rename files/dirs
   */
  public boolean rename(Path src, Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.rename(getPathName(src), getPathName(dst));
  }

  /**
   * Get rid of Path f, whether a true file or dir.
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.delete(getPathName(f));
  }
  
  /**
   * requires a boolean check to delete a non 
   * empty directory recursively.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.delete(getPathName(f), recursive);
  }
  
  /** {@inheritDoc} */
  public ContentSummary getContentSummary(Path f) throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getContentSummary(getPathName(f));
  }

  /** Set a directory's quotas
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long) 
   */
  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota) 
                       throws IOException {
    dfs.setQuota(getPathName(src), namespaceQuota, diskspaceQuota);
  }
  
  private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(),
        f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.getFullPath(parent).makeQualified(this)); // fully-qualify path
  }

  /**
   * List all the entries of a directory
   * 
   * Note that this operation is not atomic for a large directory.
   * The entries of a directory may be fetched from NameNode multiple times.
   * It only guarantees that  each name occurs once if a directory 
   * undergoes changes between the calls.
   */
  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    String src = getPathName(p);
    
    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = dfs.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);
    
    if (thisListing == null) { // the directory does not exist
      return null;
    }
    
    HdfsFileStatus[] partialListing = thisListing.getPartialListing();
    if (!thisListing.hasMore()) { // got all entries of the directory
      FileStatus[] stats = new FileStatus[partialListing.length];
      for (int i = 0; i < partialListing.length; i++) {
        stats[i] = makeQualified(partialListing[i], p);
      }
      statistics.incrementReadOps(1);
      return stats;
    }
    
    // The directory size is too big that it needs to fetch more
    // estimate the total number of entries in the directory
    int totalNumEntries = 
      partialListing.length + thisListing.getRemainingEntries();
    ArrayList<FileStatus> listing = 
      new ArrayList<FileStatus>(totalNumEntries);
    // add the first batch of entries to the array list
    for (HdfsFileStatus fileStatus : partialListing) {
      listing.add(makeQualified(fileStatus, p));
    }
    statistics.incrementLargeReadOps(1);

    // now fetch more entries
    do {
      thisListing = dfs.listPaths(src, thisListing.getLastName());
      
      if (thisListing == null) {
        return null; // the directory is deleted
      }
      
      partialListing = thisListing.getPartialListing();
      for (HdfsFileStatus fileStatus : partialListing) {
        listing.add(makeQualified(fileStatus, p));
      }
      statistics.incrementLargeReadOps(1);
    } while (thisListing.hasMore());

    return listing.toArray(new FileStatus[listing.size()]);
  }

  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.mkdirs(getPathName(f), permission);
  }

  /** {@inheritDoc} */
  public void close() throws IOException {
    try {
      super.processDeleteOnExit();
      dfs.close();
    } finally {
      super.close();
    }
  }

  public String toString() {
    return "DFS[" + dfs + "]";
  }

  public DFSClient getClient() {
    return dfs;
  }        
  
  /** @deprecated Use {@link org.apache.hadoop.fs.FsStatus instead */
  @Deprecated
  public static class DiskStatus extends FsStatus {
    public DiskStatus(FsStatus stats) {
      super(stats.getCapacity(), stats.getUsed(), stats.getRemaining());
    }

    public DiskStatus(long capacity, long dfsUsed, long remaining) {
      super(capacity, dfsUsed, remaining);
    }

    public long getDfsUsed() {
      return super.getUsed();
    }
  }
  
  /** {@inheritDoc} */
  public FsStatus getStatus(Path p) throws IOException {
    return dfs.getDiskStatus();
  }

  /** Return the disk usage of the filesystem, including total capacity,
   * used space, and remaining space 
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()} 
   * instead */
   @Deprecated
  public DiskStatus getDiskStatus() throws IOException {
    return new DiskStatus(dfs.getDiskStatus());
  }
  
  /** Return the total raw capacity of the filesystem, disregarding
   * replication.
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()} 
   * instead */
   @Deprecated
  public long getRawCapacity() throws IOException{
    return dfs.getDiskStatus().getCapacity();
  }

  /** Return the total raw used space in the filesystem, disregarding
   * replication.
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()} 
   * instead */
   @Deprecated
  public long getRawUsed() throws IOException{
    return dfs.getDiskStatus().getUsed();
  }
   
  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   * 
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    return dfs.getMissingBlocksCount();
  }

  /**
   * Returns count of blocks with one of more replica missing.
   * 
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    return dfs.getUnderReplicatedBlocksCount();
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   * 
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    return dfs.getCorruptBlocksCount();
  }

  /** Return statistics for each datanode. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return dfs.datanodeReport(DatanodeReportType.ALL);
  }

  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *    FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) 
  throws IOException {
    return dfs.setSafeMode(action);
  }

  /**
   * Save namespace image.
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()
   */
  public void saveNamespace() throws AccessControlException, IOException {
    dfs.saveNamespace();
  }

  /**
   * Refreshes the list of hosts and excluded hosts from the configured 
   * files.  
   */
  public void refreshNodes() throws IOException {
    dfs.refreshNodes();
  }

  /**
   * Finalize previously upgraded files system state.
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException {
    dfs.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
  ) throws IOException {
    return dfs.distributedUpgradeProgress(action);
  }

  /*
   * Requests the namenode to dump data strcutures into specified 
   * file.
   */
  public void metaSave(String pathname) throws IOException {
    dfs.metaSave(pathname);
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  public boolean reportChecksumFailure(Path f, 
    FSDataInputStream in, long inPos, 
    FSDataInputStream sums, long sumsPos) {

    LocatedBlock lblocks[] = new LocatedBlock[2];

    // Find block in data stream.
    DFSClient.DFSDataInputStream dfsIn = (DFSClient.DFSDataInputStream) in;
    Block dataBlock = dfsIn.getCurrentBlock();
    if (dataBlock == null) {
      LOG.error("Error: Current block in data stream is null! ");
      return false;
    }
    DatanodeInfo[] dataNode = {dfsIn.getCurrentDatanode()}; 
    lblocks[0] = new LocatedBlock(dataBlock, dataNode);
    LOG.info("Found checksum error in data stream at block="
        + dataBlock + " on datanode="
        + dataNode[0].getName());

    // Find block in checksum stream
    DFSClient.DFSDataInputStream dfsSums = (DFSClient.DFSDataInputStream) sums;
    Block sumsBlock = dfsSums.getCurrentBlock();
    if (sumsBlock == null) {
      LOG.error("Error: Current block in checksum stream is null! ");
      return false;
    }
    DatanodeInfo[] sumsNode = {dfsSums.getCurrentDatanode()}; 
    lblocks[1] = new LocatedBlock(sumsBlock, sumsNode);
    LOG.info("Found checksum error in checksum stream at block="
        + sumsBlock + " on datanode="
        + sumsNode[0].getName());

    // Ask client to delete blocks.
    dfs.reportChecksumFailure(f.toString(), lblocks);

    return true;
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    HdfsFileStatus fi = dfs.getFileInfo(getPathName(f));
    if (fi != null) {
      return makeQualified(fi, f);
    } else {
      throw new FileNotFoundException("File does not exist: " + f);
    }
  }

  /** {@inheritDoc} */
  public MD5MD5CRC32FileChecksum getFileChecksum(Path f) throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getFileChecksum(getPathName(f));
  }

  /** {@inheritDoc }*/
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    statistics.incrementWriteOps(1);
    dfs.setPermission(getPathName(p), permission);
  }

  /** {@inheritDoc }*/
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    statistics.incrementWriteOps(1);
    dfs.setOwner(getPathName(p), username, groupname);
  }

  /** {@inheritDoc }*/
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    statistics.incrementWriteOps(1);
    dfs.setTimes(getPathName(p), mtime, atime);
  }

  @Override
  protected int getDefaultPort() {
    return NameNode.DEFAULT_PORT;
  }

  @Override
  public 
  Token<DelegationTokenIdentifier> getDelegationToken(String renewer
                                                      ) throws IOException {
    Token<DelegationTokenIdentifier> result =
      dfs.getDelegationToken(renewer == null ? null : new Text(renewer));
    result.setService(new Text(getCanonicalServiceName()));
    return result;
  }

  /** 
   * Delegation Token Operations
   * These are DFS only operations.
   */
  
  /**
   * Get a valid Delegation Token.
   * 
   * @param renewer Name of the designated renewer for the token
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   * @Deprecated use {@link #getDelegationToken(String)}
   */
  @Deprecated
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return dfs.getDelegationToken(renewer);
  }

  /**
   * Renew an existing delegation token.
   * 
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return dfs.renewDelegationToken(token);
  }

  /**
   * Cancel an existing delegation token.
   * 
   * @param token delegation token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    dfs.cancelDelegationToken(token);
  }
}
