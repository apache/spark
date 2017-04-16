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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.conf.Configuration;

/**
 * An implementation of ChecksumFileSystem over DistributedFileSystem. 
 * Note that as of now (May 07), DistributedFileSystem natively checksums 
 * all of its data. Using this class is not be necessary in most cases.
 * Currently provided mainly for backward compatibility and testing.
 */
public class ChecksumDistributedFileSystem extends ChecksumFileSystem {
  
  public ChecksumDistributedFileSystem() {
    super( new DistributedFileSystem() );
  }

  /** @deprecated */
  public ChecksumDistributedFileSystem(InetSocketAddress namenode,
                                       Configuration conf) throws IOException {
    super( new DistributedFileSystem(namenode, conf) );
  }
  
  /** Any extra interface that DistributeFileSystem provides can be
   * accessed with this.*/
  DistributedFileSystem getDFS() {
    return (DistributedFileSystem)fs;
  }

  /** Return the total raw capacity of the filesystem, disregarding
   * replication .*/
  public long getRawCapacity() throws IOException{
    return getDFS().getRawCapacity();
  }

  /** Return the total raw used space in the filesystem, disregarding
   * replication .*/
  public long getRawUsed() throws IOException{
    return getDFS().getRawUsed();
  }

  /** Return statistics for each datanode. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return getDFS().getDataNodeStats();
  }
    
  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) 
    throws IOException {
    return getDFS().setSafeMode(action);
  }

  /*
   * Refreshes the list of hosts and excluded hosts from the configured 
   * files.  
   */
  public void refreshNodes() throws IOException {
    getDFS().refreshNodes();
  }

  /**
   * Finalize previously upgraded files system state.
   */
  public void finalizeUpgrade() throws IOException {
    getDFS().finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
                                                        ) throws IOException {
    return getDFS().distributedUpgradeProgress(action);
  }

  /*
   * Dumps dfs data structures into specified file.
   */
  public void metaSave(String pathname) throws IOException {
    getDFS().metaSave(pathname);
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  public boolean reportChecksumFailure(Path f, 
                                       FSDataInputStream in, long inPos, 
                                       FSDataInputStream sums, long sumsPos) {
    return getDFS().reportChecksumFailure(f, in, inPos, sums, sumsPos);
  }
  
  
  /**
   * Returns the stat information about the file.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return getDFS().getFileStatus(f);
  }

}
