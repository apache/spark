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

import java.io.IOException;
import java.io.File;
import org.apache.hadoop.hdfs.protocol.Block;

public abstract class FSDatasetTestUtil {

  /**
   * Truncate the given block in place, such that the new truncated block
   * is still valid (ie checksums are updated to stay in sync with block file)
   */
  public static void truncateBlock(DataNode dn,
                                   Block block,
                                   long newLength)
    throws IOException
  {
    FSDataset ds = (FSDataset)dn.data;
    
    File blockFile = ds.findBlockFile(block.getBlockId());
    if (blockFile == null) {
      throw new IOException("Can't find block file for block " +
                            block + " on DN " + dn);
    }
    File metaFile = FSDataset.findMetaFile(blockFile);
    FSDataset.truncateBlock(blockFile, metaFile,
                            block.getNumBytes(), newLength);
  }
  
  public static void truncateBlockFile(File blockFile, long newLength)
    throws IOException {
    File metaFile = FSDataset.findMetaFile(blockFile);
    FSDataset.truncateBlock(blockFile, metaFile,
                            blockFile.length(), newLength);    
  }

}