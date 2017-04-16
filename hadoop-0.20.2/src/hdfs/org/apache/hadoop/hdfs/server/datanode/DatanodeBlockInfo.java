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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileUtil.HardLink;
import org.apache.hadoop.io.IOUtils;

/**
 * This class is used by the datanode to maintain the map from a block 
 * to its metadata.
 */
class DatanodeBlockInfo {

  private FSVolume volume;       // volume where the block belongs
  private File     file;         // block file
  private boolean detached;      // copy-on-write done for block

  DatanodeBlockInfo(FSVolume vol, File file) {
    this.volume = vol;
    this.file = file;
    detached = false;
  }

  DatanodeBlockInfo(FSVolume vol) {
    this.volume = vol;
    this.file = null;
    detached = false;
  }

  FSVolume getVolume() {
    return volume;
  }

  File getFile() {
    return file;
  }

  /**
   * Is this block already detached?
   */
  boolean isDetached() {
    return detached;
  }

  /**
   *  Block has been successfully detached
   */
  void setDetached() {
    detached = true;
  }

  /**
   * Copy specified file into a temporary file. Then rename the
   * temporary file to the original name. This will cause any
   * hardlinks to the original file to be removed. The temporary
   * files are created in the detachDir. The temporary files will
   * be recovered (especially on Windows) on datanode restart.
   */
  private void detachFile(File file, Block b) throws IOException {
    File tmpFile = volume.createDetachFile(b, file.getName());
    try {
      IOUtils.copyBytes(new FileInputStream(file),
                        new FileOutputStream(tmpFile),
                        16*1024, true);
      if (file.length() != tmpFile.length()) {
        throw new IOException("Copy of file " + file + " size " + file.length()+
                              " into file " + tmpFile +
                              " resulted in a size of " + tmpFile.length());
      }
      FileUtil.replaceFile(tmpFile, file);
    } catch (IOException e) {
      boolean done = tmpFile.delete();
      if (!done) {
        DataNode.LOG.info("detachFile failed to delete temporary file " +
                          tmpFile);
      }
      throw e;
    }
  }

  /**
   * Returns true if this block was copied, otherwise returns false.
   */
  boolean detachBlock(Block block, int numLinks) throws IOException {
    if (isDetached()) {
      return false;
    }
    if (file == null || volume == null) {
      throw new IOException("detachBlock:Block not found. " + block);
    }
    File meta = FSDataset.getMetaFile(file, block);
    if (meta == null) {
      throw new IOException("Meta file not found for block " + block);
    }

    if (HardLink.getLinkCount(file) > numLinks) {
      DataNode.LOG.info("CopyOnWrite for block " + block);
      detachFile(file, block);
    }
    if (HardLink.getLinkCount(meta) > numLinks) {
      detachFile(meta, block);
    }
    setDetached();
    return true;
  }
  
  public String toString() {
    return getClass().getSimpleName() + "(volume=" + volume
        + ", file=" + file + ", detached=" + detached + ")";
  }
}
