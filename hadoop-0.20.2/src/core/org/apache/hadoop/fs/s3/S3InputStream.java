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

package org.apache.hadoop.fs.s3;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

class S3InputStream extends FSInputStream {

  private FileSystemStore store;

  private Block[] blocks;

  private boolean closed;

  private long fileLength;

  private long pos = 0;

  private File blockFile;
  
  private DataInputStream blockStream;

  private long blockEnd = -1;
  
  private FileSystem.Statistics stats;

  @Deprecated
  public S3InputStream(Configuration conf, FileSystemStore store,
                       INode inode) {
    this(conf, store, inode, null);
  }

  public S3InputStream(Configuration conf, FileSystemStore store,
                       INode inode, FileSystem.Statistics stats) {
    
    this.store = store;
    this.stats = stats;
    this.blocks = inode.getBlocks();
    for (Block block : blocks) {
      this.fileLength += block.getLength();
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int available() throws IOException {
    return (int) (fileLength - pos);
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > fileLength) {
      throw new IOException("Cannot seek after EOF");
    }
    pos = targetPos;
    blockEnd = -1;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    int result = -1;
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      result = blockStream.read();
      if (result >= 0) {
        pos++;
      }
    }
    if (stats != null & result >= 0) {
      stats.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      int realLen = Math.min(len, (int) (blockEnd - pos + 1));
      int result = blockStream.read(buf, off, realLen);
      if (result >= 0) {
        pos += result;
      }
      if (stats != null && result > 0) {
        stats.incrementBytesRead(result);
      }
      return result;
    }
    return -1;
  }

  private synchronized void blockSeekTo(long target) throws IOException {
    //
    // Compute desired block
    //
    int targetBlock = -1;
    long targetBlockStart = 0;
    long targetBlockEnd = 0;
    for (int i = 0; i < blocks.length; i++) {
      long blockLength = blocks[i].getLength();
      targetBlockEnd = targetBlockStart + blockLength - 1;

      if (target >= targetBlockStart && target <= targetBlockEnd) {
        targetBlock = i;
        break;
      } else {
        targetBlockStart = targetBlockEnd + 1;
      }
    }
    if (targetBlock < 0) {
      throw new IOException(
                            "Impossible situation: could not find target position " + target);
    }
    long offsetIntoBlock = target - targetBlockStart;

    // read block blocks[targetBlock] from position offsetIntoBlock

    this.blockFile = store.retrieveBlock(blocks[targetBlock], offsetIntoBlock);

    this.pos = target;
    this.blockEnd = targetBlockEnd;
    this.blockStream = new DataInputStream(new FileInputStream(blockFile));

  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (blockStream != null) {
      blockStream.close();
      blockStream = null;
    }
    if (blockFile != null) {
      blockFile.delete();
    }
    super.close();
    closed = true;
  }

  /**
   * We don't support marks.
   */
  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }

}
