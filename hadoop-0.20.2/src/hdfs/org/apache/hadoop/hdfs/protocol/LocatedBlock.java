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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.*;
import org.apache.hadoop.security.token.Token;

import java.io.*;

/****************************************************
 * A LocatedBlock is a pair of Block, DatanodeInfo[]
 * objects.  It tells where to find a Block.
 * 
 ****************************************************/
public class LocatedBlock implements Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedBlock.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedBlock(); }
       });
  }

  private Block b;
  private long offset;  // offset of the first byte of the block in the file
  private DatanodeInfo[] locs;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken = new Token<BlockTokenIdentifier>();

  /**
   */
  public LocatedBlock() {
    this(new Block(), new DatanodeInfo[0], 0L, false);
  }

  /**
   */
  public LocatedBlock(Block b, DatanodeInfo[] locs) {
    this(b, locs, -1, false); // startOffset is unknown
  }

  /**
   */
  public LocatedBlock(Block b, DatanodeInfo[] locs, long startOffset) {
    this(b, locs, startOffset, false);
  }

  /**
   */
  public LocatedBlock(Block b, DatanodeInfo[] locs, long startOffset, 
                      boolean corrupt) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs==null) {
      this.locs = new DatanodeInfo[0];
    } else {
      this.locs = locs;
    }
  }

  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  /**
   */
  public Block getBlock() {
    return b;
  }

  /**
   */
  public DatanodeInfo[] getLocations() {
    return locs;
  }
  
  public long getStartOffset() {
    return offset;
  }
  
  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }

  void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }
  
  public boolean isCorrupt() {
    return this.corrupt;
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    blockToken.write(out);
    out.writeBoolean(corrupt);
    out.writeLong(offset);
    b.write(out);
    out.writeInt(locs.length);
    for (int i = 0; i < locs.length; i++) {
      locs[i].write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    blockToken.readFields(in);
    this.corrupt = in.readBoolean();
    offset = in.readLong();
    this.b = new Block();
    b.readFields(in);
    int count = in.readInt();
    this.locs = new DatanodeInfo[count];
    for (int i = 0; i < locs.length; i++) {
      locs[i] = new DatanodeInfo();
      locs[i].readFields(in);
    }
  }
}
