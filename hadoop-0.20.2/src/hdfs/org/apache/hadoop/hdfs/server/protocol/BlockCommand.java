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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.*;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.io.*;


/****************************************************
 * A BlockCommand is an instruction to a datanode 
 * regarding some blocks under its control.  It tells
 * the DataNode to either invalidate a set of indicated
 * blocks, or to copy a set of indicated blocks to 
 * another DataNode.
 * 
 ****************************************************/
public class BlockCommand extends DatanodeCommand {
  Block blocks[];
  DatanodeInfo targets[][];

  public BlockCommand() {}

  /**
   * Create BlockCommand for transferring blocks to another datanode
   * @param blocktargetlist    blocks to be transferred 
   */
  public BlockCommand(int action, List<BlockTargetPair> blocktargetlist) {
    super(action);

    blocks = new Block[blocktargetlist.size()]; 
    targets = new DatanodeInfo[blocks.length][];
    for(int i = 0; i < blocks.length; i++) {
      BlockTargetPair p = blocktargetlist.get(i);
      blocks[i] = p.block;
      targets[i] = p.targets;
    }
  }

  private static final DatanodeInfo[][] EMPTY_TARGET = {};

  /**
   * Create BlockCommand for the given action
   * @param blocks blocks related to the action
   */
  public BlockCommand(int action, Block blocks[]) {
    super(action);
    this.blocks = blocks;
    this.targets = EMPTY_TARGET;
  }

  public Block[] getBlocks() {
    return blocks;
  }

  public DatanodeInfo[][] getTargets() {
    return targets;
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (BlockCommand.class,
       new WritableFactory() {
         public Writable newInstance() { return new BlockCommand(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(blocks.length);
    for (int i = 0; i < blocks.length; i++) {
      blocks[i].write(out);
    }
    out.writeInt(targets.length);
    for (int i = 0; i < targets.length; i++) {
      out.writeInt(targets[i].length);
      for (int j = 0; j < targets[i].length; j++) {
        targets[i][j].write(out);
      }
    }
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.blocks = new Block[in.readInt()];
    for (int i = 0; i < blocks.length; i++) {
      blocks[i] = new Block();
      blocks[i].readFields(in);
    }

    this.targets = new DatanodeInfo[in.readInt()][];
    for (int i = 0; i < targets.length; i++) {
      this.targets[i] = new DatanodeInfo[in.readInt()];
      for (int j = 0; j < targets[i].length; j++) {
        targets[i][j] = new DatanodeInfo();
        targets[i][j].readFields(in);
      }
    }
  }
}
