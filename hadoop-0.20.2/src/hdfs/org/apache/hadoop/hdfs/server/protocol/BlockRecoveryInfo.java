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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Writable;

public class BlockRecoveryInfo implements Writable {
  private Block block;
  private boolean wasRecoveredOnStartup;
  
  public BlockRecoveryInfo() {
    block = new Block();
    wasRecoveredOnStartup = false;
  }
  
  public BlockRecoveryInfo(Block block,
      boolean wasRecoveredOnStartup)
  {
    this.block = new Block(block);
    this.wasRecoveredOnStartup = wasRecoveredOnStartup;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    block.readFields(in);
    wasRecoveredOnStartup = in.readBoolean();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    block.write(out);
    out.writeBoolean(wasRecoveredOnStartup);    
  }

  public Block getBlock() {
    return block;
  }
  public boolean wasRecoveredOnStartup() {
    return wasRecoveredOnStartup;
  }
  
  public String toString() {
    return "BlockRecoveryInfo(block=" + block +
      " wasRecoveredOnStartup=" + wasRecoveredOnStartup + ")";
  }
}
