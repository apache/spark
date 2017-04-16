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

import java.util.ArrayList;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;

import junit.framework.TestCase;

/**
 * This class tests that methods in DatanodeDescriptor
 */
public class TestDatanodeDescriptor extends TestCase {
  /**
   * Test that getInvalidateBlocks observes the maxlimit.
   */
  public void testGetInvalidateBlocks() throws Exception {
    final int MAX_BLOCKS = 10;
    final int REMAINING_BLOCKS = 2;
    final int MAX_LIMIT = MAX_BLOCKS - REMAINING_BLOCKS;
    
    DatanodeDescriptor dd = new DatanodeDescriptor();
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    for (int i=0; i<MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP));
    }
    dd.addBlocksToBeInvalidated(blockList);
    BlockCommand bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.getBlocks().length, MAX_LIMIT);
    bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.getBlocks().length, REMAINING_BLOCKS);
  }
}
