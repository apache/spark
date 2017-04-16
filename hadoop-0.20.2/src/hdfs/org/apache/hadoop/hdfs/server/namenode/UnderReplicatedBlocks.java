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

import java.util.*;

import org.apache.hadoop.hdfs.protocol.Block;

/* Class for keeping track of under replication blocks
 * Blocks have replication priority, with priority 0 indicating the highest
 * Blocks have only one replicas has the highest
 */
class UnderReplicatedBlocks implements Iterable<Block> {
  static final int LEVEL = 3;
  private List<TreeSet<Block>> priorityQueues = new ArrayList<TreeSet<Block>>();
      
  /* constructor */
  UnderReplicatedBlocks() {
    for(int i=0; i<LEVEL; i++) {
      priorityQueues.add(new TreeSet<Block>());
    }
  }

  /**
   * Empty the queues.
   */
  void clear() {
    for(int i=0; i<LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
  }

  /* Return the total number of under replication blocks */
  synchronized int size() {
    int size = 0;
    for (int i=0; i<LEVEL; i++) {
      size += priorityQueues.get(i).size();
    }
    return size;
  }
        
  /* Check if a block is in the neededReplication queue */
  synchronized boolean contains(Block block) {
    for(TreeSet<Block> set:priorityQueues) {
      if(set.contains(block)) { return true; }
    }
    return false;
  }
      
  /* Return the priority of a block
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  private int getPriority(Block block, 
                          int curReplicas, 
                          int decommissionedReplicas,
                          int expectedReplicas) {
    if (curReplicas<0 || curReplicas>=expectedReplicas) {
      return LEVEL; // no need to replicate
    } else if(curReplicas==0) {
      // If there are zero non-decommissioned replica but there are
      // some decommissioned replicas, then assign them highest priority
      if (decommissionedReplicas > 0) {
        return 0;
      }
      return 2; // keep these blocks in needed replication.
    } else if(curReplicas==1) {
      return 0; // highest priority
    } else if(curReplicas*3<expectedReplicas) {
      return 1;
    } else {
      return 2;
    }
  }
      
  /* add a block to a under replication queue according to its priority
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  synchronized boolean add(
                           Block block,
                           int curReplicas, 
                           int decomissionedReplicas,
                           int expectedReplicas) {
    if(curReplicas<0 || expectedReplicas <= curReplicas) {
      return false;
    }
    int priLevel = getPriority(block, curReplicas, decomissionedReplicas,
                               expectedReplicas);
    if(priLevel != LEVEL && priorityQueues.get(priLevel).add(block)) {
      NameNode.stateChangeLog.debug(
                                    "BLOCK* NameSystem.UnderReplicationBlock.add:"
                                    + block
                                    + " has only "+curReplicas
                                    + " replicas and need " + expectedReplicas
                                    + " replicas so is added to neededReplications"
                                    + " at priority level " + priLevel);
      return true;
    }
    return false;
  }

  /* remove a block from a under replication queue */
  synchronized boolean remove(Block block, 
                              int oldReplicas, 
                              int decommissionedReplicas,
                              int oldExpectedReplicas) {
    int priLevel = getPriority(block, oldReplicas, 
                               decommissionedReplicas,
                               oldExpectedReplicas);
    return remove(block, priLevel);
  }
      
  /* remove a block from a under replication queue given a priority*/
  boolean remove(Block block, int priLevel) {
    if(priLevel >= 0 && priLevel < LEVEL 
        && priorityQueues.get(priLevel).remove(block)) {
      NameNode.stateChangeLog.debug(
                                    "BLOCK* NameSystem.UnderReplicationBlock.remove: "
                                    + "Removing block " + block
                                    + " from priority queue "+ priLevel);
      return true;
    } else {
      for(int i=0; i<LEVEL; i++) {
        if(i!=priLevel && priorityQueues.get(i).remove(block)) {
          NameNode.stateChangeLog.debug(
                                        "BLOCK* NameSystem.UnderReplicationBlock.remove: "
                                        + "Removing block " + block
                                        + " from priority queue "+ i);
          return true;
        }
      }
    }
    return false;
  }
      
  /* update the priority level of a block */
  synchronized void update(Block block, int curReplicas, 
                           int decommissionedReplicas,
                           int curExpectedReplicas,
                           int curReplicasDelta, int expectedReplicasDelta) {
    int oldReplicas = curReplicas-curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
    int curPri = getPriority(block, curReplicas, decommissionedReplicas, curExpectedReplicas);
    int oldPri = getPriority(block, oldReplicas, decommissionedReplicas, oldExpectedReplicas);
    NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " + 
                                  block +
                                  " curReplicas " + curReplicas +
                                  " curExpectedReplicas " + curExpectedReplicas +
                                  " oldReplicas " + oldReplicas +
                                  " oldExpectedReplicas  " + oldExpectedReplicas +
                                  " curPri  " + curPri +
                                  " oldPri  " + oldPri);
    if(oldPri != LEVEL && oldPri != curPri) {
      remove(block, oldPri);
    }
    if(curPri != LEVEL && priorityQueues.get(curPri).add(block)) {
      NameNode.stateChangeLog.debug(
                                    "BLOCK* NameSystem.UnderReplicationBlock.update:"
                                    + block
                                    + " has only "+curReplicas
                                    + " replicas and need " + curExpectedReplicas
                                    + " replicas so is added to neededReplications"
                                    + " at priority level " + curPri);
    }
  }
      
  /* return an iterator of all the under replication blocks */
  public synchronized BlockIterator iterator() {
    return new BlockIterator();
  }
  
    class BlockIterator implements Iterator<Block> {
      private int level;
      private List<Iterator<Block>> iterators = new ArrayList<Iterator<Block>>();
      BlockIterator()  
      {
        level=0;
        for(int i=0; i<LEVEL; i++) {
          iterators.add(priorityQueues.get(i).iterator());
        }
      }
              
      private void update() {
        while(level< LEVEL-1 && !iterators.get(level).hasNext()) {
          level++;
        }
      }
              
      public Block next() {
        update();
        return iterators.get(level).next();
      }
              
      public boolean hasNext() {
        update();
        return iterators.get(level).hasNext();
      }
              
      public void remove() {
        iterators.get(level).remove();
      }
      
      public int getPriority() {
        return level;
    };
  }
}
