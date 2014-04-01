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

import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.hdfs.util.LightWeightGSet;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes INode it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {
        
  /**
   * Internal class for block metadata.
   */
  static class BlockInfo extends Block implements LightWeightGSet.LinkedElement {
    private INodeFile          inode;

    /** For implementing {@link LightWeightGSet.LinkedElement} interface */
    private LightWeightGSet.LinkedElement nextLinkedElement;

    /**
     * This array contains triplets of references.
     * For each i-th data-node the block belongs to
     * triplets[3*i] is the reference to the DatanodeDescriptor
     * and triplets[3*i+1] and triplets[3*i+2] are references 
     * to the previous and the next blocks, respectively, in the 
     * list of blocks belonging to this data-node.
     */
    private Object[] triplets;

    BlockInfo(Block blk, int replication) {
      super(blk);
      this.triplets = new Object[3*replication];
      this.inode = null;
    }

    INodeFile getINode() {
      return inode;
    }

    DatanodeDescriptor getDatanode(int index) {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
      DatanodeDescriptor node = (DatanodeDescriptor)triplets[index*3];
      assert node == null || 
          DatanodeDescriptor.class.getName().equals(node.getClass().getName()) : 
                "DatanodeDescriptor is expected at " + index*3;
      return node;
    }

    BlockInfo getPrevious(int index) {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
      BlockInfo info = (BlockInfo)triplets[index*3+1];
      assert info == null || 
          BlockInfo.class.getName().equals(info.getClass().getName()) : 
                "BlockInfo is expected at " + index*3;
      return info;
    }

    BlockInfo getNext(int index) {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
      BlockInfo info = (BlockInfo)triplets[index*3+2];
      assert info == null || 
          BlockInfo.class.getName().equals(info.getClass().getName()) : 
                "BlockInfo is expected at " + index*3;
      return info;
    }

    void setDatanode(int index, DatanodeDescriptor node) {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
      triplets[index*3] = node;
    }

    void setPrevious(int index, BlockInfo to) {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
      triplets[index*3+1] = to;
    }

    void setNext(int index, BlockInfo to) {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
      triplets[index*3+2] = to;
    }

    private int getCapacity() {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert triplets.length % 3 == 0 : "Malformed BlockInfo";
      return triplets.length / 3;
    }

    /**
     * Ensure that there is enough  space to include num more triplets.
     *      * @return first free triplet index.
     */
    private int ensureCapacity(int num) {
      assert this.triplets != null : "BlockInfo is not initialized";
      int last = numNodes();
      if(triplets.length >= (last+num)*3)
        return last;
      /* Not enough space left. Create a new array. Should normally 
       * happen only when replication is manually increased by the user. */
      Object[] old = triplets;
      triplets = new Object[(last+num)*3];
      for(int i=0; i < last*3; i++) {
        triplets[i] = old[i];
      }
      return last;
    }

    /**
     * Count the number of data-nodes the block belongs to.
     */
    int numNodes() {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert triplets.length % 3 == 0 : "Malformed BlockInfo";
      for(int idx = getCapacity()-1; idx >= 0; idx--) {
        if(getDatanode(idx) != null)
          return idx+1;
      }
      return 0;
    }

    /**
     * Add data-node this block belongs to.
     */
    boolean addNode(DatanodeDescriptor node) {
      if(findDatanode(node) >= 0) // the node is already there
        return false;
      // find the last null node
      int lastNode = ensureCapacity(1);
      setDatanode(lastNode, node);
      setNext(lastNode, null);
      setPrevious(lastNode, null);
      return true;
    }

    /**
     * Remove data-node from the block.
     */
    boolean removeNode(DatanodeDescriptor node) {
      int dnIndex = findDatanode(node);
      if(dnIndex < 0) // the node is not found
        return false;
      assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
        "Block is still in the list and must be removed first.";
      // find the last not null node
      int lastNode = numNodes()-1; 
      // replace current node triplet by the lastNode one 
      setDatanode(dnIndex, getDatanode(lastNode));
      setNext(dnIndex, getNext(lastNode)); 
      setPrevious(dnIndex, getPrevious(lastNode)); 
      // set the last triplet to null
      setDatanode(lastNode, null);
      setNext(lastNode, null); 
      setPrevious(lastNode, null); 
      return true;
    }

    /**
     * Find specified DatanodeDescriptor.
     * @param dn
     * @return index or -1 if not found.
     */
    int findDatanode(DatanodeDescriptor dn) {
      int len = getCapacity();
      for(int idx = 0; idx < len; idx++) {
        DatanodeDescriptor cur = getDatanode(idx);
        if(cur == dn)
          return idx;
        if(cur == null)
          break;
      }
      return -1;
    }

    /**
     * Insert this block into the head of the list of blocks 
     * related to the specified DatanodeDescriptor.
     * If the head is null then form a new list.
     * @return current block as the new head of the list.
     */
    BlockInfo listInsert(BlockInfo head, DatanodeDescriptor dn) {
      int dnIndex = this.findDatanode(dn);
      assert dnIndex >= 0 : "Data node is not found: current";
      assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
              "Block is already in the list and cannot be inserted.";
      this.setPrevious(dnIndex, null);
      this.setNext(dnIndex, head);
      if(head != null)
        head.setPrevious(head.findDatanode(dn), this);
      return this;
    }

    /**
     * Remove this block from the list of blocks 
     * related to the specified DatanodeDescriptor.
     * If this block is the head of the list then return the next block as 
     * the new head.
     * @return the new head of the list or null if the list becomes
     * empy after deletion.
     */
    BlockInfo listRemove(BlockInfo head, DatanodeDescriptor dn) {
      if(head == null)
        return null;
      int dnIndex = this.findDatanode(dn);
      if(dnIndex < 0) // this block is not on the data-node list
        return head;

      BlockInfo next = this.getNext(dnIndex);
      BlockInfo prev = this.getPrevious(dnIndex);
      this.setNext(dnIndex, null);
      this.setPrevious(dnIndex, null);
      if(prev != null)
        prev.setNext(prev.findDatanode(dn), next);
      if(next != null)
        next.setPrevious(next.findDatanode(dn), prev);
      if(this == head)  // removing the head
        head = next;
      return head;
    }

    int listCount(DatanodeDescriptor dn) {
      int count = 0;
      for(BlockInfo cur = this; cur != null;
            cur = cur.getNext(cur.findDatanode(dn)))
        count++;
      return count;
    }

    boolean listIsConsistent(DatanodeDescriptor dn) {
      // going forward
      int count = 0;
      BlockInfo next, nextPrev;
      BlockInfo cur = this;
      while(cur != null) {
        next = cur.getNext(cur.findDatanode(dn));
        if(next != null) {
          nextPrev = next.getPrevious(next.findDatanode(dn));
          if(cur != nextPrev) {
            System.out.println("Inconsistent list: cur->next->prev != cur");
            return false;
          }
        }
        cur = next;
        count++;
      }
      return true;
    }

    @Override
    public LightWeightGSet.LinkedElement getNext() {
      return nextLinkedElement;
    }

    @Override
    public void setNext(LightWeightGSet.LinkedElement next) {
      this.nextLinkedElement = next;
    }
  }

  private static class NodeIterator implements Iterator<DatanodeDescriptor> {
    private BlockInfo blockInfo;
    private int nextIdx = 0;
      
    NodeIterator(BlockInfo blkInfo) {
      this.blockInfo = blkInfo;
    }

    public boolean hasNext() {
      return blockInfo != null && nextIdx < blockInfo.getCapacity()
              && blockInfo.getDatanode(nextIdx) != null;
    }

    public DatanodeDescriptor next() {
      return blockInfo.getDatanode(nextIdx++);
    }

    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  /** Constant {@link LightWeightGSet} capacity. */
  private final int capacity;
  
  private GSet<Block, BlockInfo> blocks;

  BlocksMap(int initialCapacity, float loadFactor) {
    this.capacity = computeCapacity();
    this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity);
  }

  /**
   * Let t = 2% of max memory.
   * Let e = round(log_2 t).
   * Then, we choose capacity = 2^e/(size of reference),
   * unless it is outside the close interval [1, 2^30].
   */
  private static int computeCapacity() {
    //VM detection
    //See http://java.sun.com/docs/hotspot/HotSpotFAQ.html#64bit_detection
    final String vmBit = System.getProperty("sun.arch.data.model");

    //2% of max memory
    final double twoPC = Runtime.getRuntime().maxMemory()/50.0;

    //compute capacity
    final int e1 = (int)(Math.log(twoPC)/Math.log(2.0) + 0.5);
    final int e2 = e1 - ("32".equals(vmBit)? 2: 3);
    final int exponent = e2 < 0? 0: e2 > 30? 30: e2;
    final int c = 1 << exponent;

    LightWeightGSet.LOG.info("VM type       = " + vmBit + "-bit");
    LightWeightGSet.LOG.info("2% max memory = " + twoPC/(1 << 20) + " MB");
    LightWeightGSet.LOG.info("capacity      = 2^" + exponent
        + " = " + c + " entries");
    return c;
  }

  void close() {
    blocks = null;
  }

  /**
   * Add BlockInfo if mapping does not exist.
   */
  private BlockInfo checkBlockInfo(Block b, int replication) {
    BlockInfo info = blocks.get(b);
    if (info == null) {
      info = new BlockInfo(b, replication);
      blocks.put(info);
    }
    return info;
  }

  INodeFile getINode(Block b) {
    BlockInfo info = blocks.get(b);
    return (info != null) ? info.inode : null;
  }

  /**
   * Add block b belonging to the specified file inode to the map.
   */
  BlockInfo addINode(Block b, INodeFile iNode) {
    BlockInfo info = checkBlockInfo(b, iNode.getReplication());
    info.inode = iNode;
    return info;
  }

  /**
   * Remove INode reference from block b.
   * If it does not belong to any file and data-nodes,
   * then remove the block from the block map.
   */
  void removeINode(Block b) {
    BlockInfo info = blocks.get(b);
    if (info != null) {
      info.inode = null;
      if (info.getDatanode(0) == null) {  // no datanodes left
        blocks.remove(b);  // remove block from the map
      }
    }
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(BlockInfo blockInfo) {
    if (blockInfo == null)
      return;
    blockInfo.inode = null;
    for(int idx = blockInfo.numNodes()-1; idx >= 0; idx--) {
      DatanodeDescriptor dn = blockInfo.getDatanode(idx);
      dn.removeBlock(blockInfo); // remove from the list and wipe the location
    }
    blocks.remove(blockInfo);  // remove block from the map
  }

  /** Returns the block object it it exists in the map. */
  BlockInfo getStoredBlock(Block b) {
    return blocks.get(b);
  }

  /** Return the block object without matching against generation stamp. */
  BlockInfo getStoredBlockWithoutMatchingGS(Block b) {
    return blocks.get(new Block(b.getBlockId()));
  }

  /** Returned Iterator does not support. */
  Iterator<DatanodeDescriptor> nodeIterator(Block b) {
    return new NodeIterator(blocks.get(b));
  }

  /** counts number of containing nodes. Better than using iterator. */
  int numNodes(Block b) {
    BlockInfo info = blocks.get(b);
    return info == null ? 0 : info.numNodes();
  }

  /** returns true if the node does not already exists and is added.
   * false if the node already exists.*/
  boolean addNode(Block b, DatanodeDescriptor node, int replication) {
    // insert into the map if not there yet
    BlockInfo info = checkBlockInfo(b, replication);
    // add block to the data-node list and the node to the block info
    return node.addBlock(info);
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = blocks.get(b);
    if (info == null)
      return false;

    // remove block from the data-node list and the node from the block info
    boolean removed = node.removeBlock(info);

    if (info.getDatanode(0) == null     // no datanodes left
              && info.inode == null) {  // does not belong to a file
      blocks.remove(b);  // remove block from the map
    }
    return removed;
  }

  int size() {
    return blocks.size();
  }

  Iterable<BlockInfo> getBlocks() {
    return blocks;
  }
  /**
   * Check if the block exists in map
   */
  boolean contains(Block block) {
    return blocks.contains(block);
  }
  
  /**
   * Check if the replica at the given datanode exists in map
   */
  boolean contains(Block block, DatanodeDescriptor datanode) {
    BlockInfo info = blocks.get(block);
    if (info == null)
      return false;
    
    if (-1 == info.findDatanode(datanode))
      return false;
    
    return true;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  public int getCapacity() {
    return capacity;
  }
}
