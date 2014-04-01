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
package org.apache.hadoop.tools.rumen;

/**
 * {@link MachineNode} represents the configuration of a cluster node.
 * {@link MachineNode} should be constructed by {@link MachineNode.Builder}.
 */
public final class MachineNode extends Node {
  long memory = -1; // in KB
  int mapSlots = 1;
  int reduceSlots = 1;
  long memoryPerMapSlot = -1; // in KB
  long memoryPerReduceSlot = -1; // in KB
  int numCores = 1;
  
  MachineNode(String name, int level) {
    super(name, level);
  }
  
  @Override
  public boolean equals(Object obj) {
    // name/level sufficient
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    // match equals
    return super.hashCode();
  }

  /**
   * Get the available physical RAM of the node.
   * @return The available physical RAM of the node, in KB.
   */
  public long getMemory() {
    return memory;
  }
  
  /**
   * Get the number of map slots of the node.
   * @return The number of map slots of the node.
   */
  public int getMapSlots() {
    return mapSlots;
  }
  
  /**
   * Get the number of reduce slots of the node.
   * @return The number of reduce slots fo the node.
   */
  public int getReduceSlots() {
    return reduceSlots;
  }
  
  /**
   * Get the amount of RAM reserved for each map slot.
   * @return the amount of RAM reserved for each map slot, in KB.
   */
  public long getMemoryPerMapSlot() {
    return memoryPerMapSlot;
  }

  /**
   * Get the amount of RAM reserved for each reduce slot.
   * @return the amount of RAM reserved for each reduce slot, in KB.
   */
  public long getMemoryPerReduceSlot() {
    return memoryPerReduceSlot;
  }
  
  /**
   * Get the number of cores of the node.
   * @return the number of cores of the node.
   */
  public int getNumCores() {
    return numCores;
  }

  /**
   * Get the rack node that the machine belongs to.
   * 
   * @return The rack node that the machine belongs to. Returns null if the
   *         machine does not belong to any rack.
   */
  public RackNode getRackNode() {
    return (RackNode)getParent();
  }
  
  @Override
  public synchronized boolean addChild(Node child) {
    throw new IllegalStateException("Cannot add child to MachineNode");
  }

  /**
   * Builder for a NodeInfo object
   */
  public static final class Builder {
    private MachineNode node;
    
    /**
     * Start building a new NodeInfo object.
     * @param name
     *          Unique name of the node. Typically the fully qualified domain
     *          name.
     */
    public Builder(String name, int level) {
      node = new MachineNode(name, level);
    }

    /**
     * Set the physical memory of the node.
     * @param memory Available RAM in KB.
     */
    public Builder setMemory(long memory) {
      node.memory = memory;
      return this;
    }
    
    /**
     * Set the number of map slot for the node.
     * @param mapSlots The number of map slots for the node.
     */
    public Builder setMapSlots(int mapSlots) {
      node.mapSlots = mapSlots;
      return this;
    }
    
    /**
     * Set the number of reduce slot for the node.
     * @param reduceSlots The number of reduce slots for the node.
     */   
    public Builder setReduceSlots(int reduceSlots) {
      node.reduceSlots = reduceSlots;
      return this;
    }
    
    /**
     * Set the amount of RAM reserved for each map slot.
     * @param memoryPerMapSlot The amount of RAM reserved for each map slot, in KB.
     */
    public Builder setMemoryPerMapSlot(long memoryPerMapSlot) {
      node.memoryPerMapSlot = memoryPerMapSlot;
      return this;
    }
    
    /**
     * Set the amount of RAM reserved for each reduce slot.
     * @param memoryPerReduceSlot The amount of RAM reserved for each reduce slot, in KB.
     */
    public Builder setMemoryPerReduceSlot(long memoryPerReduceSlot) {
      node.memoryPerReduceSlot = memoryPerReduceSlot;
      return this;
    }
    
    /**
     * Set the number of cores for the node.
     * @param numCores Number of cores for the node.
     */
    public Builder setNumCores(int numCores) {
      node.numCores = numCores;
      return this;
    }
    
    /**
     * Clone the settings from a reference {@link MachineNode} object.
     * @param ref The reference {@link MachineNode} object.
     */
    public Builder cloneFrom(MachineNode ref) {
      node.memory = ref.memory;
      node.mapSlots = ref.mapSlots;
      node.reduceSlots = ref.reduceSlots;
      node.memoryPerMapSlot = ref.memoryPerMapSlot;
      node.memoryPerReduceSlot = ref.memoryPerReduceSlot;
      node.numCores = ref.numCores;
      return this;
    }
    
    /**
     * Build the {@link MachineNode} object.
     * @return The {@link MachineNode} object being built.
     */
    public MachineNode build() {
      MachineNode retVal = node;
      node = null;
      return retVal;
    }
  }
}
