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

import java.util.Set;

/**
 * {@link RackNode} represents a rack node in the cluster topology.
 */
public final class RackNode extends Node {
  public RackNode(String name, int level) {
    // Hack: ensuring rack name starts with "/".
    super(name.startsWith("/") ? name : "/" + name, level);
  }
  
  @Override
  public synchronized boolean addChild(Node child) {
    if (!(child instanceof MachineNode)) {
      throw new IllegalArgumentException(
          "Only MachineNode can be added to RackNode");
    }
    return super.addChild(child);
  }
  
  /**
   * Get the machine nodes that belong to the rack.
   * @return The machine nodes that belong to the rack.
   */
  @SuppressWarnings({ "cast", "unchecked" })
  public Set<MachineNode> getMachinesInRack() {
    return (Set<MachineNode>)(Set)getChildren();
  }
}
