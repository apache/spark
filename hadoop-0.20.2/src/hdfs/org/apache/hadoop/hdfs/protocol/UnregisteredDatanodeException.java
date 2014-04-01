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

import java.io.IOException;


/**
 * This exception is thrown when a datanode that has not previously 
 * registered is trying to access the name node.
 * 
 */
public class UnregisteredDatanodeException extends IOException {

  public UnregisteredDatanodeException(DatanodeID nodeID) {
    super("Unregistered data node: " + nodeID.getName());
  }

  public UnregisteredDatanodeException(DatanodeID nodeID, 
                                       DatanodeInfo storedNode) {
    super("Data node " + nodeID.getName() 
          + " is attempting to report storage ID "
          + nodeID.getStorageID() + ". Node " 
          + storedNode.getName() + " is expected to serve this storage.");
  }
}
