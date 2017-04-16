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

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeObject;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;

/**
 * Base class for name-node upgrade objects.
 * Data-node upgrades are run in separate threads.
 */
public abstract class UpgradeObjectNamenode extends UpgradeObject {

  /**
   * Process an upgrade command.
   * RPC has only one very generic command for all upgrade related inter 
   * component communications. 
   * The actual command recognition and execution should be handled here.
   * The reply is sent back also as an UpgradeCommand.
   * 
   * @param command
   * @return the reply command which is analyzed on the client side.
   */
  public abstract UpgradeCommand processUpgradeCommand(UpgradeCommand command
                                               ) throws IOException;

  public HdfsConstants.NodeType getType() {
    return HdfsConstants.NodeType.NAME_NODE;
  }

  /**
   */
  public UpgradeCommand startUpgrade() throws IOException {
    // broadcast that data-nodes must start the upgrade
    return new UpgradeCommand(UpgradeCommand.UC_ACTION_START_UPGRADE,
                              getVersion(), (short)0);
  }

  protected FSNamesystem getFSNamesystem() {
    return FSNamesystem.getFSNamesystem();
  }

  public void forceProceed() throws IOException {
    // do nothing by default
    NameNode.LOG.info("forceProceed() is not defined for the upgrade. " 
        + getDescription());
  }
}
