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

package org.apache.hadoop.hdfs.test.system;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * Datanode client for system tests. Assumption of the class is that the
 * configuration key is set for the configuration key : {@code
 * DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY} is set, only the port portion of
 * the address is used.
 */
public class DNClient extends HDFSDaemonClient<DNProtocol> {

  DNProtocol proxy;

  public DNClient(Configuration conf, RemoteProcess process) throws IOException {
    super(conf, process);
  }

  @Override
  public void connect() throws IOException {
    if (isConnected()) {
      return;
    }
    String sockAddrStr = getConf().get(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY);
    if (sockAddrStr == null) {
      throw new IllegalArgumentException("Datenode IPC address is not set."
          + "Check if " + DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY
          + " is configured.");
    }
    String[] splits = sockAddrStr.split(":");
    if (splits.length != 2) {
      throw new IllegalArgumentException(
          "Datanode IPC address is not correctly configured");
    }
    String port = splits[1];
    String sockAddr = getHostName() + ":" + port;
    InetSocketAddress bindAddr = NetUtils.createSocketAddr(sockAddr);
    proxy = (DNProtocol) RPC.getProxy(DNProtocol.class, DNProtocol.versionID,
        bindAddr, getConf());
    setConnected(true);
  }

  @Override
  public void disconnect() throws IOException {
    RPC.stopProxy(proxy);
    setConnected(false);
  }

  @Override
  protected DNProtocol getProxy() {
    return proxy;
  }

  public Configuration getDatanodeConfig() throws IOException {
    return getProxy().getDaemonConf();
  }
}
