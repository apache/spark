/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.system.process.RemoteProcess;

public class NNClient extends HDFSDaemonClient<NNProtocol> {
  
  NNProtocol proxy;

  public NNClient(Configuration conf, RemoteProcess process) throws IOException {
    super(conf, process);
  }

  @Override
  public void connect() throws IOException {
    if (isConnected())
      return;
    String sockAddrStr = FileSystem.getDefaultUri(getConf()).getAuthority();
    if (sockAddrStr == null) {
      throw new IllegalArgumentException("Namenode IPC address is not set");
    }
    String[] splits = sockAddrStr.split(":");
    if (splits.length != 2) {
      throw new IllegalArgumentException(
          "Namenode report IPC is not correctly configured");
    }
    String port = splits[1];
    String sockAddr = getHostName() + ":" + port;

    InetSocketAddress bindAddr = NetUtils.createSocketAddr(sockAddr);
    proxy = (NNProtocol) RPC.getProxy(NNProtocol.class, NNProtocol.versionID,
        bindAddr, getConf());
    setConnected(true);
  }

  @Override
  public void disconnect() throws IOException {
    RPC.stopProxy(proxy);
    setConnected(false);
  }

  @Override
  protected NNProtocol getProxy() {
    return proxy;
  }
}
