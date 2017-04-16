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

package org.apache.hadoop.hdfs.security;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.*;
import org.apache.commons.logging.impl.Log4JLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.log4j.Level;
import org.junit.Test;

/** Unit tests for using Delegation Token over RPC. */
public class TestClientProtocolWithDelegationToken {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG = LogFactory
      .getLog(TestClientProtocolWithDelegationToken.class);

  private static Configuration conf;
  static {
    conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  static {
    ((Log4JLogger) Client.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) Server.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslInputStream.LOG).getLogger().setLevel(Level.ALL);
  }

  @Test
  public void testDelegationTokenRpc() throws Exception {
    ClientProtocol mockNN = mock(ClientProtocol.class);
    FSNamesystem mockNameSys = mock(FSNamesystem.class);
    when(mockNN.getProtocolVersion(anyString(), anyLong())).thenReturn(
        ClientProtocol.versionID);
    DelegationTokenSecretManager sm = new DelegationTokenSecretManager(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
        3600000, mockNameSys);
    sm.startThreads();
    final Server server = RPC.getServer(mockNN, ADDRESS,
        0, 5, true, conf, sm);

    server.start();

    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    String user = current.getUserName();
    Text owner = new Text(user);
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner, owner, null);
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
        dtId, sm);
    Text host = new Text(addr.getAddress().getHostAddress() + ":"
        + addr.getPort());
    token.setService(host);
    LOG.info("Service IP address for token is " + host);
    current.addToken(token);
    current.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        ClientProtocol proxy = null;
        try {
          proxy = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
              ClientProtocol.versionID, addr, conf);
          proxy.getStats();
        } finally {
          server.stop();
          if (proxy != null) {
            RPC.stopProxy(proxy);
          }
        }
        return null;
      }
    });
  }

}
