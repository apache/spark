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



import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.*;
import org.apache.hadoop.security.TestDoAsEffectiveUser;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDelegationTokenForProxyUser {
  private MiniDFSCluster cluster;
  Configuration config;
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String[] GROUP_NAMES = new String[] { GROUP1_NAME,
      GROUP2_NAME };
  final private static String REAL_USER = "RealUser";
  final private static String PROXY_USER = "ProxyUser";
  
  private static final Log LOG = LogFactory.getLog(TestDoAsEffectiveUser.class);
  
  private void configureSuperUserIPAddresses(Configuration conf,
      String superUserShortName) throws IOException {
    ArrayList<String> ipList = new ArrayList<String>();
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface
        .getNetworkInterfaces();
    while (netInterfaceList.hasMoreElements()) {
      NetworkInterface inf = netInterfaceList.nextElement();
      Enumeration<InetAddress> addrList = inf.getInetAddresses();
      while (addrList.hasMoreElements()) {
        InetAddress addr = addrList.nextElement();
        ipList.add(addr.getHostAddress());
      }
    }
    StringBuilder builder = new StringBuilder();
    for (String ip : ipList) {
      builder.append(ip);
      builder.append(',');
    }
    builder.append("127.0.1.1,");
    builder.append(InetAddress.getLocalHost().getCanonicalHostName());
    LOG.info("Local Ip addresses: " + builder.toString());
    conf.setStrings(ProxyUsers.getProxySuperuserIpConfKey(superUserShortName),
        builder.toString());
  }
  
  @Before
  public void setUp() throws Exception {
    config = new Configuration();
    config.setLong(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
    config.setLong(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 5000);
    config.setStrings(ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER),
        "group1");
    configureSuperUserIPAddresses(config, REAL_USER);
    FileSystem.setDefaultUri(config, "hdfs://localhost:" + "0");
    cluster = new MiniDFSCluster(0, config, 1, true, true, true, null, null,
        null, null);
    cluster.waitActive();
    cluster.getNameNode().getNamesystem().getDelegationTokenSecretManager().startThreads();
    ProxyUsers.refreshSuperUserGroupsConfiguration(config);
  }

  @After
  public void tearDown() throws Exception {
    if(cluster!=null) {
      cluster.shutdown();
    }
  }
 
  @Test
  public void testDelegationTokenWithRealUser() throws IOException {
    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(REAL_USER);
    final UserGroupInformation proxyUgi = UserGroupInformation
        .createProxyUserForTesting(PROXY_USER, ugi, GROUP_NAMES);
    try {
      Token<DelegationTokenIdentifier> token = proxyUgi
          .doAs(new PrivilegedExceptionAction<Token<DelegationTokenIdentifier>>() {
            public Token<DelegationTokenIdentifier> run() throws IOException {
              DistributedFileSystem dfs = (DistributedFileSystem) cluster
                  .getFileSystem();
              return dfs.getDelegationToken(new Text("RenewerUser"));
            }
          });
      DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
      byte[] tokenId = token.getIdentifier();
      identifier.readFields(new DataInputStream(new ByteArrayInputStream(
          tokenId)));
      Assert.assertEquals(identifier.getUser().getUserName(), PROXY_USER);
      Assert.assertEquals(identifier.getUser().getRealUser().getUserName(),
          REAL_USER);
    } catch (InterruptedException e) {
      //Do Nothing
    }
  }
  
}
