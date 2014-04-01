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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDelegationTokenAuthMethod {
  private MiniDFSCluster cluster;
  Configuration config;
  
  @Before
  public void setUp() throws Exception {
    config = new Configuration();
    FileSystem.setDefaultUri(config, "hdfs://localhost:" + "0");
    cluster = new MiniDFSCluster(0, config, 1, true, true, true,  null, null, null, null);
    cluster.waitActive();
    cluster.getNameNode().getNamesystem().getDelegationTokenSecretManager().startThreads();
  }

  @After
  public void tearDown() throws Exception {
    if(cluster!=null) {
      cluster.shutdown();
    }
  }
  
  private Token<DelegationTokenIdentifier> generateDelegationToken(
      String owner, String renewer) {
    DelegationTokenSecretManager dtSecretManager = cluster.getNameNode().getNamesystem()
        .getDelegationTokenSecretManager();
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
        owner), new Text(renewer), null);
    return new Token<DelegationTokenIdentifier>(dtId, dtSecretManager);
  }
  
  @Test
  public void testDelegationTokenNamesystemApi() throws Exception {
    final FSNamesystem namesys = cluster.getNameNode().getNamesystem();
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
    config.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    //Set conf again so that namesystem finds security enabled
    UserGroupInformation.setConfiguration(config);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        try {
          Token<DelegationTokenIdentifier> token = namesys
              .getDelegationToken(new Text(ugi.getShortUserName()));
          namesys.renewDelegationToken(token);
          namesys.cancelDelegationToken(token);
        } catch (IOException e) {
          e.printStackTrace();
          throw e;
        }
        return null;
      }
    });
  }
  
  @Test
  public void testGetDelegationTokenWithoutKerberos() throws Exception {
    final FSNamesystem namesys = cluster.getNameNode().getNamesystem();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    config.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    //Set conf again so that namesystem finds security enabled
    UserGroupInformation.setConfiguration(config);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        try {
          namesys.getDelegationToken(new Text("arenewer"));
          Assert
              .fail("Delegation token should not be issued without Kerberos authentication");
        } catch (IOException e) {
          // success
        }
        return null;
      }
    });
  }

  @Test
  public void testRenewDelegationTokenWithoutKerberos() throws Exception {
    final FSNamesystem namesys = cluster.getNameNode().getNamesystem();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    config.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    //Set conf again so that namesystem finds security enabled
    UserGroupInformation.setConfiguration(config);
    final Token<DelegationTokenIdentifier> token = generateDelegationToken(
        "owner", ugi.getShortUserName());
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        try {
          namesys.renewDelegationToken(token);
          Assert
              .fail("Delegation token should not be renewed without Kerberos authentication");
        } catch (IOException e) {
          // success
        }
        return null;
      }
    });
  }
}
