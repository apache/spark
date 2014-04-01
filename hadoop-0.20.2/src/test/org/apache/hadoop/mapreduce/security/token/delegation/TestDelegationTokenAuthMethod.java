/** Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.mapreduce.security.token.delegation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDelegationTokenAuthMethod {
  private MiniMRCluster cluster;
  private JobConf config;

  @Before
  public void setup() throws Exception {
    config = new JobConf();
    cluster = new MiniMRCluster(0, 0, 1, "file:///", 1, null, null, null,
        config);
  }

  private Token<DelegationTokenIdentifier> generateDelegationToken(
      String owner, String renewer) {
    DelegationTokenSecretManager dtSecretManager = cluster
        .getJobTrackerRunner().getJobTracker()
        .getDelegationTokenSecretManager();
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
        owner), new Text(renewer), null);
    return new Token<DelegationTokenIdentifier>(dtId, dtSecretManager);
  }

  @Test
  public void testDelegationToken() throws Exception {
    final JobTracker jt = cluster.getJobTrackerRunner().getJobTracker();
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
    config.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    // Set configuration again so that job tracker finds security enabled
    UserGroupInformation.setConfiguration(config);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        try {
          Token<DelegationTokenIdentifier> token = jt
              .getDelegationToken(new Text(ugi.getShortUserName()));
          jt.renewDelegationToken(token);
          jt.cancelDelegationToken(token);
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
    final JobTracker jt = cluster.getJobTrackerRunner().getJobTracker();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    config.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    // Set configuration again so that job tracker finds security enabled
    UserGroupInformation.setConfiguration(config);
    Assert.assertTrue(UserGroupInformation.isSecurityEnabled());
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        try {
          Token<DelegationTokenIdentifier> token = jt
              .getDelegationToken(new Text("arenewer"));
          Assert.assertTrue(token != null);
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
    final JobTracker jt = cluster.getJobTrackerRunner().getJobTracker();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    config.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    // Set configuration again so that job tracker finds security enabled
    UserGroupInformation.setConfiguration(config);
    Assert.assertTrue(UserGroupInformation.isSecurityEnabled());
    final Token<DelegationTokenIdentifier> token = generateDelegationToken(
        "owner", ugi.getShortUserName());
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        try {
          jt.renewDelegationToken(token);
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
