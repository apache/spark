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
package org.apache.hadoop.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
import org.junit.Test;
import org.apache.hadoop.ipc.TestSaslRPC;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenSecretManager;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenIdentifier;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenSelector;
import org.apache.commons.logging.*;

/**
 *
 */
public class TestDoAsEffectiveUser {
  final private static String REAL_USER_NAME = "realUser1@HADOOP.APACHE.ORG";
  final private static String REAL_USER_SHORT_NAME = "realUser1";
  final private static String PROXY_USER_NAME = "proxyUser";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String[] GROUP_NAMES = new String[] { GROUP1_NAME,
      GROUP2_NAME };
  private static final String ADDRESS = "0.0.0.0";
  private TestProtocol proxy;
  private static Configuration masterConf = new Configuration();
  
  public static final Log LOG = LogFactory
      .getLog(TestDoAsEffectiveUser.class);
  

  static {
    masterConf.set("hadoop.security.auth_to_local",
        "RULE:[2:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//" +
        "RULE:[1:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//"
        + "DEFAULT");
    UserGroupInformation.setConfiguration(masterConf);
  }
  
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
    LOG.info("Local Ip addresses: "+builder.toString());
    conf.setStrings(ProxyUsers.getProxySuperuserIpConfKey(superUserShortName),
        builder.toString());
  }
  
  /**
   * Test method for
   * {@link org.apache.hadoop.security.UserGroupInformation#createProxyUser(java.lang.String, org.apache.hadoop.security.UserGroupInformation)}
   * .
   */
  @Test
  public void testCreateProxyUser() throws Exception {
    // ensure that doAs works correctly
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUser(
        PROXY_USER_NAME, realUserUgi);
    UserGroupInformation curUGI = proxyUserUgi
        .doAs(new PrivilegedExceptionAction<UserGroupInformation>() {
          public UserGroupInformation run() throws IOException {
            return UserGroupInformation.getCurrentUser();
          }
        });
    Assert.assertEquals(curUGI.toString(),
        PROXY_USER_NAME + " via " + REAL_USER_NAME + " (auth:SIMPLE) (auth:PROXY)");
  }

  @TokenInfo(TestTokenSelector.class)
  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;

    String aMethod() throws IOException;
  }

  public class TestImpl implements TestProtocol {

    public String aMethod() throws IOException {
      return UserGroupInformation.getCurrentUser().toString();
    }

    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      // TODO Auto-generated method stub
      return TestProtocol.versionID;
    }
  }

  @Test
  public void testRealUserSetup() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings(ProxyUsers
        .getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME), "group1");
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    Server server = RPC.getServer(new TestImpl(), ADDRESS,
        0, 5, true, conf, null);

    refreshConf(conf);
    
    try {
      server.start();

      final InetSocketAddress addr = NetUtils.getConnectAddress(server);

      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);
      UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
          PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            public String run() throws IOException {
              proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
                  TestProtocol.versionID, addr, conf);
              String ret = proxy.aMethod();
              return ret;
            }
          });

      Assert.assertEquals(PROXY_USER_NAME + " via " + REAL_USER_NAME + " (auth:SIMPLE) (auth:SIMPLE)", retVal);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  @Test
  public void testRealUserAuthorizationSuccess() throws IOException {
    final Configuration conf = new Configuration();
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    conf.setStrings(ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME),
        "group1");
    Server server = RPC.getServer(new TestImpl(), ADDRESS,
        0, 2, false, conf, null);

    refreshConf(conf);
    
    try {
      server.start();

      final InetSocketAddress addr = NetUtils.getConnectAddress(server);

      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            public String run() throws IOException {
              proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
                  TestProtocol.versionID, addr, conf);
              String ret = proxy.aMethod();
              return ret;
            }
          });

      Assert.assertEquals(PROXY_USER_NAME + " via " + REAL_USER_NAME + " (auth:SIMPLE) (auth:SIMPLE)", retVal);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  /*
   * Tests authorization of superuser's ip.
   */
  @Test
  public void testRealUserIPAuthorizationFailure() throws IOException {
    final Configuration conf = new Configuration(masterConf);
    conf.setStrings(ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_SHORT_NAME),
        "20.20.20.20"); //Authorized IP address
    conf.setStrings(ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME),
        "group1");
    Server server = RPC.getServer(new TestImpl(), ADDRESS,
        0, 2, false, conf, null);
    
    refreshConf(conf);
    
    try {
      server.start();

      final InetSocketAddress addr = NetUtils.getConnectAddress(server);

      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            public String run() throws IOException {
              proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
                  TestProtocol.versionID, addr, conf);
              String ret = proxy.aMethod();
              return ret;
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
  
  @Test
  public void testRealUserIPNotSpecified() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings(ProxyUsers
        .getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME), "group1");
    Server server = RPC.getServer(new TestImpl(), ADDRESS,
        0, 2, false, conf, null);

    refreshConf(conf);
    
    try {
      server.start();

      final InetSocketAddress addr = NetUtils.getConnectAddress(server);

      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            public String run() throws IOException {
              proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
                  TestProtocol.versionID, addr, conf);
              String ret = proxy.aMethod();
              return ret;
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  @Test
  public void testRealUserGroupNotSpecified() throws IOException {
    final Configuration conf = new Configuration();
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    Server server = RPC.getServer(new TestImpl(), ADDRESS,
        0, 2, false, conf, null);

    refreshConf(conf);
    
    try {
      server.start();

      final InetSocketAddress addr = NetUtils.getConnectAddress(server);

      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            public String run() throws IOException {
              proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
                  TestProtocol.versionID, addr, conf);
              String ret = proxy.aMethod();
              return ret;
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
  
  @Test
  public void testRealUserGroupAuthorizationFailure() throws IOException {
    final Configuration conf = new Configuration();
    configureSuperUserIPAddresses(conf, REAL_USER_SHORT_NAME);
    conf.setStrings(ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_SHORT_NAME),
        "group3");
    Server server = RPC.getServer(new TestImpl(), ADDRESS,
        0, 2, false, conf, null);

    refreshConf(conf);
    
    try {
      server.start();

      final InetSocketAddress addr = NetUtils.getConnectAddress(server);

      UserGroupInformation realUserUgi = UserGroupInformation
          .createRemoteUser(REAL_USER_NAME);

      UserGroupInformation proxyUserUgi = UserGroupInformation
          .createProxyUserForTesting(PROXY_USER_NAME, realUserUgi, GROUP_NAMES);
      String retVal = proxyUserUgi
          .doAs(new PrivilegedExceptionAction<String>() {
            public String run() throws IOException {
              proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
                  TestProtocol.versionID, addr, conf);
              String ret = proxy.aMethod();
              return ret;
            }
          });

      Assert.fail("The RPC must have failed " + retVal);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  /*
   *  Tests the scenario when token authorization is used.
   *  The server sees only the the owner of the token as the
   *  user.
   */
  @Test
  public void testProxyWithToken() throws Exception {
    final Configuration conf = new Configuration(masterConf);
    TestTokenSecretManager sm = new TestTokenSecretManager();
    conf
        .set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    final Server server = RPC.getServer(new TestImpl(),
        ADDRESS, 0, 5, true, conf, sm);

    server.start();

    final UserGroupInformation current = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()), new Text("SomeSuperUser"));
    Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(tokenId,
        sm);
    Text host = new Text(addr.getAddress().getHostAddress() + ":"
        + addr.getPort());
    token.setService(host);
    UserGroupInformation proxyUserUgi = UserGroupInformation
        .createProxyUserForTesting(PROXY_USER_NAME, current, GROUP_NAMES);
    proxyUserUgi.addToken(token);
    
    refreshConf(conf);
    
    String retVal = proxyUserUgi.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {
        try {
          proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
              TestProtocol.versionID, addr, conf);
          String ret = proxy.aMethod();
          return ret;
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        } finally {
          server.stop();
          if (proxy != null) {
            RPC.stopProxy(proxy);
          }
        }
      }
    });
    //The user returned by server must be the one in the token.
    Assert.assertEquals(REAL_USER_NAME + " via SomeSuperUser (auth:SIMPLE) (auth:TOKEN)", retVal);
  }

  /*
   * The user gets the token via a superuser. Server should authenticate
   * this user. 
   */
  @Test
  public void testTokenBySuperUser() throws Exception {
    TestTokenSecretManager sm = new TestTokenSecretManager();
    final Configuration newConf = new Configuration(masterConf);
    newConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(newConf);
    final Server server = RPC.getServer(new TestImpl(),
        ADDRESS, 0, 5, true, newConf, sm);

    server.start();

    final UserGroupInformation current = UserGroupInformation
        .createUserForTesting(REAL_USER_NAME, GROUP_NAMES);
    refreshConf(newConf);
    
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()), new Text("SomeSuperUser"));
    Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(tokenId,
        sm);
    Text host = new Text(addr.getAddress().getHostAddress() + ":"
        + addr.getPort());
    token.setService(host);
    current.addToken(token);
    String retVal = current.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {
        try {
          proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
              TestProtocol.versionID, addr, newConf);
          String ret = proxy.aMethod();
          return ret;
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        } finally {
          server.stop();
          if (proxy != null) {
            RPC.stopProxy(proxy);
          }
        }
      }
    });
    String expected = REAL_USER_NAME + " via SomeSuperUser (auth:SIMPLE) (auth:TOKEN)";
    Assert.assertEquals(retVal + "!=" + expected, expected, retVal);
  }
  
  //
  private void refreshConf(Configuration conf) throws IOException {
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }
}
