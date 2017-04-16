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
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;

import junit.framework.Assert;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenSecretManager;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

/**
 * MiniRPCBenchmark measures time to establish an RPC connection 
 * to a secure RPC server.
 * It sequentially establishes connections the specified number of times, 
 * and calculates the average time taken to connect.
 * The time to connect includes the server side authentication time.
 * The benchmark supports three authentication methods:
 * <ol>
 * <li>simple - no authentication. In order to enter this mode 
 * the configuration file <tt>core-site.xml</tt> should specify
 * <tt>hadoop.security.authentication = simple</tt>.
 * This is the default mode.</li>
 * <li>kerberos - kerberos authentication. In order to enter this mode 
 * the configuration file <tt>core-site.xml</tt> should specify
 * <tt>hadoop.security.authentication = kerberos</tt> and 
 * the argument string should provide qualifying
 * <tt>keytabFile</tt> and <tt>userName</tt> parameters.
 * <li>delegation token - authentication using delegation token.
 * In order to enter this mode the benchmark should provide all the
 * mentioned parameters for kerberos authentication plus the
 * <tt>useToken</tt> argument option.
 * </ol>
 * Input arguments:
 * <ul>
 * <li>numIterations - number of connections to establish</li>
 * <li>keytabFile - keytab file for kerberos authentication</li>
 * <li>userName - principal name for kerberos authentication</li>
 * <li>useToken - should be specified for delegation token authentication</li>
 * <li>logLevel - logging level, see {@link Level}</li>
 * </ul>
 */
public class MiniRPCBenchmark {
  private static final String KEYTAB_FILE_KEY = "test.keytab.file";
  private static final String USER_NAME_KEY = "test.user.name";
  private static final String MINI_USER = "miniUser";
  private static final String RENEWER = "renewer";
  private static final String GROUP_NAME_1 = "MiniGroup1";
  private static final String GROUP_NAME_2 = "MiniGroup2";
  private static final String[] GROUP_NAMES = 
                                    new String[] {GROUP_NAME_1, GROUP_NAME_2};

  private UserGroupInformation currentUgi;
  private Level logLevel;

  MiniRPCBenchmark(Level l) {
    currentUgi = null;
    logLevel = l;
  }

  public static class TestDelegationTokenSelector extends 
  AbstractDelegationTokenSelector<TestDelegationTokenIdentifier>{

    protected TestDelegationTokenSelector() {
      super(new Text("MY KIND"));
    }    
  }
  
  @KerberosInfo(serverPrincipal = USER_NAME_KEY)
  @TokenInfo(TestDelegationTokenSelector.class)
  public static interface MiniProtocol extends VersionedProtocol {
    public static final long versionID = 1L;

    /**
     * Get a Delegation Token.
     */
    public Token<TestDelegationTokenIdentifier> getDelegationToken(Text renewer) 
        throws IOException;
  }

  /**
   * Primitive RPC server, which
   * allows clients to connect to it.
   */
  static class MiniServer implements MiniProtocol {
    private static final String DEFAULT_SERVER_ADDRESS = "0.0.0.0";

    private TestDelegationTokenSecretManager secretManager;
    private Server rpcServer;

    @Override // VersionedProtocol
    public long getProtocolVersion(String protocol, 
                                   long clientVersion) throws IOException {
      if (protocol.equals(MiniProtocol.class.getName()))
        return versionID;
      throw new IOException("Unknown protocol: " + protocol);
    }

    @Override // MiniProtocol
    public Token<TestDelegationTokenIdentifier> getDelegationToken(Text renewer) 
    throws IOException {
      String owner = UserGroupInformation.getCurrentUser().getUserName();
      String realUser = 
        UserGroupInformation.getCurrentUser().getRealUser() == null ? "":
        UserGroupInformation.getCurrentUser().getRealUser().getUserName();
      TestDelegationTokenIdentifier tokenId = 
        new TestDelegationTokenIdentifier(
            new Text(owner), renewer, new Text(realUser));
      return new Token<TestDelegationTokenIdentifier>(tokenId, secretManager);
    }

    /** Start RPC server */
    MiniServer(Configuration conf, String user, String keytabFile)
    throws IOException {
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(user, keytabFile);
      secretManager = 
        new TestDelegationTokenSecretManager(24*60*60*1000,
            7*24*60*60*1000,24*60*60*1000,3600000);
      secretManager.startThreads();
      rpcServer = RPC.getServer(
          this, DEFAULT_SERVER_ADDRESS, 0, 1, false, conf, secretManager);
      rpcServer.start();
    }

    /** Stop RPC server */
    void stop() {
      if(rpcServer != null) rpcServer.stop();
      rpcServer = null;
    }

    /** Get RPC server address */
    InetSocketAddress getAddress() {
      if(rpcServer == null) return null;
      return NetUtils.getConnectAddress(rpcServer);
    }
  }

  long connectToServer(Configuration conf, InetSocketAddress addr)
  throws IOException {
    MiniProtocol client = null;
    try {
      long start = System.currentTimeMillis();
      client = (MiniProtocol) RPC.getProxy(MiniProtocol.class,
          MiniProtocol.versionID, addr, conf);
      long end = System.currentTimeMillis();
      return end - start;
    } finally {
      RPC.stopProxy(client);
    }
  }

  void connectToServerAndGetDelegationToken(
      final Configuration conf, final InetSocketAddress addr) throws IOException {
    MiniProtocol client = null;
    try {
      UserGroupInformation current = UserGroupInformation.getCurrentUser();
      UserGroupInformation proxyUserUgi = 
        UserGroupInformation.createProxyUserForTesting(
            MINI_USER, current, GROUP_NAMES);
      
      try {
        client =  proxyUserUgi.doAs(new PrivilegedExceptionAction<MiniProtocol>() {
          public MiniProtocol run() throws IOException {
            MiniProtocol p = (MiniProtocol) RPC.getProxy(MiniProtocol.class,
                MiniProtocol.versionID, addr, conf);
            Token<TestDelegationTokenIdentifier> token;
            token = p.getDelegationToken(new Text(RENEWER));
            currentUgi = UserGroupInformation.createUserForTesting(MINI_USER, 
                GROUP_NAMES);
            token.setService(new Text(addr.getAddress().getHostAddress() 
                + ":" + addr.getPort()));
            currentUgi.addToken(token);
            return p;
          }
        });
      } catch (InterruptedException e) {
        Assert.fail(Arrays.toString(e.getStackTrace()));
      }
    } finally {
      RPC.stopProxy(client);
    }
  }

  long connectToServerUsingDelegationToken(
      final Configuration conf, final InetSocketAddress addr) throws IOException {
    MiniProtocol client = null;
    try {
      long start = System.currentTimeMillis();
      try {
        client = currentUgi.doAs(new PrivilegedExceptionAction<MiniProtocol>() {
          public MiniProtocol run() throws IOException {
            return (MiniProtocol) RPC.getProxy(MiniProtocol.class,
                MiniProtocol.versionID, addr, conf);
          }
        });
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      long end = System.currentTimeMillis();
      return end - start;
    } finally {
      RPC.stopProxy(client);
    }
  }

  static void setLoggingLevel(Level level) {
    LogManager.getLogger(Server.class.getName()).setLevel(level);
    LogManager.getLogger("SecurityLogger."+Server.class.getName()).setLevel(level);
    LogManager.getLogger(Client.class.getName()).setLevel(level);
  }

  /**
   * Run MiniBenchmark with MiniServer as the RPC server.
   * 
   * @param conf - configuration
   * @param count - connect this many times
   * @param keytabKey - key for keytab file in the configuration
   * @param userNameKey - key for user name in the configuration
   * @return average time to connect
   * @throws IOException
   */
  long runMiniBenchmark(Configuration conf,
                        int count,
                        String keytabKey,
                        String userNameKey) throws IOException {
    // get login information
    String user = System.getProperty("user.name");
    if(userNameKey != null)
      user = conf.get(userNameKey, user);
    String keytabFile = null;
    if(keytabKey != null)
      keytabFile = conf.get(keytabKey, keytabFile);
    MiniServer miniServer = null;
    try {
      // start the server
      miniServer = new MiniServer(conf, user, keytabFile);
      InetSocketAddress addr = miniServer.getAddress();

      connectToServer(conf, addr);
      // connect to the server count times
      setLoggingLevel(logLevel);
      long elapsed = 0L;
      for(int idx = 0; idx < count; idx ++) {
        elapsed += connectToServer(conf, addr);
      }
      return elapsed;
    } finally {
      if(miniServer != null) miniServer.stop();
    }
  }

  /**
   * Run MiniBenchmark using delegation token authentication.
   * 
   * @param conf - configuration
   * @param count - connect this many times
   * @param keytabKey - key for keytab file in the configuration
   * @param userNameKey - key for user name in the configuration
   * @return average time to connect
   * @throws IOException
   */
  long runMiniBenchmarkWithDelegationToken(Configuration conf,
                                           int count,
                                           String keytabKey,
                                           String userNameKey)
  throws IOException {
    // get login information
    String user = System.getProperty("user.name");
    if(userNameKey != null)
      user = conf.get(userNameKey, user);
    String keytabFile = null;
    if(keytabKey != null)
      keytabFile = conf.get(keytabKey, keytabFile);
    MiniServer miniServer = null;
    UserGroupInformation.setConfiguration(conf);
    String shortUserName =
      UserGroupInformation.createRemoteUser(user).getShortUserName();
    try {
      conf.setStrings(ProxyUsers.getProxySuperuserGroupConfKey(shortUserName),
          GROUP_NAME_1);
      configureSuperUserIPAddresses(conf, shortUserName);
      // start the server
      miniServer = new MiniServer(conf, user, keytabFile);
      InetSocketAddress addr = miniServer.getAddress();

      connectToServerAndGetDelegationToken(conf, addr);
      // connect to the server count times
      setLoggingLevel(logLevel);
      long elapsed = 0L;
      for(int idx = 0; idx < count; idx ++) {
        elapsed += connectToServerUsingDelegationToken(conf, addr);
      }
      return elapsed;
    } finally {
      if(miniServer != null) miniServer.stop();
    }
  }

  static void printUsage() {
    System.err.println(
        "Usage: MiniRPCBenchmark <numIterations> [<keytabFile> [<userName> " +
        "[useToken|useKerberos [<logLevel>]]]]");
    System.exit(-1);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Benchmark: RPC session establishment.");
    if(args.length < 1)
      printUsage();

    Configuration conf = new Configuration();
    int count = Integer.parseInt(args[0]);
    if(args.length > 1)
      conf.set(KEYTAB_FILE_KEY, args[1]);
    if(args.length > 2)
      conf.set(USER_NAME_KEY, args[2]);
    boolean useDelegationToken = false;
    if(args.length > 3)
      useDelegationToken = args[3].equalsIgnoreCase("useToken");
    Level l = Level.ERROR;
    if(args.length > 4)
      l = Level.toLevel(args[4]);

    MiniRPCBenchmark mb = new MiniRPCBenchmark(l);
    long elapsedTime = 0;
    if(useDelegationToken) {
      System.out.println(
          "Running MiniRPCBenchmark with delegation token authentication.");
      elapsedTime = mb.runMiniBenchmarkWithDelegationToken(
                              conf, count, KEYTAB_FILE_KEY, USER_NAME_KEY);
    } else {
      String auth = 
        conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, 
                        "simple");
      System.out.println(
          "Running MiniRPCBenchmark with " + auth + " authentication.");
      elapsedTime = mb.runMiniBenchmark(
                              conf, count, KEYTAB_FILE_KEY, USER_NAME_KEY);
    }
    System.out.println(org.apache.hadoop.util.VersionInfo.getVersion());
    System.out.println("Number  of  connects: " + count);
    System.out.println("Average connect time: " + ((double)elapsedTime/count));
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
    conf.setStrings(ProxyUsers.getProxySuperuserIpConfKey(superUserShortName),
        builder.toString());
  }
}
