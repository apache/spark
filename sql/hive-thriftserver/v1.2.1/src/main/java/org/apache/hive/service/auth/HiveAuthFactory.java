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
package org.apache.hive.service.auth;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import javax.net.ssl.SSLServerSocket;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.DBTokenStore;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class HiveAuthFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAuthFactory.class);


  public enum AuthTypes {
    NOSASL("NOSASL"),
    NONE("NONE"),
    LDAP("LDAP"),
    KERBEROS("KERBEROS"),
    CUSTOM("CUSTOM"),
    PAM("PAM");

    private final String authType;

    AuthTypes(String authType) {
      this.authType = authType;
    }

    public String getAuthName() {
      return authType;
    }

  }

  private HadoopThriftAuthBridge.Server saslServer;
  private String authTypeStr;
  private final String transportMode;
  private final HiveConf conf;

  public static final String HS2_PROXY_USER = "hive.server2.proxy.user";
  public static final String HS2_CLIENT_TOKEN = "hiveserver2ClientToken";

  private static Field keytabFile = null;
  private static Method getKeytab = null;
  static {
    Class<?> clz = UserGroupInformation.class;
    try {
      keytabFile = clz.getDeclaredField("keytabFile");
      keytabFile.setAccessible(true);
    } catch (NoSuchFieldException nfe) {
      LOG.debug("Cannot find private field \"keytabFile\" in class: " +
        UserGroupInformation.class.getCanonicalName(), nfe);
      keytabFile = null;
    }

    try {
      getKeytab = clz.getDeclaredMethod("getKeytab");
      getKeytab.setAccessible(true);
    } catch(NoSuchMethodException nme) {
      LOG.debug("Cannot find private method \"getKeytab\" in class:" +
        UserGroupInformation.class.getCanonicalName(), nme);
      getKeytab = null;
    }
  }

  public HiveAuthFactory(HiveConf conf) throws TTransportException, IOException {
    this.conf = conf;
    transportMode = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    authTypeStr = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION);

    // In http mode we use NOSASL as the default auth type
    if ("http".equalsIgnoreCase(transportMode)) {
      if (authTypeStr == null) {
        authTypeStr = AuthTypes.NOSASL.getAuthName();
      }
    } else {
      if (authTypeStr == null) {
        authTypeStr = AuthTypes.NONE.getAuthName();
      }
      if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
        String principal = conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
        String keytab = conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
        if (needUgiLogin(UserGroupInformation.getCurrentUser(),
          SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keytab)) {
          saslServer = ShimLoader.getHadoopThriftAuthBridge().createServer(principal, keytab);
        } else {
          // Using the default constructor to avoid unnecessary UGI login.
          saslServer = new HadoopThriftAuthBridge.Server();
        }

        // start delegation token manager
        try {
          // rawStore is only necessary for DBTokenStore
          Object rawStore = null;
          String tokenStoreClass = conf.getVar(HiveConf.ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS);

          if (tokenStoreClass.equals(DBTokenStore.class.getName())) {
            HMSHandler baseHandler = new HiveMetaStore.HMSHandler(
                "new db based metaserver", conf, true);
            rawStore = baseHandler.getMS();
          }

          saslServer.startDelegationTokenSecretManager(conf, rawStore, ServerMode.HIVESERVER2);
        }
        catch (MetaException|IOException e) {
          throw new TTransportException("Failed to start token manager", e);
        }
      }
    }
  }

  public Map<String, String> getSaslProperties() {
    Map<String, String> saslProps = new HashMap<String, String>();
    SaslQOP saslQOP = SaslQOP.fromString(conf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    saslProps.put(Sasl.QOP, saslQOP.toString());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }

  public TTransportFactory getAuthTransFactory() throws LoginException {
    TTransportFactory transportFactory;
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      try {
        transportFactory = saslServer.createTransportFactory(getSaslProperties());
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NONE.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.LDAP.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.PAM.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NOSASL.getAuthName())) {
      transportFactory = new TTransportFactory();
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.CUSTOM.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else {
      throw new LoginException("Unsupported authentication type " + authTypeStr);
    }
    return transportFactory;
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   * @param service
   * @return
   * @throws LoginException
   */
  public TProcessorFactory getAuthProcFactory(ThriftCLIService service) throws LoginException {
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      return KerberosSaslHelper.getKerberosProcessorFactory(saslServer, service);
    } else {
      return PlainSaslHelper.getPlainProcessorFactory(service);
    }
  }

  public String getRemoteUser() {
    return saslServer == null ? null : saslServer.getRemoteUser();
  }

  public String getIpAddress() {
    if (saslServer == null || saslServer.getRemoteAddress() == null) {
      return null;
    } else {
      return saslServer.getRemoteAddress().getHostAddress();
    }
  }

  // Perform kerberos login using the hadoop shim API if the configuration is available
  public static void loginFromKeytab(HiveConf hiveConf) throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    if (principal.isEmpty() || keyTabFile.isEmpty()) {
      throw new IOException("HiveServer2 Kerberos principal or keytab is not correctly configured");
    } else {
      UserGroupInformation.loginUserFromKeytab(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile);
    }
  }

  // Perform SPNEGO login using the hadoop shim API if the configuration is available
  public static UserGroupInformation loginFromSpnegoKeytabAndReturnUGI(HiveConf hiveConf)
    throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB);
    if (principal.isEmpty() || keyTabFile.isEmpty()) {
      throw new IOException("HiveServer2 SPNEGO principal or keytab is not correctly configured");
    } else {
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile);
    }
  }

  public static TTransport getSocketTransport(String host, int port, int loginTimeout) {
    return new TSocket(host, port, loginTimeout);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout)
    throws TTransportException {
    return TSSLTransportFactory.getClientSocket(host, port, loginTimeout);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout,
    String trustStorePath, String trustStorePassWord) throws TTransportException {
    TSSLTransportFactory.TSSLTransportParameters params =
      new TSSLTransportFactory.TSSLTransportParameters();
    params.setTrustStore(trustStorePath, trustStorePassWord);
    params.requireClientAuth(true);
    return TSSLTransportFactory.getClientSocket(host, port, loginTimeout, params);
  }

  public static TServerSocket getServerSocket(String hiveHost, int portNum)
    throws TTransportException {
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    return new TServerSocket(serverAddress);
  }

  public static TServerSocket getServerSSLSocket(String hiveHost, int portNum, String keyStorePath,
      String keyStorePassWord, List<String> sslVersionBlacklist) throws TTransportException,
      UnknownHostException {
    TSSLTransportFactory.TSSLTransportParameters params =
        new TSSLTransportFactory.TSSLTransportParameters();
    params.setKeyStore(keyStorePath, keyStorePassWord);
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    TServerSocket thriftServerSocket =
        TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress.getAddress(), params);
    if (thriftServerSocket.getServerSocket() instanceof SSLServerSocket) {
      List<String> sslVersionBlacklistLocal = new ArrayList<String>();
      for (String sslVersion : sslVersionBlacklist) {
        sslVersionBlacklistLocal.add(sslVersion.trim().toLowerCase(Locale.ROOT));
      }
      SSLServerSocket sslServerSocket = (SSLServerSocket) thriftServerSocket.getServerSocket();
      List<String> enabledProtocols = new ArrayList<String>();
      for (String protocol : sslServerSocket.getEnabledProtocols()) {
        if (sslVersionBlacklistLocal.contains(protocol.toLowerCase(Locale.ROOT))) {
          LOG.debug("Disabling SSL Protocol: " + protocol);
        } else {
          enabledProtocols.add(protocol);
        }
      }
      sslServerSocket.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
      LOG.info("SSL Server Socket Enabled Protocols: "
          + Arrays.toString(sslServerSocket.getEnabledProtocols()));
    }
    return thriftServerSocket;
  }

  // retrieve delegation token for the given user
  public String getDelegationToken(String owner, String renewer) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }

    try {
      String tokenStr = saslServer.getDelegationTokenWithService(owner, renewer, HS2_CLIENT_TOKEN);
      if (tokenStr == null || tokenStr.isEmpty()) {
        throw new HiveSQLException(
            "Received empty retrieving delegation token for user " + owner, "08S01");
      }
      return tokenStr;
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error retrieving delegation token for user " + owner, "08S01", e);
    } catch (InterruptedException e) {
      throw new HiveSQLException("delegation token retrieval interrupted", "08S01", e);
    }
  }

  // cancel given delegation token
  public void cancelDelegationToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      saslServer.cancelDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error canceling delegation token " + delegationToken, "08S01", e);
    }
  }

  public void renewDelegationToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      saslServer.renewDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error renewing delegation token " + delegationToken, "08S01", e);
    }
  }

  public String getUserFromToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      return saslServer.getUserFromToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error extracting user from delegation token " + delegationToken, "08S01", e);
    }
  }

  public static void verifyProxyAccess(String realUser, String proxyUser, String ipAddress,
    HiveConf hiveConf) throws HiveSQLException {
    try {
      UserGroupInformation sessionUgi;
      if (UserGroupInformation.isSecurityEnabled()) {
        KerberosNameShim kerbName = ShimLoader.getHadoopShims().getKerberosNameShim(realUser);
        sessionUgi = UserGroupInformation.createProxyUser(
            kerbName.getServiceName(), UserGroupInformation.getLoginUser());
      } else {
        sessionUgi = UserGroupInformation.createRemoteUser(realUser);
      }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration(hiveConf);
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, sessionUgi),
            ipAddress, hiveConf);
      }
    } catch (IOException e) {
      throw new HiveSQLException(
        "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e);
    }
  }

  public static boolean needUgiLogin(UserGroupInformation ugi, String principal, String keytab) {
    return null == ugi || !ugi.hasKerberosCredentials() || !ugi.getUserName().equals(principal) ||
      !Objects.equals(keytab, getKeytabFromUgi());
  }

  private static String getKeytabFromUgi() {
    synchronized (UserGroupInformation.class) {
      try {
        if (keytabFile != null) {
          return (String) keytabFile.get(null);
        } else if (getKeytab != null) {
          return (String) getKeytab.invoke(UserGroupInformation.getCurrentUser());
        } else {
          return null;
        }
      } catch (Exception e) {
        LOG.debug("Fail to get keytabFile path via reflection", e);
        return null;
      }
    }
  }
}
