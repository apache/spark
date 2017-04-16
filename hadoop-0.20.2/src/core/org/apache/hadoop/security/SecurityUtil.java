/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;

import sun.security.jgss.krb5.Krb5Util;
import sun.security.krb5.Credentials;
import sun.security.krb5.PrincipalName;

public class SecurityUtil {
  public static final Log LOG = LogFactory.getLog(SecurityUtil.class);
  public static final String HOSTNAME_PATTERN = "_HOST";

  /**
   * Find the original TGT within the current subject's credentials. Cross-realm
   * TGT's of the form "krbtgt/TWO.COM@ONE.COM" may be present.
   * 
   * @return The TGT from the current subject
   * @throws IOException
   *           if TGT can't be found
   */
  private static KerberosTicket getTgtFromSubject() throws IOException {
    Subject current = UserGroupInformation.getCurrentUser().getSubject();
    if (current == null) {
      throw new IOException(
          "Can't get TGT from current Subject, because it is null");
    }
    Set<KerberosTicket> tickets = current
        .getPrivateCredentials(KerberosTicket.class);
    for (KerberosTicket t : tickets) {
      if (isOriginalTGT(t.getServer().getName()))
        return t;
    }
    throw new IOException("Failed to find TGT from current Subject:"+current);
  }
  
  // Original TGT must be of form "krbtgt/FOO@FOO". Verify this
  protected static boolean isOriginalTGT(String name) {
    if(name == null) return false;
    
    String [] components = name.split("[/@]");

    return components.length == 3 &&
           "krbtgt".equals(components[0]) &&
           components[1].equals(components[2]);
  }

  /**
   * Explicitly pull the service ticket for the specified host.  This solves a
   * problem with Java's Kerberos SSL problem where the client cannot 
   * authenticate against a cross-realm service.  It is necessary for clients
   * making kerberized https requests to call this method on the target URL
   * to ensure that in a cross-realm environment the remote host will be 
   * successfully authenticated.  
   * 
   * This method is internal to Hadoop and should not be used by other 
   * applications.  This method should not be considered stable or open: 
   * it will be removed when the Java behavior is changed.
   * 
   * @param remoteHost Target URL the krb-https client will access
   */
  public static void fetchServiceTicket(URL remoteHost) throws IOException {
    if(!UserGroupInformation.isSecurityEnabled())
      return;
    
    String serviceName = "host/" + remoteHost.getHost();
    if (LOG.isDebugEnabled())
      LOG.debug("Fetching service ticket for host at: " + serviceName);
    Credentials serviceCred = null;
    try {
      PrincipalName principal = new PrincipalName(serviceName,
          PrincipalName.KRB_NT_SRV_HST);
      serviceCred = Credentials.acquireServiceCreds(principal
          .toString(), Krb5Util.ticketToCreds(getTgtFromSubject()));
    } catch (Exception e) {
      throw new IOException("Can't get service ticket for: "
          + serviceName, e);
    }
    if (serviceCred == null) {
      throw new IOException("Can't get service ticket for " + serviceName);
    }
    UserGroupInformation.getCurrentUser().getSubject().getPrivateCredentials()
        .add(Krb5Util.credsToTicket(serviceCred));
  }
  
  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal
   * names. It replaces hostname pattern with hostname, which should be
   * fully-qualified domain name. If hostname is null or "0.0.0.0", it uses
   * dynamically looked-up fqdn of the current host instead.
   * 
   * @param principalConfig
   *          the Kerberos principal name conf value to convert
   * @param hostname
   *          the fully-qualified domain name used for substitution
   * @return converted Kerberos principal name
   * @throws IOException
   */
  public static String getServerPrincipal(String principalConfig,
      String hostname) throws IOException {
    String[] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      return replacePattern(components, hostname);
    }
  }
  
  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal names.
   * This method is similar to {@link #getServerPrincipal(String, String)},
   * except 1) the reverse DNS lookup from addr to hostname is done only when
   * necessary, 2) param addr can't be null (no default behavior of using local
   * hostname when addr is null).
   * 
   * @param principalConfig
   *          Kerberos principal name pattern to convert
   * @param addr
   *          InetAddress of the host used for substitution
   * @return converted Kerberos principal name
   * @throws IOException
   */
  public static String getServerPrincipal(String principalConfig,
      InetAddress addr) throws IOException {
    String[] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      if (addr == null) {
        throw new IOException("Can't replace " + HOSTNAME_PATTERN
            + " pattern since client address is null");
      }
      return replacePattern(components, addr.getCanonicalHostName());
    }
  }
  
  private static String[] getComponents(String principalConfig) {
    if (principalConfig == null)
      return null;
    return principalConfig.split("[/@]");
  }
  
  private static String replacePattern(String[] components, String hostname)
      throws IOException {
    String fqdn = hostname;
    if (fqdn == null || fqdn.equals("") || fqdn.equals("0.0.0.0")) {
      fqdn = getLocalHostName();
    }
    return components[0] + "/" + fqdn + "@" + components[2];
  }
  
  static String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  /**
   * If a keytab has been provided, login as that user. Substitute $host in
   * user's Kerberos principal name with a dynamically looked-up fully-qualified
   * domain name of the current host.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @throws IOException
   */
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey) throws IOException {
    login(conf, keytabFileKey, userNameKey, getLocalHostName());
  }

  /**
   * If a keytab has been provided, login as that user. Substitute $host in
   * user's Kerberos principal name with hostname.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @param hostname
   *          hostname to use for substitution
   * @throws IOException
   */
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey, String hostname)
      throws IOException {
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null) {
      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.warn("No keytab file '" + keytabFileKey + "' configured.");
      }
      return;
    }

    String principalConfig = conf.get(userNameKey, System
        .getProperty("user.name"));
    String principalName = SecurityUtil.getServerPrincipal(principalConfig,
        hostname);
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename);
  }
  
  /**
   * create service name for Delegation token ip:port
   * @param uri
   * @return "ip:port"
   */
  public static String buildDTServiceName(URI uri, int defPort) {
    int port = uri.getPort();
    if(port == -1) 
      port = defPort;
    
    // build the service name string "/ip:port"
    // for whatever reason using NetUtils.createSocketAddr(target).toString()
    // returns "localhost/ip:port"
    StringBuffer sb = new StringBuffer();
    String host = uri.getHost();
    if (host != null) {
      host = NetUtils.normalizeHostName(host);
    } else {
      host = "";
    }
    sb.append(host).append(":").append(port);
    return sb.toString();
  }
  
  /**
   * Get the ACL object representing the cluster administrators
   * The user who starts the daemon is automatically added as an admin
   * @param conf
   * @param configKey the key that holds the ACL string in its value
   * @return AccessControlList instance
   */
  public static AccessControlList getAdminAcls(Configuration conf, 
      String configKey) {
    try {
      AccessControlList adminAcl = 
        new AccessControlList(conf.get(configKey, " "));
      adminAcl.addUser(UserGroupInformation.getCurrentUser().
                       getShortUserName());
      return adminAcl;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
