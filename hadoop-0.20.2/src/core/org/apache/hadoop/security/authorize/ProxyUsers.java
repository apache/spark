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

package org.apache.hadoop.security.authorize;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

public class ProxyUsers {
  private static final String CONF_HOSTS = ".hosts";
  public static final String CONF_GROUPS = ".groups";
  public static final String CONF_HADOOP_PROXYUSER = "hadoop.proxyuser.";
  public static final String CONF_HADOOP_PROXYUSER_RE = "hadoop\\.proxyuser\\.";
  private static boolean init = false;
  // list of groups and hosts per proxyuser
  private static Map<String, Collection<String>> proxyGroups = 
    new HashMap<String, Collection<String>>();
  private static Map<String, Collection<String>> proxyHosts = 
    new HashMap<String, Collection<String>>();
  
  /**
   * reread the conf and get new values for "hadoop.proxyuser.*.groups/hosts"
   */
  public static void refreshSuperUserGroupsConfiguration() {
    //load server side configuration;
    refreshSuperUserGroupsConfiguration(new Configuration());
  }

  /**
   * refresh configuration
   * @param conf
   */
  public static synchronized void refreshSuperUserGroupsConfiguration(Configuration conf) {
    // remove alle existing stuff
    proxyGroups.clear();
    proxyHosts.clear();

    // get all the new keys for groups
    String regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_GROUPS;
    Map<String,String> allMatchKeys = conf.getValByRegex(regex);
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {
      proxyGroups.put(entry.getKey(), 
          StringUtils.getStringCollection(entry.getValue()));
    }
    
    // now hosts
    regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_HOSTS;
    allMatchKeys = conf.getValByRegex(regex);
    for(Entry<String, String> entry : allMatchKeys.entrySet()) {
      proxyHosts.put(entry.getKey(),
          StringUtils.getStringCollection(entry.getValue()));
    }
    
    init = true;
  }

  /**
   * Returns configuration key for effective user groups allowed for a superuser
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser groups
   */
  public static String getProxySuperuserGroupConfKey(String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_GROUPS;
  }
  
  /**
   * Return configuration key for superuser ip addresses
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser ip-addresses
   */
  public static String getProxySuperuserIpConfKey(String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_HOSTS;
  }
  
  /**
   * Authorize the superuser which is doing doAs
   * 
   * @param user ugi of the effective or proxy user which contains a real user
   * @param remoteAddress the ip address of client
   * @param newConf configuration
   * @throws AuthorizationException
   */
  public static synchronized void authorize(UserGroupInformation user, String remoteAddress,
      Configuration newConf) throws AuthorizationException {
    
    if(!init) {
      refreshSuperUserGroupsConfiguration(); 
    }

    if (user.getRealUser() == null) {
      return;
    }
    boolean groupAuthorized = false;
    boolean ipAuthorized = false;
    UserGroupInformation superUser = user.getRealUser();

    Collection<String> allowedUserGroups = proxyGroups.get(
        getProxySuperuserGroupConfKey(superUser.getShortUserName()));
    
    if (isWildcardList(allowedUserGroups)) {
      groupAuthorized = true;
    } else if (allowedUserGroups != null && !allowedUserGroups.isEmpty()) {
      for (String group : user.getGroupNames()) {
        if (allowedUserGroups.contains(group)) {
          groupAuthorized = true;
          break;
        }
      }
    }
    
    if (!groupAuthorized) {
      throw new AuthorizationException("User: " + superUser.getUserName()
          + " is not allowed to impersonate " + user.getUserName());
    }
    
    Collection<String> ipList = proxyHosts.get(
        getProxySuperuserIpConfKey(superUser.getShortUserName()));
   
    if (isWildcardList(ipList)) {
      ipAuthorized = true;
    } else if (ipList != null && !ipList.isEmpty()) {
      for (String allowedHost : ipList) {
        InetAddress hostAddr;
        try {
          hostAddr = InetAddress.getByName(allowedHost);
        } catch (UnknownHostException e) {
          continue;
        }
        if (hostAddr.getHostAddress().equals(remoteAddress)) {
          // Authorization is successful
          ipAuthorized = true;
        }
      }
    }
    if(!ipAuthorized) {
      throw new AuthorizationException("Unauthorized connection for super-user: "
          + superUser.getUserName() + " from IP " + remoteAddress);
    }
  }

  /**
   * Return true if the configuration specifies the special configuration value
   * "*", indicating that any group or host list is allowed to use this configuration.
   */
  private static boolean isWildcardList(Collection<String> list) {
    return (list != null) &&
      (list.size() == 1) &&
      (list.contains("*"));
  }

}
