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

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;

import junit.framework.TestCase;

public class TestProxyUsers extends TestCase {
  private static final String REAL_USER_NAME = "proxier";
  private static final String PROXY_USER_NAME = "proxied_user";
  private static final String[] GROUP_NAMES =
    new String[] { "foo_group" };
  private static final String[] OTHER_GROUP_NAMES =
    new String[] { "bar_group" };
  private static final String PROXY_IP = "1.2.3.4";
  
  public void testProxyUsers() throws Exception {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      StringUtils.join(",", Arrays.asList(GROUP_NAMES)));
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      PROXY_IP);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);


    // First try proxying a group that's allowed
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, GROUP_NAMES);

    // From good IP
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");

    // Now try proxying a group that's not allowed
    realUserUgi = UserGroupInformation.createRemoteUser(REAL_USER_NAME);
    proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, OTHER_GROUP_NAMES);
    
    // From good IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");
  }

  public void testWildcardGroup() {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      "*");
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      PROXY_IP);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    // First try proxying a group that's allowed
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, GROUP_NAMES);

    // From good IP
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");

    // Now try proxying a different group (just to make sure we aren't getting spill over
    // from the other test case!)
    realUserUgi = UserGroupInformation.createRemoteUser(REAL_USER_NAME);
    proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, OTHER_GROUP_NAMES);
    
    // From good IP
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");
  }

  public void testWildcardIP() {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      StringUtils.join(",", Arrays.asList(GROUP_NAMES)));
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      "*");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    // First try proxying a group that's allowed
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, GROUP_NAMES);

    // From either IP should be fine
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    assertAuthorized(proxyUserUgi, "1.2.3.5");

    // Now set up an unallowed group
    realUserUgi = UserGroupInformation.createRemoteUser(REAL_USER_NAME);
    proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, OTHER_GROUP_NAMES);
    
    // Neither IP should be OK
    assertNotAuthorized(proxyUserUgi, "1.2.3.4");
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");
  }

  private void assertNotAuthorized(UserGroupInformation proxyUgi, String host) {
    try {
      ProxyUsers.authorize(proxyUgi, host, null);
      fail("Allowed authorization of " + proxyUgi + " from " + host);
    } catch (AuthorizationException e) {
      // Expected
    }
  }
  
  private void assertAuthorized(UserGroupInformation proxyUgi, String host) {
    try {
      ProxyUsers.authorize(proxyUgi, host, null);
    } catch (AuthorizationException e) {
      fail("Did not allowed authorization of " + proxyUgi + " from " + host);
    }
  }
}
