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

import java.util.List;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;


import junit.framework.TestCase;

public class TestAccessControlList extends TestCase {

  /**
   * test the netgroups (groups in ACL rules that start with @),
   */
  public void testNetgroups() throws Exception {
    // set the config for Groups (test mapping class)
    // we rely on hardcoded groups and netgroups in
    // ShellBasedUnixGroupsMappingTestWrapper
    Configuration conf = new Configuration();
    conf.set("hadoop.security.group.mapping",
      "org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMappingTestWrapper");

    Groups groups = Groups.getUserToGroupsMappingService(conf);

    AccessControlList acl;

    // create these ACLs to populate groups cache
    acl = new AccessControlList("ja my"); // plain
    acl = new AccessControlList("sinatra ratpack,@lasVegas"); // netgroup
    acl = new AccessControlList(" somegroups,@somenetgroup"); // no user

    // check that the netgroups are working
    List<String> elvisGroups = groups.getGroups("elvis");
    assertTrue(elvisGroups.contains("@lasVegas"));

    // refresh cache - not testing this directly but if the results are ok
    // after the refresh that means it worked fine (very likely)
    groups.refresh();

    // create an ACL with netgroups (@xxx)
    acl = new AccessControlList("ja ratpack,@lasVegas");
    // elvis is in @lasVegas
    UserGroupInformation elvis = 
      UserGroupInformation.createRemoteUser("elvis");
    // ja's groups are not in ACL
    UserGroupInformation ja = 
      UserGroupInformation.createRemoteUser("ja");
    // unwanted and unwanted's grops are not in ACL
    UserGroupInformation unwanted = 
      UserGroupInformation.createRemoteUser("unwanted");

    // test the ACLs!
    assertUserAllowed(elvis, acl);
    assertUserAllowed(ja, acl);
    assertUserNotAllowed(unwanted, acl);
  }
  
  public void testWildCardAccessControlList() throws Exception {
    AccessControlList acl;
    
    acl = new AccessControlList("*");
    assertTrue(acl.isAllAllowed());
    
    acl = new AccessControlList("  * ");
    assertTrue(acl.isAllAllowed());
    
    acl = new AccessControlList(" *");
    assertTrue(acl.isAllAllowed());
    
    acl = new AccessControlList("*  ");
    assertTrue(acl.isAllAllowed());
  }

  // check if AccessControlList.toString() works as expected
  public void testToString() {
    AccessControlList acl;

    acl = new AccessControlList("*");
    assertTrue(acl.toString().equals("All users are allowed"));

    acl = new AccessControlList(" ");
    assertTrue(acl.toString().equals("No users are allowed"));

    acl = new AccessControlList("user1,user2");
    assertTrue(acl.toString().equals("Users [user1, user2] are allowed"));

    acl = new AccessControlList("user1,user2 ");// with space
    assertTrue(acl.toString().equals("Users [user1, user2] are allowed"));

    acl = new AccessControlList(" group1,group2");
    assertTrue(acl.toString().equals(
        "Members of the groups [group1, group2] are allowed"));

    acl = new AccessControlList("user1,user2 group1,group2");
    assertTrue(acl.toString().equals(
        "Users [user1, user2] and " +
        "members of the groups [group1, group2] are allowed"));
  }
  
  public void testAccessControlList() throws Exception {
    AccessControlList acl;
    Set<String> users;
    Set<String> groups;
    
    acl = new AccessControlList("drwho tardis");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertEquals(users.iterator().next(), "drwho");
    groups = acl.getGroups();
    assertEquals(groups.size(), 1);
    assertEquals(groups.iterator().next(), "tardis");
    
    acl = new AccessControlList("drwho");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertEquals(users.iterator().next(), "drwho");
    groups = acl.getGroups();
    assertEquals(groups.size(), 0);
    
    acl = new AccessControlList("drwho ");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertEquals(users.iterator().next(), "drwho");
    groups = acl.getGroups();
    assertEquals(groups.size(), 0);
    
    acl = new AccessControlList(" tardis");
    users = acl.getUsers();
    assertEquals(users.size(), 0);
    groups = acl.getGroups();
    assertEquals(groups.size(), 1);
    assertEquals(groups.iterator().next(), "tardis");

    Iterator<String> iter;    
    acl = new AccessControlList("drwho,joe tardis, users");
    users = acl.getUsers();
    assertEquals(users.size(), 2);
    iter = users.iterator();
    assertEquals(iter.next(), "drwho");
    assertEquals(iter.next(), "joe");
    groups = acl.getGroups();
    assertEquals(groups.size(), 2);
    iter = groups.iterator();
    assertEquals(iter.next(), "tardis");
    assertEquals(iter.next(), "users");
  }

  /**
   * Verify the method isUserAllowed()
   */
  public void testIsUserAllowed() {
    AccessControlList acl;

    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@APACHE.ORG",
            new String[] { "aliens", "humanoids", "timelord" });
    UserGroupInformation susan =
        UserGroupInformation.createUserForTesting("susan@APACHE.ORG",
            new String[] { "aliens", "humanoids", "timelord" });
    UserGroupInformation barbara =
        UserGroupInformation.createUserForTesting("barbara@APACHE.ORG",
            new String[] { "humans", "teachers" });
    UserGroupInformation ian =
        UserGroupInformation.createUserForTesting("ian@APACHE.ORG",
            new String[] { "humans", "teachers" });

    acl = new AccessControlList("drwho humanoids");
    assertUserAllowed(drwho, acl);
    assertUserAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList("drwho");
    assertUserAllowed(drwho, acl);
    assertUserNotAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList("drwho ");
    assertUserAllowed(drwho, acl);
    assertUserNotAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList(" humanoids");
    assertUserAllowed(drwho, acl);
    assertUserAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList("drwho,ian aliens,teachers");
    assertUserAllowed(drwho, acl);
    assertUserAllowed(susan, acl);
    assertUserAllowed(barbara, acl);
    assertUserAllowed(ian, acl);
  }

  private void assertUserAllowed(UserGroupInformation ugi,
      AccessControlList acl) {
    assertTrue("User " + ugi + " is not granted the access-control!!",
        acl.isUserAllowed(ugi));
  }

  private void assertUserNotAllowed(UserGroupInformation ugi,
      AccessControlList acl) {
    assertFalse("User " + ugi
        + " is incorrectly granted the access-control!!",
        acl.isUserAllowed(ugi));
  }
}
