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

import java.util.Iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.conf.Configuration;

/**
 * Class representing a configured access control list.
 */
public class AccessControlList implements Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (AccessControlList.class,
       new WritableFactory() {
         public Writable newInstance() { return new AccessControlList(); }
       });
  }

  // Indicates an ACL string that represents access to all users
  public static final String WILDCARD_ACL_VALUE = "*";
  private static final int INITIAL_CAPACITY = 256;

  // Set of users who are granted access.
  private Set<String> users;
  // Set of groups which are granted access
  private Set<String> groups;
  // Whether all users are granted access.
  private boolean allAllowed;

  /**
   * This constructor exists primarily for AccessControlList to be Writable.
   */
  public AccessControlList() {
  }

  /**
   * Construct a new ACL from a String representation of the same.
   * 
   * The String is a a comma separated list of users and groups.
   * The user list comes first and is separated by a space followed 
   * by the group list. For e.g. "user1,user2 group1,group2"
   * 
   * @param aclString String representation of the ACL
   */
  public AccessControlList(String aclString) {
    buildACL(aclString);
  }

  // build ACL from the given string
  private void buildACL(String aclString) {
    users = new TreeSet<String>();
    groups = new TreeSet<String>();
    if (aclString.contains(WILDCARD_ACL_VALUE) && 
        aclString.trim().equals(WILDCARD_ACL_VALUE)) {
      allAllowed = true;
    } else {
      allAllowed = false;
      String[] userGroupStrings = aclString.split(" ", 2);
      Configuration conf = new Configuration();
      Groups groupsMapping = Groups.getUserToGroupsMappingService(conf);

      if (userGroupStrings.length >= 1) {
        List<String> usersList = new LinkedList<String>(
          Arrays.asList(userGroupStrings[0].split(",")));
        cleanupList(usersList);
        addToSet(users, usersList);
      }
      
      if (userGroupStrings.length == 2) {
        List<String> groupsList = new LinkedList<String>(
          Arrays.asList(userGroupStrings[1].split(",")));
        cleanupList(groupsList);
        addToSet(groups, groupsList);
        groupsMapping.cacheGroupsAdd(groupsList);
      }
    }
  }

  public boolean isAllAllowed() {
    return allAllowed;
  }
  
  public void addUser(String user) {
    users.add(user);
  }

  /**
   * Get the names of users allowed for this service.
   * @return the set of user names. the set must not be modified.
   */
  Set<String> getUsers() {
    return users;
  }
  
  /**
   * Get the names of user groups allowed for this service.
   * @return the set of group names. the set must not be modified.
   */
  Set<String> getGroups() {
    return groups;
  }

  public boolean isUserAllowed(UserGroupInformation ugi) {
    if (allAllowed || users.contains(ugi.getShortUserName())) {
      return true;
    } else {
      for(String group: ugi.getGroupNames()) {
        if (groups.contains(group)) {
          return true;
        }
      }
    }
    return false;
  }

  private static final void cleanupList(List<String> list) {
    ListIterator<String> i = list.listIterator();
    while(i.hasNext()) {
      String s = i.next();
      if(s.length() == 0) {
        i.remove();
      } else {
        s = s.trim();
        i.set(s);
      }
    }
  }
  
  private static final void addToSet(Set<String> set, List<String> list) {
    for(String s : list) {
      set.add(s);
    }
  }
  
  @Override
  public String toString() {
    String str = null;

    if (allAllowed) {
      str = "All users are allowed";
    }
    else if (users.isEmpty() && groups.isEmpty()) {
      str = "No users are allowed";
    }
    else {
      String usersStr = null;
      String groupsStr = null;
      if (!users.isEmpty()) {
        usersStr = users.toString();
      }
      if (!groups.isEmpty()) {
        groupsStr = groups.toString();
      }

      if (!users.isEmpty() && !groups.isEmpty()) {
        str = "Users " + usersStr + " and members of the groups "
            + groupsStr + " are allowed";
      }
      else if (!users.isEmpty()) {
        str = "Users " + usersStr + " are allowed";
      }
      else {// users is empty array and groups is nonempty
        str = "Members of the groups "
            + groupsStr + " are allowed";
      }
    }

    return str;
  }

  // Serializes the AccessControlList object
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getACLString());
  }

  // Deserialize
  public void readFields(DataInput in) throws IOException {
    String aclString = Text.readString(in);
    buildACL(aclString);
  }

  /** Returns the String representation of this ACL. Unlike toString() method's
   *  return value, this String can be directly given to the constructor of
   *  AccessControlList to build AccessControlList object.
   *  This is the method used by the serialization method write().
   */
  public String getACLString() {
    StringBuilder sb = new StringBuilder(INITIAL_CAPACITY);
    if (allAllowed) {
      sb.append('*');
    }
    else {
      sb.append(getUsersString());
      sb.append(" ");
      sb.append(getGroupsString());
    }
    return sb.toString();
  }

  // Returns comma-separated concatenated single String of the set 'users'
  private String getUsersString() {
    return getString(users);
  }

  // Returns comma-separated concatenated single String of the set 'groups'
  private String getGroupsString() {
    return getString(groups);
  }

  // Returns comma-separated concatenated single String of all strings of
  // the given set
  private String getString(Set<String> strings) {
    StringBuilder sb = new StringBuilder(INITIAL_CAPACITY);
    boolean first = true;
    for(String str: strings) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      sb.append(str);
    }
    return sb.toString();
  }
}
