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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;

/**
 * A simple shell-based implementation of {@link GroupMappingServiceProvider} 
 * that exec's the <code>groups</code> shell command to fetch the group
 * memberships of a given user.
 */
public class ShellBasedUnixGroupsNetgroupMapping extends ShellBasedUnixGroupsMapping {
  
  private static final Log LOG = LogFactory.getLog(ShellBasedUnixGroupsNetgroupMapping.class);

  protected static boolean netgroupToUsersMapUpdated = true;
  protected static Map<String, Set<String>> netgroupToUsersMap =
    new ConcurrentHashMap<String, Set<String>>();

  protected static Map<String, Set<String>> userToNetgroupsMap =
    new ConcurrentHashMap<String, Set<String>>();
  
  @Override
  public List<String> getGroups(String user) throws IOException {
    List<String> groups = new LinkedList<String>();
    getUnixGroups(user, groups);
    getNetgroups(user, groups);
    return groups;
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    List<String> groups = new LinkedList<String>(netgroupToUsersMap.keySet());
    netgroupToUsersMap.clear();
    cacheGroupsAdd(groups);
    netgroupToUsersMapUpdated = true; // at the end to avoid race
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    for(String group: groups) {
      if(group.length() == 0) {
        // better safe than sorry (should never happen)
      } else if(group.charAt(0) == '@') {
        cacheNetgroup(group);
      } else {
        // unix group, not caching
      }
    }
  }

  protected void cacheNetgroup(String group) throws IOException {
    if(netgroupToUsersMap.containsKey(group)) {
      return;
    } else {
      // returns a string similar to this:
      // group               ( , user, ) ( domain, user1, host.com )
      String usersRaw = execShellGetUserForNetgroup(group);
      // get rid of spaces, makes splitting much easier
      usersRaw = usersRaw.replaceAll(" +", "");
      // remove netgroup name at the beginning of the string
      usersRaw = usersRaw.replaceFirst(
        group.replaceFirst("@", "") + "[()]+",
        "");
      // split string into user infos
      String[] userInfos = usersRaw.split("[()]+");
      for(String userInfo : userInfos) {
        // userInfo: xxx,user,yyy (xxx, yyy can be empty strings)
        // get rid of everything before first and after last comma
        String user = userInfo.replaceFirst("[^,]*,", "");
        user = user.replaceFirst(",.*$", "");
        // voila! got username!
        if(!netgroupToUsersMap.containsKey(group)) {
          netgroupToUsersMap.put(group, new HashSet<String>());
        }
        netgroupToUsersMap.get(group).add(user);
      }
      netgroupToUsersMapUpdated = true; // at the end to avoid race
    }
  }

  /** 
   * Get the current user's group list from Unix by running the command 'groups'
   * NOTE. For non-existing user it will return EMPTY list
   * @param user user name
   * @return the groups list that the <code>user</code> belongs to
   * @throws IOException if encounter any error when running the command
   */
  private void getUnixGroups(final String user,
      List<String> groups) throws IOException {
    String result = execShellGetUnixGroups(user);

    StringTokenizer tokenizer = new StringTokenizer(result);
    while (tokenizer.hasMoreTokens()) {
      groups.add(tokenizer.nextToken());
    }
  }

  protected void getNetgroups(final String user,
      List<String> groups) throws IOException {
    if(netgroupToUsersMapUpdated) {
      netgroupToUsersMapUpdated = false; // at the beginning to avoid race
      //update userToNetgroupsMap
      for(String netgroup : netgroupToUsersMap.keySet()) {
        for(String netuser : netgroupToUsersMap.get(netgroup)) {
          // add to userToNetgroupsMap
          if(!userToNetgroupsMap.containsKey(netuser)) {
            userToNetgroupsMap.put(netuser, new HashSet<String>());
          }
          userToNetgroupsMap.get(netuser).add(netgroup);
        }
      }
    }
    if(userToNetgroupsMap.containsKey(user)) {
      for(String netgroup : userToNetgroupsMap.get(user)) {
        groups.add(netgroup);
      }
    }
  }

  protected String execShellGetUnixGroups(final String user)
      throws IOException {
    String result = "";
    try {
      result = Shell.execCommand(Shell.getGroupsForUserCommand(user));
    } catch (ExitCodeException e) {
      // if we didn't get the group - just return empty list;
      LOG.warn("error while getting groups for user " + user, e);
    }
    return result;
  }

  protected String execShellGetUserForNetgroup(final String netgroup)
      throws IOException {
    String result = "";
    try {
      // shell command does not expect '@' at the begining of the group name
      result = Shell.execCommand(
        Shell.getUsersForNetgroupCommand(netgroup.substring(1)));
    } catch (ExitCodeException e) {
      // if we didn't get the group - just return empty list;
      LOG.warn("error while getting users for netgroup " + netgroup, e);
    }
    return result;
  }
}
