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
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.hadoop.security.NetgroupCache;

/**
 * A JNI-based implementation of {@link GroupMappingServiceProvider} 
 * that invokes libC calls to get the group
 * memberships of a given user.
 */
public class JniBasedUnixGroupsNetgroupMapping
  extends JniBasedUnixGroupsMapping {
  
  private static final Log LOG = LogFactory.getLog(
    JniBasedUnixGroupsNetgroupMapping.class);

  private static final NetgroupCache netgroupCache = new NetgroupCache();

  native String[] getUsersForNetgroupJNI(String group);
  
  /**
   * Gets unix groups and netgroups for the user.
   *
   * It gets all unix groups as returned by id -Gn but it
   * only returns netgroups that are used in ACLs (there is
   * no way to get all netgroups for a given user, see
   * documentation for getent netgroup)
   */
  @Override
  public List<String> getGroups(String user) throws IOException {
    // parent gets unix groups
    List<String> groups = new LinkedList<String>(super.getGroups(user));
    netgroupCache.getNetgroups(user, groups);
    return groups;
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    List<String> groups = netgroupCache.getNetgroupNames();
    netgroupCache.clear();
    cacheGroupsAdd(groups);
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    for(String group: groups) {
      if(group.length() == 0) {
        // better safe than sorry (should never happen)
      } else if(group.charAt(0) == '@') {
        if(!netgroupCache.isCached(group)) {
          netgroupCache.add(group, getUsersForNetgroup(group));
        }
      } else {
        // unix group, not caching
      }
    }
  }

  /**
   * Calls JNI function to get users for a netgroup, since C functions
   * are not reentrant we need to make this synchronized (see
   * documentation for setnetgrent, getnetgrent and endnetgrent)
   */
  protected synchronized List<String> getUsersForNetgroup(String netgroup) {
    String[] users = null;
    try {
      // JNI code does not expect '@' at the begining of the group name
      users = getUsersForNetgroupJNI(netgroup.substring(1));
    } catch (Exception e) {
      LOG.warn("Got exception while trying to obtain the users for netgroup ["
        + netgroup + "] [" + e + "]");
    }
    if (users != null && users.length != 0) {
      return Arrays.asList(users);
    }
    return new LinkedList<String>();
  }
}
