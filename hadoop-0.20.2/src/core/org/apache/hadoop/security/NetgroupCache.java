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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Class that caches the netgroups and inverts group-to-user map
 * to user-to-group map
 */
public class NetgroupCache {

  private static final Log LOG = LogFactory.getLog(NetgroupCache.class);

  private static boolean netgroupToUsersMapUpdated = true;
  private static Map<String, Set<String>> netgroupToUsersMap =
    new ConcurrentHashMap<String, Set<String>>();

  private static Map<String, Set<String>> userToNetgroupsMap =
    new ConcurrentHashMap<String, Set<String>>();


  public void getNetgroups(final String user,
      List<String> groups) {
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

  public List<String> getNetgroupNames() {
    return new LinkedList<String>(netgroupToUsersMap.keySet());
  }

  public boolean isCached(String group) {
    return netgroupToUsersMap.containsKey(group);
  }

  public void clear() {
    netgroupToUsersMap.clear();
  }

  public void add(String group, List<String> users) {
    if(!isCached(group)) {
      netgroupToUsersMap.put(group, new HashSet<String>());
      for(String user: users) {
        netgroupToUsersMap.get(group).add(user);
      }
    }
    netgroupToUsersMapUpdated = true; // at the end to avoid race
  }
}
