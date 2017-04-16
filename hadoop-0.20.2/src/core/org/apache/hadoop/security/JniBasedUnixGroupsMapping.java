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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A JNI-based implementation of {@link GroupMappingServiceProvider} 
 * that invokes libC calls to get the group
 * memberships of a given user.
 */
public class JniBasedUnixGroupsMapping implements GroupMappingServiceProvider {
  
  private static final Log LOG = LogFactory.getLog(
    JniBasedUnixGroupsMapping.class);
  
  native String[] getGroupForUser(String user);
  
  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      LOG.info("Bailing out since native library couldn't be loaded");
      throw new RuntimeException();
    }
    LOG.info("Using JniBasedUnixGroupsMapping for Group resolution");
  }

  @Override
  public List<String> getGroups(String user) throws IOException {
    String[] groups = null;
    try {
      groups = getGroupForUser(user);
    } catch (Exception e) {
      LOG.warn("Got exception while trying to obtain the groups for user " + user);
    }
    if (groups != null && groups.length != 0) {
      return Arrays.asList(groups);
    }
    return Arrays.asList(new String[0]);
  }
  @Override
  public void cacheGroupsRefresh() throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }
}
