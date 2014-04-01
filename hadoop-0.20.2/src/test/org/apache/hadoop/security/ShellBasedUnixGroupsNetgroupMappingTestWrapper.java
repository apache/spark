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

/**
 * A wrapper for ShellBasedUnixGroupsMapping that replaces functions
 * that call external programs (to get users/groups) by static stubs
 * for the purposes of testing
 *
 * Keep in sync with TestAccessControlList.java
 */
public class ShellBasedUnixGroupsNetgroupMappingTestWrapper
    extends ShellBasedUnixGroupsNetgroupMapping {
  
  @Override
  protected String execShellGetUnixGroups(final String user)
      throws IOException {
    if(user.equals("ja")) {
      return "my";
    } else if(user.equals("sinatra")) {
      return "ratpack";
    } else if(user.equals("elvis")) {
      return "users otherGroup";
    }
    return "";
  }

  @Override
  protected String execShellGetUserForNetgroup(final String netgroup)
      throws IOException {
    if(netgroup.equals("@lasVegas")) {
      return "lasVegas               ( , sinatra, ) ( domain, elvis, host.com)";
    } else if(netgroup.equals("@somenetgroup")) {
      return "somenetgroup           ( , nobody, )";
    } else {
      return "";
    }
  }  

}
