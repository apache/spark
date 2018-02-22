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
package org.apache.hive.service.auth.ldap;

import org.apache.hadoop.hive.conf.HiveConf;

import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import java.util.Collection;

/**
 * A factory for a {@link Filter} that check whether provided user could be found in the directory.
 * <br>
 * The produced filter object filters out all users that are not found in the directory.
 */
public final class UserSearchFilterFactory implements FilterFactory {

  /**
   * {@inheritDoc}
   */
  @Override
  public Filter getInstance(HiveConf conf) {
    Collection<String> groupFilter = conf.getStringCollection(
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER.varname);
    Collection<String> userFilter = conf.getStringCollection(
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERFILTER.varname);

    if (groupFilter.isEmpty() && userFilter.isEmpty()) {
      return null;
    }

    return new UserSearchFilter();
  }

  private static final class UserSearchFilter implements Filter {
    @Override
    public void apply(DirSearch client, String user) throws AuthenticationException {
      try {
        String userDn = client.findUserDn(user);

        // This should not be null because we were allowed to bind with this username
        // safe check in case we were able to bind anonymously.
        if (userDn == null) {
          throw new AuthenticationException("Authentication failed: User search failed");
        }
      } catch (NamingException e) {
        throw new AuthenticationException("LDAP Authentication failed for user", e);
      }
    }
  }
}
