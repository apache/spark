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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A factory for a {@link Filter} based on a list of allowed users.
 * <br>
 * The produced filter object filters out all users that are not on the provided in
 * Hive configuration list.
 * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERFILTER
 */
public final class UserFilterFactory implements FilterFactory {

  /**
   * {@inheritDoc}
   */
  @Override
  public Filter getInstance(HiveConf conf) {
    Collection<String> userFilter = conf.getStringCollection(
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERFILTER.varname);

    if (userFilter.isEmpty()) {
      return null;
    }

    return new UserFilter(userFilter);
  }

  private static final class UserFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(UserFilter.class);

    private final Set<String> userFilter = new HashSet<>();

    UserFilter(Collection<String> userFilter) {
      for (String userFilterItem : userFilter) {
        this.userFilter.add(userFilterItem.toLowerCase());
      }
    }

    @Override
    public void apply(DirSearch ldap, String user) throws AuthenticationException {
      LOG.info("Authenticating user '{}' using user filter", user);
      String userName = LdapUtils.extractUserName(user).toLowerCase();
      if (!userFilter.contains(userName)) {
        LOG.info("Authentication failed based on user membership");
        throw new AuthenticationException("Authentication failed: "
            + "User not a member of specified list");
      }
    }
  }
}
