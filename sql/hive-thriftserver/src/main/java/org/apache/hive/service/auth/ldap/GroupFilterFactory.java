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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import java.util.*;

/**
 * A factory for a {@link Filter} based on a list of allowed groups.
 * <br>
 * The produced filter object filters out all users that are not members of at least one of
 * the groups provided in Hive configuration.
 * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER
 */
public final class GroupFilterFactory implements FilterFactory {

  /**
   * {@inheritDoc}
   */
  @Override
  public Filter getInstance(HiveConf conf) {
    Collection<String> groupFilter = conf.getStringCollection(
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER.varname);

    if (groupFilter.isEmpty()) {
      return null;
    }

    if (conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERMEMBERSHIP_KEY) == null) {
      return new GroupMembershipKeyFilter(groupFilter);
    } else {
      return new UserMembershipKeyFilter(groupFilter);
    }
  }

  @VisibleForTesting
  static final class GroupMembershipKeyFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(GroupMembershipKeyFilter.class);

    private final Set<String> groupFilter = new HashSet<>();

    GroupMembershipKeyFilter(Collection<String> groupFilter) {
      this.groupFilter.addAll(groupFilter);
    }

    @Override
    public void apply(DirSearch ldap, String user) throws AuthenticationException {
      LOG.info("Authenticating user '{}' using {}", user,
          GroupMembershipKeyFilter.class.getSimpleName());

      List<String> memberOf = null;

      try {
        String userDn = ldap.findUserDn(user);
        memberOf = ldap.findGroupsForUser(userDn);
        LOG.debug("User {} member of : {}", userDn, memberOf);
      } catch (NamingException e) {
        throw new AuthenticationException("LDAP Authentication failed for user", e);
      }

      for (String groupDn : memberOf) {
        String shortName = LdapUtils.getShortName(groupDn);
        if (groupFilter.contains(shortName)) {
          LOG.debug("GroupMembershipKeyFilter passes: user '{}' is a member of '{}' group",
              user, groupDn);
          LOG.info("Authentication succeeded based on group membership");
          return;
        }
      }
      LOG.info("Authentication failed based on user membership");
      throw new AuthenticationException("Authentication failed: "
          + "User not a member of specified list");
    }
  }

  @VisibleForTesting
  static final class UserMembershipKeyFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(UserMembershipKeyFilter.class);

    private final Collection<String> groupFilter;

    UserMembershipKeyFilter(Collection<String> groupFilter) {
      this.groupFilter = groupFilter;
    }

    @Override
    public void apply(DirSearch ldap, String user) throws AuthenticationException {
      LOG.info("Authenticating user '{}' using {}", user,
          UserMembershipKeyFilter.class.getSimpleName());

      List<String> groupDns = new ArrayList<>();
      for (String groupId : groupFilter) {
        try {
          String groupDn = ldap.findGroupDn(groupId);
          groupDns.add(groupDn);
        } catch (NamingException e) {
          LOG.warn("Cannot find DN for group", e);
          LOG.debug("Cannot find DN for group " + groupId, e);
        }
      }

      if (groupDns.isEmpty()) {
        String msg = String.format("No DN(s) has been found for any of group(s): %s",
            Joiner.on(',').join(groupFilter));
        LOG.debug(msg);
        throw new AuthenticationException("No DN(s) has been found for any of specified group(s)");
      }

      for (String groupDn : groupDns) {
        try {
          if (ldap.isUserMemberOfGroup(user, groupDn)) {
            LOG.debug("UserMembershipKeyFilter passes: user '{}' is a member of '{}' group",
                user, groupDn);
            LOG.info("Authentication succeeded based on user membership");
            return;
          }
        } catch (NamingException e) {
          LOG.warn("Cannot match user and group", e);
          if (LOG.isDebugEnabled()) {
            String msg = String.format("Cannot match user '%s' and group '%s'", user, groupDn);
            LOG.debug(msg, e);
          }
        }
      }
      throw new AuthenticationException(String.format(
          "Authentication failed: User '%s' is not a member of listed groups", user));
    }
  }
}
