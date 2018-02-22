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
package org.apache.hive.service.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.ServiceUtils;
import org.apache.hive.service.auth.ldap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.util.Iterator;
import java.util.List;

public class LdapAuthenticationProviderImpl implements PasswdAuthenticationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(LdapAuthenticationProviderImpl.class);

  private static final List<FilterFactory> FILTER_FACTORIES = ImmutableList.<FilterFactory>of(
      new CustomQueryFilterFactory(),
      new ChainFilterFactory(new UserSearchFilterFactory(), new UserFilterFactory(),
          new GroupFilterFactory())
  );

  private final HiveConf conf;
  private final Filter filter;
  private final DirSearchFactory searchFactory;

  public LdapAuthenticationProviderImpl(HiveConf conf) {
    this(conf, new LdapSearchFactory());
  }

  @VisibleForTesting
  LdapAuthenticationProviderImpl(HiveConf conf, DirSearchFactory searchFactory) {
    this.conf = conf;
    this.searchFactory = searchFactory;
    filter = resolveFilter(conf);
  }

  @Override
  public void Authenticate(String user, String password) throws AuthenticationException {
    DirSearch search = null;
    try {
      search = createDirSearch(user, password);
      applyFilter(search, user);
    } finally {
      ServiceUtils.cleanup(LOG, search);
    }
  }

  private DirSearch createDirSearch(String user, String password) throws AuthenticationException {
    if (StringUtils.isBlank(user)) {
      throw new AuthenticationException("Error validating LDAP user:"
          + " a null or blank user name has been provided");
    }
    if (StringUtils.isBlank(password) || password.getBytes()[0] == 0) {
      throw new AuthenticationException("Error validating LDAP user:"
          + " a null or blank password has been provided");
    }
    List<String> principals = LdapUtils.createCandidatePrincipals(conf, user);
    for (Iterator<String> iterator = principals.iterator(); iterator.hasNext();) {
      String principal = iterator.next();
      try {
        return searchFactory.getInstance(conf, principal, password);
      } catch (AuthenticationException ex) {
        if (!iterator.hasNext()) {
          throw ex;
        }
      }
    }
    throw new AuthenticationException(
        String.format("No candidate principals for %s was found.", user));
  }

  private static Filter resolveFilter(HiveConf conf) {
    for (FilterFactory filterProvider : FILTER_FACTORIES) {
      Filter filter = filterProvider.getInstance(conf);
      if (filter != null) {
        return filter;
      }
    }
    return null;
  }

  private void applyFilter(DirSearch client, String user) throws AuthenticationException {
    if (filter != null) {
      if (LdapUtils.hasDomain(user)) {
        filter.apply(client, LdapUtils.extractUserName(user));
      } else {
        filter.apply(client, user);
      }
    }
  }
}
