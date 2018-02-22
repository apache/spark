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

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implements search for LDAP.
 */
public final class LdapSearch implements DirSearch {

  private static final Logger LOG = LoggerFactory.getLogger(LdapSearch.class);

  private final String baseDn;
  private final List<String> groupBases;
  private final List<String> userBases;
  private final List<String> userPatterns;

  private final QueryFactory queries;

  private final DirContext ctx;

  /**
   * Construct an instance of {@code LdapSearch}.
   * @param conf Hive configuration
   * @param ctx Directory service that will be used for the queries.
   * @throws NamingException
   */
  public LdapSearch(HiveConf conf, DirContext ctx) throws NamingException {
    baseDn = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN);
    userPatterns = LdapUtils.parseDnPatterns(conf,
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN);
    groupBases = LdapUtils.patternsToBaseDns(LdapUtils.parseDnPatterns(conf,
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN));
    userBases = LdapUtils.patternsToBaseDns(userPatterns);
    this.ctx = ctx;
    queries = new QueryFactory(conf);
  }

  /**
   * Closes this search object and releases any system resources associated
   * with it. If the search object is already closed then invoking this
   * method has no effect.
   */
  @Override
  public void close() {
    try {
      ctx.close();
    } catch (NamingException e) {
      LOG.warn("Exception when closing LDAP context:", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String findUserDn(String user) throws NamingException {
    List<String> allLdapNames;
    if (LdapUtils.isDn(user)) {
      String userBaseDn = LdapUtils.extractBaseDn(user);
      String userRdn = LdapUtils.extractFirstRdn(user);
      allLdapNames = execute(Collections.singletonList(userBaseDn),
          queries.findUserDnByRdn(userRdn)).getAllLdapNames();
    } else {
      allLdapNames = findDnByPattern(userPatterns, user);
      if (allLdapNames.isEmpty()) {
        allLdapNames = execute(userBases, queries.findUserDnByName(user)).getAllLdapNames();
      }
    }

    if (allLdapNames.size() == 1) {
      return allLdapNames.get(0);
    } else {
      LOG.info("Expected exactly one user result for the user: {}, but got {}. Returning null",
          user, allLdapNames.size());
      LOG.debug("Matched users: {}", allLdapNames);
      return null;
    }
  }

  private List<String> findDnByPattern(List<String> patterns, String name) throws NamingException {
    for (String pattern : patterns) {
      String baseDnFromPattern = LdapUtils.extractBaseDn(pattern);
      String rdn = LdapUtils.extractFirstRdn(pattern).replaceAll("%s", name);
      List<String> list = execute(Collections.singletonList(baseDnFromPattern),
          queries.findDnByPattern(rdn)).getAllLdapNames();
      if (!list.isEmpty()) {
        return list;
      }
    }
    return Collections.emptyList();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String findGroupDn(String group) throws NamingException {
    return execute(groupBases, queries.findGroupDnById(group)).getSingleLdapName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isUserMemberOfGroup(String user, String groupDn) throws NamingException {
    String userId = LdapUtils.extractUserName(user);
    return execute(userBases, queries.isUserMemberOfGroup(userId, groupDn)).hasSingleResult();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> findGroupsForUser(String userDn) throws NamingException {
    String userName = LdapUtils.extractUserName(userDn);
    return execute(groupBases, queries.findGroupsForUser(userName, userDn)).getAllLdapNames();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> executeCustomQuery(String query) throws NamingException {
    return execute(Collections.singletonList(baseDn), queries.customQuery(query))
        .getAllLdapNamesAndAttributes();
  }

  private SearchResultHandler execute(Collection<String> baseDns, Query query) {
    List<NamingEnumeration<SearchResult>> searchResults = new ArrayList<>();
    LOG.debug("Executing a query: '{}' with base DNs {}.", query.getFilter(), baseDns);
    for (String aBaseDn : baseDns) {
      try {
        NamingEnumeration<SearchResult> searchResult = ctx.search(aBaseDn, query.getFilter(),
            query.getControls());
        if (searchResult != null) {
          searchResults.add(searchResult);
        }
      } catch (NamingException ex) {
        LOG.debug("Exception happened for query '" + query.getFilter() +
            "' with base DN '" + aBaseDn + "'", ex);
      }
    }
    return new SearchResultHandler(searchResults);
  }
}
