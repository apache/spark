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

import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import java.util.List;

/**
 * A factory for a {@link Filter} based on a custom query.
 * <br>
 * The produced filter object filters out all users that are not found in the search result
 * of the query provided in Hive configuration.
 * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY
 */
public class CustomQueryFilterFactory implements FilterFactory {

  /**
   * {@inheritDoc}
   */
  @Override
  public Filter getInstance(HiveConf conf) {
    String customQuery = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY);

    if (Strings.isNullOrEmpty(customQuery)) {
      return null;
    }

    return new CustomQueryFilter(customQuery);
  }

  private static final class CustomQueryFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(CustomQueryFilter.class);

    private final String query;

    public CustomQueryFilter(String query) {
      this.query = query;
    }

    @Override
    public void apply(DirSearch client, String user) throws AuthenticationException {
      List<String> resultList;
      try {
        resultList = client.executeCustomQuery(query);
      } catch (NamingException e) {
        throw new AuthenticationException("LDAP Authentication failed for user", e);
      }
      if (resultList != null) {
        for (String matchedDn : resultList) {
          String shortUserName = LdapUtils.getShortName(matchedDn);
          LOG.info("<queried user=" + shortUserName + ",user=" + user + ">");
          if (shortUserName.equalsIgnoreCase(user) || matchedDn.equalsIgnoreCase(user)) {
            LOG.info("Authentication succeeded based on result set from LDAP query");
            return;
          }
        }
      }
      LOG.info("Authentication failed based on result set from custom LDAP query");
      throw new AuthenticationException("Authentication failed: LDAP query "
          + "from property returned no data");
    }
  }
}
