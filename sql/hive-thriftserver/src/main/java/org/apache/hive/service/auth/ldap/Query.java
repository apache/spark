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

import com.google.common.base.Preconditions;
import org.stringtemplate.v4.ST;

import javax.naming.directory.SearchControls;
import java.util.ArrayList;
import java.util.List;

/**
 * The object that encompasses all components of a Directory Service search query.
 * <br>
 * @see LdapSearch
 */
public final class Query {

  private final String filter;
  private final SearchControls controls;

  /**
   * Constructs an instance of Directory Service search query.
   * @param filter search filter
   * @param controls search controls
   */
  public Query(String filter, SearchControls controls) {
    this.filter = filter;
    this.controls = controls;
  }

  /**
   * Returns search filter.
   * @return search filter
   */
  public String getFilter() {
    return filter;
  }

  /**
   * Returns search controls.
   * @return search controls
   */
  public SearchControls getControls() {
    return controls;
  }

  /**
   * Creates Query Builder.
   * @return query builder.
   */
  public static QueryBuilder builder() {
    return new QueryBuilder();
  }

  /**
   * A builder of the {@link Query}.
   */
  public static final class QueryBuilder {

    private ST filterTemplate;
    private final SearchControls controls = new SearchControls();
    private final List<String> returningAttributes = new ArrayList<>();

    private QueryBuilder() {
      controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
      controls.setReturningAttributes(new String[0]);
    }

    /**
     * Sets search filter template.
     * @param filterTemplate search filter template
     * @return the current instance of the builder
     */
    public QueryBuilder filter(String filterTemplate) {
      this.filterTemplate = new ST(filterTemplate);
      return this;
    }

    /**
     * Sets mapping between names in the search filter template and actual values.
     * @param key marker in the search filter template.
     * @param value actual value
     * @return the current instance of the builder
     */
    public QueryBuilder map(String key, String value) {
      filterTemplate.add(key, value);
      return this;
    }

    /**
     * Sets mapping between names in the search filter template and actual values.
     * @param key marker in the search filter template.
     * @param values array of values
     * @return the current instance of the builder
     */
    public QueryBuilder map(String key, String[] values) {
      filterTemplate.add(key, values);
      return this;
    }

    /**
     * Sets attribute that should be returned in results for the query.
     * @param attributeName attribute name
     * @return the current instance of the builder
     */
    public QueryBuilder returnAttribute(String attributeName) {
      returningAttributes.add(attributeName);
      return this;
    }

    /**
     * Sets the maximum number of entries to be returned as a result of the search.
     * <br>
     * 0 indicates no limit: all entries will be returned.
     * @param limit The maximum number of entries that will be returned.
     * @return the current instance of the builder
     */
    public QueryBuilder limit(int limit) {
      controls.setCountLimit(limit);
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(filterTemplate != null,
          "filter is required for LDAP search query");
    }

    private String createFilter() {
      return filterTemplate.render();
    }

    private void updateControls() {
      if (!returningAttributes.isEmpty()) {
        controls.setReturningAttributes(returningAttributes
            .toArray(new String[returningAttributes.size()]));
      }
    }

    /**
     * Builds an instance of {@link Query}.
     * @return configured directory service query
     */
    public Query build() {
      validate();
      String filter = createFilter();
      updateControls();
      return new Query(filter, controls);
    }
  }
}
