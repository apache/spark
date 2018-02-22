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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The object that handles Directory Service search results.
 * In most cases it converts search results into a list of names in the namespace.
 */
public final class SearchResultHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SearchResultHandler.class);

  private final Collection<NamingEnumeration<SearchResult>> searchResults;

  /**
   * Constructs a search result handler object for the provided search results.
   * @param searchResults directory service search results
   */
  public SearchResultHandler(Collection<NamingEnumeration<SearchResult>> searchResults) {
    this.searchResults = searchResults;
  }

  /**
   * Returns all entries from the search result.
   * @return a list of names in the namespace
   * @throws NamingException
   */
  public List<String> getAllLdapNames() throws NamingException {
    final List<String> result = new ArrayList<>();
    handle(new RecordProcessor() {
      @Override
      public boolean process(SearchResult record) throws NamingException {
        result.add(record.getNameInNamespace());
        return true;
      }
    });
    return result;
  }

  /**
   * Checks whether search result contains exactly one entry.
   * @return true if the search result contains a single entry.
   * @throws NamingException
   */
  public boolean hasSingleResult() throws NamingException {
    List<String> allResults = getAllLdapNames();
    return allResults != null && allResults.size() == 1;
  }

  /**
   * Returns a single entry from the search result.
   * Throws {@code NamingException} if the search result doesn't contain exactly one entry.
   * @return name in the namespace
   * @throws NamingException
   */
  public String getSingleLdapName() throws NamingException {
    List<String> allLdapNames = getAllLdapNames();
    if (allLdapNames.size() == 1) {
      return allLdapNames.get(0);
    }
    throw new NamingException("Single result was expected");
  }

  /**
   * Returns all entries and all attributes for these entries.
   * @return a list that includes all entries and all attributes from these entries.
   * @throws NamingException
   */
  public List<String> getAllLdapNamesAndAttributes() throws NamingException {
    final List<String> result = new ArrayList<>();
    handle(new RecordProcessor() {
      @Override
      public boolean process(SearchResult record) throws NamingException {
        result.add(record.getNameInNamespace());
        NamingEnumeration<? extends Attribute> allAttributes = record.getAttributes().getAll();
        while(allAttributes.hasMore()) {
          Attribute attribute = allAttributes.next();
          addAllAttributeValuesToResult(attribute.getAll());
        }
        return true;
      }

      private void addAllAttributeValuesToResult(NamingEnumeration values) throws NamingException {
        while(values.hasMore()) {
          result.add(String.valueOf(values.next()));
        }
      }

    });
    return result;
  }

  /**
   * Allows for custom processing of the search results.
   * @param processor {@link RecordProcessor} implementation
   * @throws NamingException
   */
  public void handle(RecordProcessor processor) throws NamingException {
    try {
      for (NamingEnumeration<SearchResult> searchResult : searchResults) {
        while (searchResult.hasMore()) {
          if (!processor.process(searchResult.next())) {
            return;
          }
        }
      }
    } finally {
      for (NamingEnumeration<SearchResult> searchResult : searchResults) {
        try {
          searchResult.close();
        } catch (NamingException ex) {
          LOG.warn("Failed to close LDAP search result", ex);
        }
      }
    }
  }

  /**
   * An interface used by {@link SearchResultHandler} for processing records of
   * a {@link SearchResult} on a per-record basis.
   * <br>
   * Implementations of this interface perform the actual work of processing each record,
   * but don't need to worry about exception handling, closing underlying data structures,
   * and combining results from several search requests.
   * {@see SearchResultHandler}
   */
  public interface RecordProcessor {

    /**
     * Implementations must implement this method to process each record in {@link SearchResult}.
     * @param record the {@code SearchResult} to precess
     * @return {@code true} to continue processing, {@code false} to stop iterating
     * over search results
     * @throws NamingException
     */
    boolean process(SearchResult record) throws NamingException;
  }
}
