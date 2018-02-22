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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.security.sasl.AuthenticationException;
import java.util.ArrayList;
import java.util.List;

/**
 * A factory that produces a {@link Filter} that is implemented as a chain of other filters.
 * The chain of filters are created as a result of
 * {@link #getInstance(org.apache.hadoop.hive.conf.HiveConf) }
 * method call. The resulting object filters out all users that don't pass <b>all</b>
 * chained filters. The filters will be applied in the order they are mentioned in the factory
 * constructor.
 */
public class ChainFilterFactory  implements FilterFactory {

  private final List<FilterFactory> chainedFactories;

  /**
   * Constructs a factory for a chain of filters.
   *
   * @param factories The array of factories that will be used to construct a chain of filters.
   */
  public ChainFilterFactory(FilterFactory... factories) {
    this.chainedFactories = ImmutableList.copyOf(factories);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Filter getInstance(HiveConf conf) {
    List<Filter> filters = new ArrayList<>();
    for (FilterFactory factory : chainedFactories) {
      Filter filter = factory.getInstance(conf);
      if (filter != null) {
        filters.add(filter);
      }
    }

    return filters.isEmpty() ? null : new ChainFilter(ImmutableList.copyOf(filters));
  }

  private static final class ChainFilter implements Filter {

    private final List<Filter> chainedFilters;

    public ChainFilter(List<Filter> chainedFilters) {
      this.chainedFilters = chainedFilters;
    }

    @Override
    public void apply(DirSearch client, String user) throws AuthenticationException {
      for (Filter filter : chainedFilters) {
        filter.apply(client, user);
      }
    }
  }
}
