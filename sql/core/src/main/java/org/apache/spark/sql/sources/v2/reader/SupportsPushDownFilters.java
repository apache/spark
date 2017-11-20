/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.Filter;

/**
 * A mix-in interface for {@link DataSourceV2Reader}. Data source readers can implement this
 * interface to push down filters to the data source and reduce the size of the data to be read.
 *
 * Note that, if data source readers implement both this interface and
 * {@link SupportsPushDownCatalystFilters}, Spark will ignore this interface and only process
 * {@link SupportsPushDownCatalystFilters}.
 */
@InterfaceStability.Evolving
public interface SupportsPushDownFilters {

  /**
   * Pushes down filters, and returns unsupported filters.
   */
  Filter[] pushFilters(Filter[] filters);

  /**
   * Returns the filters that are pushed in {@link #pushFilters(Filter[])}.
   * It's possible that there is no filters in the query and {@link #pushFilters(Filter[])}
   * is never called, empty array should be returned for this case.
   */
  Filter[] pushedFilters();
}
