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
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * A mix-in interface for {@link DataSourceV2Reader}. Data source readers can implement this
 * interface to push down arbitrary expressions as predicates to the data source.
 * This is an experimental and unstable interface as {@link Expression} is not public and may get
 * changed in the future Spark versions.
 *
 * Note that, if data source readers implement both this interface and
 * {@link SupportsPushDownFilters}, Spark will ignore {@link SupportsPushDownFilters} and only
 * process this interface.
 */
@InterfaceStability.Unstable
public interface SupportsPushDownCatalystFilters {

  /**
   * Pushes down filters, and returns unsupported filters.
   */
  Expression[] pushCatalystFilters(Expression[] filters);

  /**
   * Returns the catalyst filters that are pushed in {@link #pushCatalystFilters(Expression[])}.
   * It's possible that there is no filters in the query and
   * {@link #pushCatalystFilters(Expression[])} is never called, empty array should be returned for
   * this case.
   */
  Expression[] pushedCatalystFilters();
}
