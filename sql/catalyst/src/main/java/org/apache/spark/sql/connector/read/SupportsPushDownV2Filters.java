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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down V2 {@link Predicate} to the data source and reduce the size of the data to be read.
 * Please Note that this interface is preferred over {@link SupportsPushDownFilters}, which uses
 * V1 {@link org.apache.spark.sql.sources.Filter} and is less efficient due to the
 * internal -&gt; external data conversion.
 *
 * @since 3.3.0
 */
@Evolving
public interface SupportsPushDownV2Filters extends ScanBuilder {

  /**
   * Pushes down predicates, and returns predicates that need to be evaluated after scanning.
   * <p>
   * Rows should be returned from the data source if and only if all of the predicates match.
   * That is, predicates must be interpreted as ANDed together.
   */
  Predicate[] pushPredicates(Predicate[] predicates);

  /**
   * Returns the predicates that are pushed to the data source via
   * {@link #pushPredicates(Predicate[])}.
   * <p>
   * There are 3 kinds of predicates:
   * <ol>
   *  <li>pushable predicates which don't need to be evaluated again after scanning.</li>
   *  <li>pushable predicates which still need to be evaluated after scanning, e.g. parquet row
   *  group predicate.</li>
   *  <li>non-pushable predicates.</li>
   * </ol>
   * <p>
   * Both case 1 and 2 should be considered as pushed predicates and should be returned
   * by this method.
   * <p>
   * It's possible that there is no predicates in the query and
   * {@link #pushPredicates(Predicate[])} is never called,
   * empty array should be returned for this case.
   */
  Predicate[] pushedPredicates();
}
