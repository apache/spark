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
import org.apache.spark.sql.connector.expressions.PartitionFieldReference;
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down V2 {@link Predicate} to the data source and reduce the size of the data to be read.
 * Please Note that this interface is preferred over {@link SupportsPushDownFilters}, which uses
 * V1 {@link org.apache.spark.sql.sources.Filter} and is less efficient due to the
 * internal -&gt; external data conversion.
 * <p>
 * <b>Iterative pushdown:</b> When {@link #supportsIterativePushdown()} returns true,
 * {@link #pushPredicates(Predicate[])} may be called <i>multiple times</i> on the same
 * {@link ScanBuilder} instance with additional predicates (e.g. {@link PartitionPredicate}).
 * The implementation must accumulate state across all calls, and
 * {@link #pushedPredicates()} must return predicates from all of them.
 *
 * @since 3.3.0
 */
@Evolving
public interface SupportsPushDownV2Filters extends ScanBuilder {

  /**
   * Pushes down predicates, and returns predicates that need to be evaluated after scanning.
   * Any predicate that the data source cannot fully push down must be returned as-is so that
   * Spark can evaluate it after the scan; the data source must not modify or drop such predicates.
   * <p>
   * Rows should be returned from the data source if and only if all of the predicates match.
   * That is, predicates must be interpreted as ANDed together.
   * <p>
   * This method may be called multiple times with additional predicates (e.g.
   * {@link PartitionPredicate} when {@link #supportsIterativePushdown()} returns true).
   * The implementation must accumulate state across all calls so that
   * {@link #pushedPredicates()} can return predicates from all of them.
   * <p>
   * For each {@link PartitionPredicate}, the implementation can use
   * {@link PartitionPredicate#references()} (each {@link PartitionFieldReference} has
   * {@link PartitionFieldReference#ordinal()}) to decide whether to return it for post-scan
   * filtering. For example, data sources with
   * partition spec evolution may return predicates that reference later-added partition
   * transforms (incompletely partitioned data) so Spark evaluates them after the scan, while
   * predicates that reference only initially-added partition transforms may be fully pushed.
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
   * When iterative pushdown is supported and {@link #pushPredicates(Predicate[])} was called
   * multiple times, this method must return predicates from <i>all</i> calls.
   * <p>
   * It's possible that there is no predicates in the query and
   * {@link #pushPredicates(Predicate[])} is never called,
   * empty array should be returned for this case.
   */
  Predicate[] pushedPredicates();

  /**
   * Returns true if this data source supports iterative filter pushdown. When true,
   * {@link #pushPredicates(Predicate[])} may be called multiple times with additional
   * predicates (e.g. {@link PartitionPredicate}). The implementation must accumulate state
   * across all calls, and {@link #pushedPredicates()} must return predicates from all of them.
   * See the class-level Javadoc for the full contract.
   *
   * @since 4.2.0
   */
  default boolean supportsIterativePushdown() {
    return false;
  }
}
