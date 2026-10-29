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
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

/**
 * A mix-in for a {@link ScanBuilder} that can serve parameterized lookups. The builder may also
 * implement the column-pruning and filter-pushdown mix-ins.
 *
 * @since 4.4.0
 */
@Evolving
public interface SupportsExternalLookup extends ScanBuilder {
  /**
   * Selects the offered predicates that this source serves as one lookup access path. Spark
   * currently offers {@code =}, {@code <=>}, {@code <}, {@code <=}, {@code >}, {@code >=}
   * comparisons of a lookup-side {@link NamedReference} with a {@link LookupParameter}; the array
   * may be empty. For example:
   * <pre>{@code
   *   candidates: [tenant = ?, event_time >= ?, event_time < ?, region = ?]
   *   returned:   [event_time >= ?, tenant = ?]
   *   => each parameter row is (event_time lower bound, tenant);
   *      Spark itself evaluates event_time < ? and region = ?
   * }</pre>
   * <ul>
   *   <li>The result must be non-null and hold offered instances, unchanged, without nulls or
   *   duplicates. Spark matches them by identity, not {@code equals}.</li>
   *   <li>Each offered predicate has exactly one {@link LookupParameter}, so field {@code i} of
   *   every parameter row is the value for the {@code i}-th returned predicate. A field may
   *   appear in several predicates, as range bounds do.</li>
   *   <li>Each selected predicate must hold with Spark semantics, and results must be complete
   *   for their conjunction. Unsupported names or shapes must not be selected.</li>
   *   <li>An empty result means no access path fits; the query then fails.</li>
   *   <li>The input array and its elements are read-only.</li>
   * </ul>
   * Spark calls this method only on builders of lookup scans, exactly once, after filter pushdown
   * and before column pruning. Spark calls no other pushdown method on a lookup builder
   * (aggregate, limit, top-N, offset, sample, join or variant). The selection must survive
   * {@code pruneColumns} and {@link #build()}. For example:
   * <pre>{@code
   *   SELECT s.*, t.name FROM s JOIN t ON t.id = s.id
   *   => selected [id = ?], then pruneColumns(<name>), so readSchema() is (name);
   *      the connector must still look up by id
   * }</pre>
   */
  Predicate[] selectLookupPredicates(Predicate[] candidates);
}
