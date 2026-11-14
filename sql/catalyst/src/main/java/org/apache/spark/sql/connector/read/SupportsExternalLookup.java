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
 * implement the column-pruning and filter-pushdown mix-ins. For a lookup scan, Spark evaluates
 * every lookup-side filter again on the returned rows, including the filters that the source
 * reports as pushed, so the source may return rows that fail them. It may omit only rows that
 * fail a filter that it reports as pushed, as {@link ExternalLookupReader#lookupBatch} says.
 * <p>
 * Spark may also plan a lookup when a streaming query starts, up to
 * {@link ExternalLookup#createReaderFactory()}, and discard the result.
 *
 * @since 4.4.0
 */
@Evolving
public interface SupportsExternalLookup extends ScanBuilder {
  /**
   * Selects the offered predicates that this source serves as one lookup access path. Spark
   * offers {@code =}, {@code <=>}, {@code <}, {@code <=}, {@code >}, {@code >=} comparisons of a
   * lookup-side {@link NamedReference} with a {@link LookupParameter}; the array is never empty. In
   * every offered comparison the reference is {@code children()[0]} and the parameter is
   * {@code children()[1]}: when the query writes the lookup column on the right, Spark flips the
   * operator, so {@code s.ts < t.event_time} is offered as {@code event_time > ?}. Spark offers
   * {@code =} and {@code <=>} only on a lookup column of an integral type, BOOLEAN, DECIMAL, DATE,
   * TIMESTAMP, TIMESTAMP_NTZ, BINARY, or a STRING or VARCHAR whose collation compares bytes, such
   * as UTF8_BINARY. It offers {@code <}, {@code <=}, {@code >} and {@code >=} only on a lookup
   * column of an integral type, DECIMAL, DATE, TIMESTAMP or TIMESTAMP_NTZ, and only when the
   * parameter cannot fail to evaluate. When a selected predicate is a range comparison whose
   * parameter reads a stream column, each request holds exactly one parameter row. It offers no
   * comparison on VARCHAR under {@code spark.sql.charVarchar.standardSemantics.enabled}, and never
   * one on FLOAT, DOUBLE, CHAR, a string with another collation, a complex type or any other type.
   * For example:
   * <pre>{@code
   *   candidates: [tenant = ?, event_time >= ?, event_time < ?, region = ?]
   *   returned:   [event_time >= ?, tenant = ?]
   *   => each parameter row is (event_time lower bound, tenant);
   *      event_time < ? and region = ? stay in Spark's join condition
   * }</pre>
   * <ul>
   *   <li>The result must be non-null and hold offered instances, unchanged, without nulls or
   *   duplicates. Spark matches them by identity, not {@code equals}.</li>
   *   <li>Each offered predicate has exactly one {@link LookupParameter}, so field {@code i} of
   *   every parameter row is the value for the {@code i}-th returned predicate. A field may
   *   appear in several predicates, as range bounds do.</li>
   *   <li>Spark evaluates every selected predicate again on each returned row and drops a row
   *   that fails it, so extra rows do no harm. Spark cannot recover a missing row: for each
   *   parameter row, the source must return every row that passes the filters this source reports
   *   as pushed and satisfies all selected predicates under Spark semantics, as
   *   {@link ExternalLookupReader#lookupBatch} defines. Do not select a predicate that the source
   *   cannot evaluate that way, and do not select unsupported names or shapes.</li>
   *   <li>An empty result means no access path fits; the query then fails.</li>
   *   <li>The input array and its elements are read-only.</li>
   * </ul>
   * Spark calls this method only on builders of lookup scans, at most once, after any filter
   * pushdown and before column pruning. Besides filter pushdown and column pruning, Spark calls
   * no pushdown method on a lookup builder (aggregate, limit, top-N, offset, sample, join or
   * variant). Spark passes only whole top-level columns to {@code pruneColumns}, never nested
   * fields. Spark always keeps the columns that the selected predicates read in the pruned schema,
   * because it evaluates the predicates on them. So returned rows must carry the stored values of
   * those columns. The selection must survive {@code pruneColumns} and {@link #build()}. For
   * example, with a streaming {@code s}:
   * <pre>{@code
   *   SELECT s.*, t.name FROM s JOIN t ON t.id = s.id
   *   => selected [id = ?], then pruneColumns(<id, name>);
   *      Spark keeps id because it evaluates id = ? on every returned row
   * }</pre>
   */
  Predicate[] selectLookupPredicates(Predicate[] candidates);
}
