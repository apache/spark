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

package org.apache.spark.sql.internal.connector

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Scan

/** A table that declares when its scans fully evaluate Catalyst predicates. */
trait SupportsCatalystFilterGuarantees extends Table {

  /**
   * Returns attributes for which this table's Catalyst runtime-filtering scans fully evaluate
   * runtime predicates.
   *
   * Spark omits post-scan evaluation of eligible runtime predicates referencing only these
   * attributes. Each attribute's value must be fixed within every returned input partition,
   * and the scan must return only partitions proven to satisfy the predicates passed to
   * [[SupportsRuntimeCatalystFiltering.filter]]. The scan must be able to evaluate every
   * predicate over these attributes with Spark's semantics, including error behavior.
   *
   * References must be top-level table columns. Spark ignores columns pruned from the scan;
   * remaining columns must also be returned by
   * [[SupportsRuntimeCatalystFiltering.filterAttributes]].
   * A root struct declares support for predicates over every nested field.
   */
  def fullyPushedFilterAttributes(): Array[NamedReference] = Array.empty

  /**
   * Returns whether `filter` evaluates to SQL `true` for every row returned by `scan`.
   *
   * Spark calls this read-only method during physical planning. A `true` result allows Spark to
   * omit the conjunct from executable filters and Boolean projections. Logical filters remain
   * available for statistics estimation. Return `false` when the predicate can evaluate to
   * `false` or `null`, or the table cannot prove it true for the supplied scan.
   *
   * Spark checks deterministic, subquery-free top-level conjuncts, including optimizer-generated
   * expressions. Column references are bound to the scan output and may have different expression
   * IDs from those passed during pushdown. The guarantee must hold for the supplied scan,
   * including after scan rebuilding, and must not depend on filters evaluated above it. This
   * method must not change the table or scan, push filters, or plan input partitions.
   */
  def isFilterGuaranteed(scan: Scan, filter: Expression): Boolean = false
}
