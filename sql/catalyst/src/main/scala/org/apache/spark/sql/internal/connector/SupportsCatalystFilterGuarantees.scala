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

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.NamedReference

/** A table that declares when its scans fully evaluate Catalyst predicates. */
trait SupportsCatalystFilterGuarantees extends Table {

  /**
   * Returns attributes for which this table's scans fully evaluate Catalyst filters.
   *
   * For scans reporting inferred filters, Spark omits executable static filter conjuncts that
   * reference only these attributes. The source must fully enforce the query filters over these
   * attributes during pushdown. Logical filters remain available for statistics estimation.
   * Boolean projections retain their evaluation, including aliases created by MergeSubplans.
   *
   * Catalyst runtime-filtering scans also omit post-scan evaluation of eligible runtime predicates
   * referencing only these attributes. Each attribute's value must be fixed within every returned
   * input partition, and the scan must return only partitions satisfying the predicates passed to
   * [[SupportsRuntimeCatalystFiltering.filter]]. These attributes must also be returned by
   * [[SupportsRuntimeCatalystFiltering.filterAttributes]] when present in the scan output.
   *
   * References must be top-level table columns. Spark ignores columns pruned from the scan.
   * The source must evaluate every eligible predicate over these attributes with Spark's semantics,
   * including error behavior. A root struct declares support for every nested field.
   *
   * This declaration takes precedence over
   * [[SupportsRuntimeCatalystFiltering.fullyPushedFilterAttributes]].
   */
  def fullyPushedFilterAttributes(): Array[NamedReference] = Array.empty
}
