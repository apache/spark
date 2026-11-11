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
trait SupportsFullyPushedCatalystFilters extends Table {

  /**
   * Returns attributes for which this table's scans fully evaluate Catalyst filters.
   *
   * For scans reporting inferred filters, Spark may omit static conjuncts over these attributes
   * that are unchanged from returned residuals or adjustment predicates recorded during pushdown.
   * Eligible conjuncts must be deterministic and contain no subqueries or Python UDFs. The source
   * must fully enforce the query filters it receives over these attributes, including residuals
   * returned to Spark, and reflect their enforcement in Scan equality for logical scan comparison.
   * For physical reuse, enforcement must also be reflected in equality and hashCode of the Batch
   * returned by Scan.toBatch, or the relation returned by toV1TableScan for V1-fallback scans.
   *
   * The declaration must stay fixed for a Table instance and hold for every scan its builders
   * produce, including scans built with different read options.
   *
   * References may name top-level data or metadata columns, or nested struct fields. Declaring a
   * struct covers all of its nested fields. Spark ignores fields pruned from the scan. The source
   * must evaluate every eligible predicate over these fields with Spark's semantics, including
   * error behavior. Declarations apply to the original table columns, not new output columns
   * created by pushed joins, aggregates or variant extraction, even if their names match.
   */
  def fullyPushedFilterAttributes(): Array[NamedReference] = Array.empty
}
