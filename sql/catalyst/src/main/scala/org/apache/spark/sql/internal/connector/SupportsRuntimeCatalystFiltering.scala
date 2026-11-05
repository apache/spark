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
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Scan

/**
 * A mix-in interface for [[Scan]]. Data sources can implement this interface if they can
 * filter initially planned [[org.apache.spark.sql.connector.read.InputPartition]]s using
 * Catalyst [[Expression]]s Spark infers at runtime.
 * A scan must not implement this interface together with
 * [[org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering]] or its subinterface
 * [[org.apache.spark.sql.connector.read.SupportsRuntimeFiltering]]; Spark rejects such a scan.
 *
 * Spark considers a runtime predicate fully pushed when all attributes referenced by the
 * predicate are returned by the table's
 * [[SupportsCatalystFilterGuarantees.fullyPushedFilterAttributes]]. Fully pushed predicates are not
 * evaluated again after the scan. If the table does not implement that interface, Spark uses this
 * scan's [[fullyPushedFilterAttributes]] declaration.
 *
 * Note that Spark will push runtime filters only if they are beneficial.
 */
trait SupportsRuntimeCatalystFiltering extends Scan {

  /**
   * Returns attributes this scan can be filtered by at runtime.
   *
   * Spark will call [[filter]] if it can derive a runtime filter for any of these attributes.
   * Each reference must resolve against the scan relation output when Spark builds it. Attributes
   * pruned out of [[Scan.readSchema]] fail to resolve.
   */
  def filterAttributes(): Array[NamedReference]

  /**
   * Returns attributes for which this scan fully evaluates runtime predicates.
   *
   * Spark uses this declaration when the table does not implement
   * [[SupportsCatalystFilterGuarantees]].
   * References must be top-level scan output columns also returned by [[filterAttributes]]. Each
   * column's value must be fixed within each returned input partition, and the scan must fully
   * evaluate every runtime predicate over these columns with Spark's semantics, including errors.
   *
   * @deprecated Implement [[SupportsCatalystFilterGuarantees]] on the table and declare the
   *             attributes in [[SupportsCatalystFilterGuarantees.fullyPushedFilterAttributes]].
   */
  @deprecated("Use SupportsCatalystFilterGuarantees.fullyPushedFilterAttributes on the table",
    "4.4.0")
  def fullyPushedFilterAttributes(): Array[NamedReference] = Array.empty

  /**
   * Filters this scan using runtime Catalyst expressions.
   *
   * The provided expressions must be interpreted as a set of predicates that are ANDed together.
   * Implementations may use the expressions to prune initially planned
   * [[org.apache.spark.sql.connector.read.InputPartition]]s.
   *
   * Spark currently tracks runtime-filter eligibility by root attribute. If [[filterAttributes]]
   * returns a nested reference, an expression may access another nested field under the same root.
   * The scan must match each access against its own partition layout and use only expressions it
   * can apply.
   *
   * Spark may call this method more than once for the same scan instance: a plan can hold several
   * scan nodes sharing one scan (e.g. the two branches of a group-based UPDATE), and each pushes
   * its own copy of the runtime filters. Implementations must treat successive calls as additive,
   * ANDing the new expressions with those already pushed rather than replacing them.
   *
   * If the scan also implements
   * [[org.apache.spark.sql.connector.read.SupportsReportPartitioning]], it must preserve
   * the originally reported partitioning during runtime filtering. While applying runtime
   * predicates, the scan may detect that some
   * [[org.apache.spark.sql.connector.read.InputPartition]]s have no matching data, in which
   * case it can either replace the initially planned
   * [[org.apache.spark.sql.connector.read.InputPartition]]s that have no matching data with
   * empty [[org.apache.spark.sql.connector.read.InputPartition]]s, or report only a subset of
   * the original partition values (omitting those with no data) via
   * [[org.apache.spark.sql.connector.read.Batch#planInputPartitions]]. The scan must not
   * report new partition values that were not present in the original partitioning.
   *
   * Note that Spark will call [[Scan.toBatch]] again after filtering the scan at runtime.
   */
  def filter(expressions: Array[Expression]): Unit
}
