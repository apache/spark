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
 * predicate are returned by [[fullyPushedFilterAttributes]]. Fully pushed predicates are not
 * evaluated again after the scan.
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
   * Any runtime predicate that references only attributes in this set is considered fully pushed
   * and will not be evaluated again after the scan. These attributes must also be returned by
   * [[filterAttributes]]. Each attribute's value must therefore be fixed within every
   * [[org.apache.spark.sql.connector.read.InputPartition]] the scan returns, since pruning
   * partitions cannot fully evaluate a predicate on a column that varies within a partition.
   *
   * Spark relies on the scan alone here, so the scan must return only partitions it has proven
   * satisfy such a predicate. Spark passes these expressions through as they are, leaving both
   * translation and capability checking to the scan, so evaluating one can fail, for example on
   * an ANSI cast or overflow error, or on a nested access that the scan matches differently
   * against its partition layout. Declare an attribute only when the scan can evaluate every
   * predicate over it.
   *
   * Each reference must be a top-level attribute present in [[Scan.readSchema]]. Nested
   * references are rejected, and attributes pruned out of the read schema fail to resolve, when
   * Spark builds the scan relation. Spark cannot currently represent an individual fully pushed
   * nested path. A scan must not return the root struct as a substitute unless it can fully
   * evaluate predicates over every nested field, since Spark would remove their post-scan
   * evaluation as well.
   */
  def fullyPushedFilterAttributes(): Array[NamedReference] = Array.empty

  /**
   * Filters this scan using runtime Catalyst expressions.
   *
   * The provided expressions must be interpreted as a set of predicates that are ANDed together.
   * Implementations may use the expressions to prune initially planned
   * [[org.apache.spark.sql.connector.read.InputPartition]]s.
   *
   * Spark tracks runtime-filter eligibility by root attribute. If [[filterAttributes]] returns a
   * nested reference, an expression may access another nested field under the same root. The scan
   * must match each access against its own partition layout and use only expressions it can apply.
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
