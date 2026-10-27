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
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.ScanBuilder

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down filters to the data source. The pushed down filters will be separated into partition
 * filters and data filters. Partition filters are used for partition pruning and data filters are
 * used to reduce the size of the data to be read.
 */
trait SupportsPushDownCatalystFilters extends ScanBuilder {

  /**
   * Pushes down catalyst Expression filters (which will be separated into partition filters and
   * data filters), and returns data filters that need to be evaluated after scanning.
   */
  def pushFilters(filters: Seq[Expression]): Seq[Expression]

  /**
   * Returns additional filters inferred from query filters passed to [[pushFilters]].
   * Spark calls this method only after passing at least one deterministic, subquery-free query
   * filter to the Catalyst [[pushFilters]] callback. If the builder also implements
   * `SupportsPushDownFilters` or `SupportsPushDownV2Filters`, Spark uses that API instead and does
   * not collect inferred filters through this interface.
   *
   * Each inferred filter must be implied by those query filters and evaluate to SQL `true`
   * (never `false` or `null`) for every row returned by the scan.
   *
   * When `SupportsReportStatistics.reflectsFullyPushedDownFilters` returns `false`, Spark adds
   * inferred predicates as logical filters for statistics adjustment by default. Scans can opt
   * into separate estimation with `SupportsReportStatistics.useInferredFilterEstimation`. For
   * those scans, inferred filters remain metadata and are not evaluated by Spark. With CBO
   * enabled, Spark estimates the original and inferred groups against the same scan statistics
   * and takes the smaller estimate. Columns are not retained solely for statistics adjustment,
   * and filters that reference pruned columns are dropped.
   *
   * Spark discards inferred filters if a join, aggregate, or variant extraction replaces the scan
   * output.
   *
   * Inferred filters must be deterministic, contain no subqueries, user-defined expressions,
   * aggregate expressions, window expressions, or generators, resolve to well-typed Boolean
   * expressions, and not duplicate fully pushed filters. They must be evaluable after column
   * binding, without further analyzer or optimizer rewrites. Spark ignores invalid inferred
   * filters.
   *
   * Column references must be represented by `AttributeReference`. A nested column is represented
   * by a dotted name, with path parts containing dots quoted using Spark SQL identifier syntax.
   * For example, nested column `tz` in `location` is `location.tz`, while nested column `c.d` in
   * top-level column `a.b` is represented as `` `a.b`.`c.d` ``.
   * Sources must not report ordinal-based field accessors such as `GetStructField` or
   * `GetArrayStructFields`; Spark resolves the dotted names against the relation's schema.
   */
  def inferredFilters: Seq[Expression] = Nil

  /**
   * Returns the data filters that are pushed to the data source via
   * {@link #pushFilters(Seq[Expression])}.
   */
  def pushedFilters: Array[Predicate]
}
