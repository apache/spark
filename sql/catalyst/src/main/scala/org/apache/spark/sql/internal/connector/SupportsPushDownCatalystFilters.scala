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
   * Returns additional filters that the data source inferred and that the query does not need in
   * order to return the right rows. These are advisory: Spark adds them to the logical Filter node
   * only so that later optimizer rules can use them, for example to make partition statistics more
   * accurate for cost-based optimization. They are discarded if a join, aggregate, or variant
   * extraction is pushed. Those operators replace the scan output, so Spark cannot rebind the
   * advisory filters onto it. Spark never evaluates them, so they are dropped from FilterExec in
   * the final physical plan.
   *
   * Spark asks for these after [[pushFilters]], so a source can infer them from the user filters,
   * if necessary. For now this is only supported when there are filters being pushed down.
   *
   * Advisory filters must be deterministic and must not contain subqueries or user-defined
   * expressions.
   *
   * Column references must be represented by `AttributeReference`. A nested column is represented
   * by a dotted name, with path parts containing dots quoted using Spark SQL identifier syntax.
   * For example, nested column `tz` in `location` is `location.tz`, while nested column `c.d` in
   * top-level column `a.b` is represented as `` `a.b`.`c.d` ``.
   */
  def advisoryFilters: Seq[Expression] = Nil

  /**
   * Returns the data filters that are pushed to the data source via
   * {@link #pushFilters(Seq[Expression])}.
   */
  def pushedFilters: Array[Predicate]
}
