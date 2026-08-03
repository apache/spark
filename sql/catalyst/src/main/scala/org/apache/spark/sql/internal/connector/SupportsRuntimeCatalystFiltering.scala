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
 * Only one runtime filtering interface should be implemented by a data source.
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
   */
  def filterAttributes(): Array[NamedReference]

  /**
   * Returns attributes for which this scan fully evaluates runtime predicates.
   *
   * Any runtime predicate that references only attributes in this set is considered fully pushed
   * and will not be evaluated again after the scan. These attributes must also be returned by
   * [[filterAttributes]].
   */
  def fullyPushedFilterAttributes(): Array[NamedReference] = Array.empty

  /**
   * Filters this scan using runtime Catalyst expressions.
   *
   * The provided expressions must be interpreted as a set of predicates that are ANDed together.
   * Implementations may use the expressions to prune initially planned
   * [[org.apache.spark.sql.connector.read.InputPartition]]s.
   *
   * Note that Spark will call [[Scan.toBatch]] again after filtering the scan at runtime.
   */
  def filter(expressions: Array[Expression]): Unit

  /**
   * Returns the predicates that are pushed to the data source via [[filter]].
   *
   * This method does not indicate whether a predicate is fully pushed. Spark infers that from
   * [[fullyPushedFilterAttributes]]. The returned predicates may fully or partially help the data
   * source prune initially planned
   * [[org.apache.spark.sql.connector.read.InputPartition]]s.
   *
   * It's possible that there are no runtime predicates and [[filter]] is never called;
   * an empty array should be returned for this case.
   */
  def pushedPredicates(): Array[Expression] = Array.empty
}
