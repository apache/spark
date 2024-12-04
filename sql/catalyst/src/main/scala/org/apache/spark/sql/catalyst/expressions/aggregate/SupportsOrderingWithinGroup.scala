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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions.SortOrder

/**
 * The trait used to set the [[SortOrder]] for supporting functions.
 */
trait SupportsOrderingWithinGroup { self: AggregateFunction =>
  def withOrderingWithinGroup(orderingWithinGroup: Seq[SortOrder]): AggregateFunction

  /** Indicator that ordering was set. */
  def orderingFilled: Boolean

  /**
   * Tells Analyzer that WITHIN GROUP (ORDER BY ...) is mandatory for function.
   *
   * @see [[QueryCompilationErrors.functionMissingWithinGroupError]]
   */
  def isOrderingMandatory: Boolean

  /**
   * Tells Analyzer that DISTINCT is supported.
   * The DISTINCT can conflict with order so some functions can ban it.
   *
   * @see [[QueryCompilationErrors.functionMissingWithinGroupError]]
   */
  def isDistinctSupported: Boolean
}
