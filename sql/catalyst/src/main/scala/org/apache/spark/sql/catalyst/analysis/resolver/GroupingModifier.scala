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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.HashSet

import org.apache.spark.sql.catalyst.expressions.ExprId

/**
 * Controls how attributes above an Aggregate are tagged for access control in [[NameScope]].
 */
sealed trait GroupingModifier

object GroupingModifier {

  /** No aggregate context. */
  case object NoGrouping extends GroupingModifier

  /** Regular GROUP BY: grouping attributes are freely accessible, others require an aggregate. */
  case class GroupBy(attributeIds: HashSet[ExprId]) extends GroupingModifier

  /**
   * GROUPING SETS / CUBE / ROLLUP: like [[GroupBy]], but Expand-output grouping attribute ExprIds
   * are additionally stored as unaggregated-access-only so that inside aggregate expressions the
   * post-Expand attributes are skipped in favor of the pre-Expand originals.
   */
  case class GroupingAnalytics(attributeIds: HashSet[ExprId]) extends GroupingModifier
}
