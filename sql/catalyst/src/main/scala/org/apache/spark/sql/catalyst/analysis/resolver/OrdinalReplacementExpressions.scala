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

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Sort

/**
 * Common trait used to store expression that are candidates to replace ordinals in grouping or
 * sorting expressions.
 */
sealed trait OrdinalReplacementExpressions

/**
 * Wrapper object used to store expression that are candidates to replace ordinals in grouping
 * expressions.
 */
case class OrdinalReplacementGroupingExpressions(
    expressions: IndexedSeq[NamedExpression],
    hasStar: Boolean,
    expressionIndexesWithAggregateFunctions: HashSet[Int])
    extends OrdinalReplacementExpressions

/**
 * Wrapper object used to store expression that are candidates to replace ordinals in sorting
 * expressions.
 */
case class OrdinalReplacementSortOrderExpressions(
    expressions: IndexedSeq[NamedExpression],
    unresolvedSort: Sort)
    extends OrdinalReplacementExpressions
