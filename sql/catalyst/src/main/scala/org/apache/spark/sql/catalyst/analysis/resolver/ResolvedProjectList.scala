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

import org.apache.spark.sql.catalyst.expressions.NamedExpression

/**
 * Structure used to return results of the resolved project list.
 *  - expressions: The resolved expressions. It is resolved using the
 *                 `resolveExpressionTreeInOperator`.
 *  - hasAggregateExpressions: True if the resolved project list contains any aggregate
 *                             expressions.
 *  - hasLateralColumnAlias: True if the resolved project list contains any lateral column aliases.
 */
case class ResolvedProjectList(
    expressions: Seq[NamedExpression],
    hasAggregateExpressions: Boolean,
    hasLateralColumnAlias: Boolean)
