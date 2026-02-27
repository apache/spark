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

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Structure used to return results of expression transformation during window resolution.
 *
 *  @param expression: Resulting expression for the window project list. Different types of results
 *      are explained in [[WindowResolver.transformExpression]]
 *  @param hasNestedWindowExpressions: Flag indicating whether there are window expressions nested
 *       in a window specification in the original expression's subtree. For example:
 *
 *      {{{
 *      SELECT (1 + SUM(col1) OVER (PARTITION BY SUM(col1) OVER ())), col1 FROM VALUES 1, 2;
 *      }}}
 *
 *      - `SUM(col1) OVER ()` doesn't contain a nested window expression,
 *      `hasNestedWindowExpressions` is `false`.
 *      - `SUM(col1) OVER (PARTITION BY SUM(col1) OVER ())` contains a nested window expression,
 *      `hasNestedWindowExpressions` is `true`.
 *      - `(1 + SUM(col1) OVER (PARTITION BY SUM(col1) OVER ()))` contains the previous expression,
 *      therefore `hasNestedWindowExpressions` is also `true`.
 *      `col1` is an attribute, so `hasNestedWindowExpressions` is `false`.
 */
case class TransformedWindowExpressionResult(
    expression: Expression,
    hasNestedWindowExpressions: Boolean
)
