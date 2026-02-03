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

import scala.collection.mutable.LinkedHashMap

import org.apache.spark.sql.catalyst.analysis.resolver.WindowTypedefs.{
  WindowExpressionAliasesWithSpecId,
  WindowSpec
}
import org.apache.spark.sql.catalyst.expressions.NamedExpression

/**
 * Structure used to return results of the transformed window project list.
 *
 *  - specificationMap: Collection of window expression buckets grouped by window spec.
 *  - windowProjectList: Expression list used for constructing [[Window]]. It matches the output
 *      schema of the original operator:
 *
 *      {{{
 *      SELECT
 *        col1,
 *        1 + SUM(col2),
 *        SUM(col1) OVER ()
 *      FROM
 *        VALUES (1, 2)
 *      GROUP BY col1;
 *      }}}
 *
 *      Analyzed plan:
 *
 *      Window [col1#0,
 *             (cast(1 as bigint) + _w1#2) AS (1 + sum(col2))#3,
 *             sum(col1#0) windowspecdefinition...#4]
 *      +- Aggregate [col1#0], [col1#0, sum(col2#1) AS _w1#2]
 *
 *      In this case `windowProjectList` will contain three expressions:
 *       - col1#0
 *       - (cast(1 as bigint) + _w1#2) AS (1 + sum(col2))#3
 *       - sum(col1#0) windowspecdefinition...#4
 *  - sourceProjectList: Expression list used for constructing the [[Window]] child source operator.
 *      In the previous example it would contain the following expressions:
 *       - col1#0
 *       - sum(col2#1) AS _w1#2
 *  - hasNestedWindowExpressions: Flag indicating whether there are nested window expressions in the
 *      `windowProjectList`.
 */
case class TransformedWindowProjectList(
    specificationMap: LinkedHashMap[WindowSpec, WindowExpressionAliasesWithSpecId],
    windowProjectList: Seq[NamedExpression],
    sourceProjectList: Seq[NamedExpression],
    hasNestedWindowExpressions: Boolean
)
