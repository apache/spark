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

package org.apache.spark.sql.catalyst.expressions

/**
 * Extracts and processes grouping expressions from GROUP BY clauses that may contain
 * grouping analytics operations (GROUPING SETS, ROLLUP, CUBE).
 *
 * This object analyzes a sequence of expressions to compute:
 *  1. The cartesian product of selected group-by expressions from all grouping sets
 *  2. A distinct list of all group-by expressions used across all grouping sets
 */
object GroupingAnalyticsExtractor {

  def apply(expressions: Seq[Expression]): Option[(Seq[Seq[Expression]], Seq[Expression])] = {
    val groups = expressions.flatMap {
      case baseGroupingSets: BaseGroupingSets => baseGroupingSets.groupByExprs
      case other: Expression => other :: Nil
    }

    val unmergedSelectedGroupByExprs = expressions.map {
      case baseGroupingSets: BaseGroupingSets => baseGroupingSets.selectedGroupByExprs
      case other: Expression => Seq(Seq(other))
    }

    val selectedGroupByExprs = unmergedSelectedGroupByExprs.tail
      .foldLeft(unmergedSelectedGroupByExprs.head) { (x, y) =>
        for (a <- x; b <- y) yield a ++ b
      }

    Some((selectedGroupByExprs, BaseGroupingSets.distinctGroupByExprs(groups)))
  }
}
