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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical._

trait AliasAwareOutputPartitioning extends UnaryExecNode {

  protected def outputExpressions: Seq[NamedExpression]

  // If projects and aggregates have aliases in output expressions, we should respect
  // these aliases so as to check if the operators satisfy their output distribution requirements.
  // If we don't respect aliases, this rule wrongly adds shuffle operations, e.g.,
  //
  // spark.range(10).selectExpr("id AS key", "0").repartition($"key").write.saveAsTable("df1")
  // spark.range(10).selectExpr("id AS key", "0").repartition($"key").write.saveAsTable("df2")
  // sql("""
  //   SELECT * FROM
  //     (SELECT key AS k from df1) t1
  //   INNER JOIN
  //     (SELECT key AS k from df2) t2
  //   ON t1.k = t2.k
  // """).explain
  //
  // == Physical Plan ==
  // *SortMergeJoin [k#56L], [k#57L], Inner
  // :- *Sort [k#56L ASC NULLS FIRST], false, 0
  // :  +- Exchange hashpartitioning(k#56L, 200) // <--- Unnecessary shuffle operation
  // :     +- *Project [key#39L AS k#56L]
  // :        +- Exchange hashpartitioning(key#39L, 200)
  // :           +- *Project [id#36L AS key#39L]
  // :              +- *Range (0, 10, step=1, splits=Some(4))
  // +- *Sort [k#57L ASC NULLS FIRST], false, 0
  //    +- ReusedExchange [k#57L], Exchange hashpartitioning(k#56L, 200)
  final override def outputPartitioning: Partitioning = if (hasAlias(outputExpressions)) {
    resolveOutputPartitioningByAliases(outputExpressions, child.outputPartitioning)
  } else {
    child.outputPartitioning
  }

  private def hasAlias(exprs: Seq[NamedExpression]): Boolean =
    exprs.exists(_.collectFirst { case _: Alias => true }.isDefined)

  private def resolveOutputPartitioningByAliases(
      exprs: Seq[NamedExpression],
      partitioning: Partitioning): Partitioning = {
    val aliasSeq = exprs.flatMap(_.collectFirst {
      case a @ Alias(child, _) => (child, a.toAttribute)
    })
    def mayReplaceExprWithAlias(e: Expression): Expression = {
      aliasSeq.find { case (c, _) => c.semanticEquals(e) }.map(_._2).getOrElse(e)
    }
    def mayReplacePartitioningExprsWithAliases(p: Partitioning): Partitioning = p match {
      case hash @ HashPartitioning(exprs, _) =>
        hash.copy(expressions = exprs.map(mayReplaceExprWithAlias))
      case range @ RangePartitioning(ordering, _) =>
        range.copy(ordering = ordering.map { order =>
          order.copy(
            child = mayReplaceExprWithAlias(order.child),
            sameOrderExpressions = order.sameOrderExpressions.map(mayReplaceExprWithAlias)
          )
        })
      case _ => p
    }

    partitioning match {
      case pc @ PartitioningCollection(ps) =>
        pc.copy(partitionings = ps.map(mayReplacePartitioningExprsWithAliases))
      case _ =>
        mayReplacePartitioningExprsWithAliases(partitioning)
    }
  }
}
