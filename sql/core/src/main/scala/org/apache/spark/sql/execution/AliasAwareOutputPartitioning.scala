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

import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection, UnknownPartitioning}

/**
 * Trait for plans which can produce an output partitioned by aliased attributes of their child.
 * It rewrites the partitioning attributes of the child with the corresponding new ones which are
 * exposed in the output of this plan. It can avoid the presence of redundant shuffles in queries
 * caused by the rename of an attribute among the partitioning ones, eg.
 *
 * spark.range(10).selectExpr("id AS key", "0").repartition($"key").write.saveAsTable("df1")
 * spark.range(10).selectExpr("id AS key", "0").repartition($"key").write.saveAsTable("df2")
 * sql("""
 *   SELECT * FROM
 *     (SELECT key AS k from df1) t1
 *   INNER JOIN
 *     (SELECT key AS k from df2) t2
 *   ON t1.k = t2.k
 * """).explain
 *
 * == Physical Plan ==
 * *SortMergeJoin [k#56L], [k#57L], Inner
 * :- *Sort [k#56L ASC NULLS FIRST], false, 0
 * :  +- Exchange hashpartitioning(k#56L, 200) // <--- Unnecessary shuffle operation
 * :     +- *Project [key#39L AS k#56L]
 * :        +- Exchange hashpartitioning(key#39L, 200)
 * :           +- *Project [id#36L AS key#39L]
 * :              +- *Range (0, 10, step=1, splits=Some(4))
 * +- *Sort [k#57L ASC NULLS FIRST], false, 0
 *    +- ReusedExchange [k#57L], Exchange hashpartitioning(k#56L, 200)
 */
trait AliasAwareOutputPartitioning extends UnaryExecNode {

  protected def outputExpressions: Seq[NamedExpression]

  private def hasAlias(exprs: Seq[NamedExpression]): Boolean =
    exprs.exists(_.collectFirst { case _: Alias => true }.isDefined)

  final override def outputPartitioning: Partitioning = {
    if (hasAlias(outputExpressions)) {
      // Returns the valid `Partitioning`s for the node w.r.t its output and its expressions.
      child.outputPartitioning match {
        case partitioning: Expression =>
          val exprToEquiv = partitioning.references.map { attr =>
            attr -> outputExpressions.filter(e =>
              CleanupAliases.trimAliases(e).semanticEquals(attr))
          }.filterNot { case (attr, exprs) =>
            exprs.size == 1 && exprs.forall(_ == attr)
          }
          val initValue = partitioning match {
            case PartitioningCollection(partitionings) => partitionings
            case other => Seq(other)
          }
          val validPartitionings = exprToEquiv.foldLeft(initValue) {
            case (partitionings, (toReplace, equivalents)) =>
              if (equivalents.isEmpty) {
                partitionings.map(_.pruneInvalidAttribute(toReplace))
              } else {
                partitionings.flatMap {
                  case p: Expression if p.references.contains(toReplace) =>
                    equivalents.map { equiv =>
                      p.transformDown {
                        case e if e == toReplace => equiv.toAttribute
                      }.asInstanceOf[Partitioning]
                    }
                  case other => Seq(other)
                }
              }
          }.distinct
          if (validPartitionings.size == 1) {
            validPartitionings.head
          } else {
            validPartitionings.filterNot(_.isInstanceOf[UnknownPartitioning]) match {
              case Seq() => PartitioningCollection(validPartitionings)
              case Seq(knownPartitioning) => knownPartitioning
              case knownPartitionings => PartitioningCollection(knownPartitionings)
            }

          }
        case other => other
      }
    } else {
      child.outputPartitioning
    }
  }
}
