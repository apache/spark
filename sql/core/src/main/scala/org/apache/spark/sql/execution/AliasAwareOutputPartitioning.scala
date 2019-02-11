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
 * spark.range(10).selectExpr("id AS key", "0").repartition($"key").createTempView("df1")
 * spark.range(10).selectExpr("id AS key", "0").repartition($"key").createTempView("df2")
 * sql("set spark.sql.autoBroadcastJoinThreshold=-1")
 * sql("""
 *   SELECT * FROM
 *     (SELECT key AS k from df1) t1
 *   INNER JOIN
 *     (SELECT key AS k from df2) t2
 *   ON t1.k = t2.k
 * """).explain
 *
 * == Physical Plan ==
 * *SortMergeJoin [k#21L], [k#22L], Inner
 * :- *Sort [k#21L ASC NULLS FIRST], false, 0
 * :  +- Exchange hashpartitioning(k#21L, 200) // <--- Unnecessary shuffle operation
 * :     +- *Project [key#2L AS k#21L]
 * :        +- Exchange hashpartitioning(key#2L, 200)
 * :           +- *Project [id#0L AS key#2L]
 * :              +- *Range (0, 10, step=1, splits=Some(4))
 * +- *(4) Sort [k#22L ASC NULLS FIRST], false, 0
 *    +- *(4) Project [key#8L AS k#22L]
 *       +- ReusedExchange [key#8L], Exchange hashpartitioning(key#2L, 200)
 */
trait AliasAwareOutputPartitioning extends UnaryExecNode {

  /**
   * `Seq` of `Expression`s which define the ouput of the node.
   */
  protected def outputExpressions: Seq[NamedExpression]

  /**
   * Returns the valid `Partitioning`s for the node w.r.t its output and its expressions.
   */
  final override def outputPartitioning: Partitioning = {
    child.outputPartitioning match {
      case partitioning: Expression =>
        // Creates a sequence of tuples where the first element is an `Attribute` referenced in the
        // partitioning expression of the child and the second is a sequence of all its aliased
        // occurrences in the node output. If there is no occurrence of an attribute in the output,
        // the second element of the tuple for it will be an empty `Seq`. If the attribute,
        // instead, is only present as is in the output, there will be no entry for it.
        // Eg. if the partitioning is RangePartitioning('a) and the node output is "a, 'a as a1,
        // a' as a2", then exprToEquiv will contain the tuple ('a, Seq('a, 'a as a1, 'a as a2)).
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
        // Replace all the aliased expressions detected earlier with all their corresponding
        // occurrences. This may produce many valid partitioning expressions from a single one.
        // Eg. in the example above, this would produce a `Seq` of 3 `RangePartitioning`, namely:
        // `RangePartitioning('a)`, `RangePartitioning('a1)`, `RangePartitioning('a2)`.
        val validPartitionings = exprToEquiv.foldLeft(initValue) {
          case (partitionings, (toReplace, equivalents)) =>
            if (equivalents.isEmpty) {
              // Remove from the partitioning expression the attribute which is not present in the
              // node output
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
  }
}
