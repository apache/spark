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

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionSet}
import org.apache.spark.sql.catalyst.plans.{AliasAwareOutputExpression, AliasAwareQueryOutputOrdering}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection, UnknownPartitioning}

/**
 * A trait that handles aliases in the `outputExpressions` to produce `outputPartitioning` that
 * satisfies distribution requirements.
 */
trait AliasAwareOutputPartitioning extends UnaryExecNode
  with AliasAwareOutputExpression {

  final override def outputPartitioning: Partitioning = {
    val normalizedOutputPartitioning = if (hasAlias) {
      child.outputPartitioning match {
        case e: Expression =>
          val normalized = normalizeExpression(e)
          if (normalized.isEmpty) {
            UnknownPartitioning(child.outputPartitioning.numPartitions)
          } else if (normalized.size == 1) {
            normalized.head.asInstanceOf[Partitioning]
          } else {
            PartitioningCollection(normalized.asInstanceOf[Seq[Partitioning]])
          }
        case other => other
      }
    } else {
      child.outputPartitioning
    }

    val (partitionWithExpr, other) = flattenPartitioning(normalizedOutputPartitioning).filter {
      case e: Expression => e.references.subsetOf(outputSet)
      case _ => true
    }.partition(_.isInstanceOf[Expression])
    val pruned = ExpressionSet(partitionWithExpr.asInstanceOf[Seq[Expression]])
    (pruned.toSeq.asInstanceOf[Seq[Partitioning]] ++ other) match {
      case Seq() => UnknownPartitioning(child.outputPartitioning.numPartitions)
      case Seq(singlePartitioning) => singlePartitioning
      case seqWithMultiplePartitionings => PartitioningCollection(seqWithMultiplePartitionings)
    }
  }

  private def flattenPartitioning(partitioning: Partitioning): Seq[Partitioning] = {
    partitioning match {
      case PartitioningCollection(childPartitionings) =>
        childPartitionings.flatMap(flattenPartitioning)
      case rest =>
        rest +: Nil
    }
  }
}

trait AliasAwareOutputOrdering extends UnaryExecNode with AliasAwareQueryOutputOrdering[SparkPlan]
