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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.{AliasAwareOutputExpression, AliasAwareQueryOutputOrdering}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection, UnknownPartitioning}

/**
 * A trait that handles aliases in the `outputExpressions` to produce `outputPartitioning` that
 * satisfies distribution requirements.
 */
trait PartitioningPreservingUnaryExecNode extends UnaryExecNode
  with AliasAwareOutputExpression {
  final override def outputPartitioning: Partitioning = {
    val partitionings: Seq[Partitioning] = if (hasAlias) {
      flattenPartitioning(child.outputPartitioning).iterator.flatMap {
        case e: Expression =>
          // We need unique partitionings but if the input partitioning is
          // `HashPartitioning(Seq(id + id))` and we have `id -> a` and `id -> b` aliases then after
          // the projection we have 4 partitionings:
          // `HashPartitioning(Seq(a + a))`, `HashPartitioning(Seq(a + b))`,
          // `HashPartitioning(Seq(b + a))`, `HashPartitioning(Seq(b + b))`, but
          // `HashPartitioning(Seq(a + b))` is the same as `HashPartitioning(Seq(b + a))`.
          val partitioningSet = mutable.Set.empty[Expression]
          projectExpression(e)
            .filter(e => partitioningSet.add(e.canonicalized))
            .take(aliasCandidateLimit)
            .asInstanceOf[LazyList[Partitioning]]
        case o => Seq(o)
      }.take(aliasCandidateLimit).toSeq
    } else {
      // Filter valid partitiongs (only reference output attributes of the current plan node)
      val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
      flattenPartitioning(child.outputPartitioning).filter {
        case e: Expression => e.references.subsetOf(outputSet)
        case _ => true
      }
    }
    partitionings match {
      case Seq() => UnknownPartitioning(child.outputPartitioning.numPartitions)
      case Seq(p) => p
      case ps => PartitioningCollection(ps)
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

trait OrderPreservingUnaryExecNode
  extends UnaryExecNode with AliasAwareQueryOutputOrdering[SparkPlan]
