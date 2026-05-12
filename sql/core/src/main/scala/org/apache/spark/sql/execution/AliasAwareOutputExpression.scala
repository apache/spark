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

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ExpressionSet}
import org.apache.spark.sql.catalyst.plans.{AliasAwareOutputExpression, AliasAwareQueryOutputOrdering}
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.catalyst.trees.MultiTransform

/**
 * A trait that handles aliases in the `outputExpressions` to produce `outputPartitioning` that
 * satisfies distribution requirements.
 */
trait PartitioningPreservingUnaryExecNode extends UnaryExecNode
  with AliasAwareOutputExpression {
  final override def outputPartitioning: Partitioning = {
    val (keyedPartitionings, otherPartitionings) =
      flattenPartitioning(child.outputPartitioning).partition(_.isInstanceOf[KeyedPartitioning])

    val projectedKPs =
      projectKeyedPartitionings(keyedPartitionings.map(_.asInstanceOf[KeyedPartitioning]))
    val projectedOthers = projectOtherPartitionings(otherPartitionings)

    (projectedKPs ++ projectedOthers).take(aliasCandidateLimit) match {
      case Seq() => UnknownPartitioning(child.outputPartitioning.numPartitions)
      case Seq(p) => p
      case ps => PartitioningCollection(ps)
    }
  }

  /**
   * Projects non-[[KeyedPartitioning]] partitionings through the current node's output expressions.
   *
   * With aliases, each partitioning expression is substituted with all possible alias combinations;
   * without aliases, partitionings whose expressions reference attributes outside the output are
   * dropped.
   */
  private def projectOtherPartitionings(
      partitionings: Seq[Partitioning]): LazyList[Partitioning] = {
    if (hasAlias) {
      partitionings.to(LazyList).flatMap {
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
            .asInstanceOf[LazyList[Partitioning]]
        case o => LazyList(o)
      }
    } else {
      // Filter valid partitionings (only reference output attributes of the current plan node)
      val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
      partitionings.to(LazyList).filter {
        case e: Expression => e.references.subsetOf(outputSet)
        case _ => true
      }
    }
  }

  /**
   * Projects all input [[KeyedPartitioning]]s through the current node's output expressions.
   *
   * For each expression position (0..N-1), collects the unique expressions at that position across
   * all input KPs, projects each through the output aliases, and unions the alternatives.
   * Positions with at least one alternative are projectable; their count determines the maximum
   * achievable granularity. Positions that cannot be expressed in the output are dropped.
   *
   * The resulting [[KeyedPartitioning]]s are the cross-product of the per-position alternatives
   * restricted to the projectable positions. All share the same `partitionKeys` object (projected
   * to the same subset of positions), preserving the invariant required by [[GroupPartitionsExec]].
   */
  private def projectKeyedPartitionings(
      kps: Seq[KeyedPartitioning]): LazyList[KeyedPartitioning] = {
    if (kps.isEmpty) return LazyList.empty
    val numPositions = kps.head.expressions.length

    val alternativesPerPosition: IndexedSeq[LazyList[Expression]] =
      if (hasAlias) {
        // For each position, gather unique expressions across all KPs (ExpressionSet deduplicates
        // semantically equal expressions, e.g. the same join-key column shared by both KP sides)
        // and project each through the output aliases.
        (0 until numPositions).map { i =>
          val seen = mutable.Set.empty[Expression]
          ExpressionSet(kps.map(_.expressions(i))).to(LazyList).flatMap { expr =>
            projectExpression(expr).filter(e => seen.add(e.canonicalized))
          }
        }
      } else {
        // No aliases: filter out non-projectable expressions first, then deduplicate.
        val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
        (0 until numPositions).map { i =>
          ExpressionSet(kps.collect {
            case kp if kp.expressions(i).references.subsetOf(outputSet) => kp.expressions(i)
          }).to(LazyList)
        }
      }

    // Non-empty positions define the maximum achievable granularity.
    val projectablePositions =
      (0 until numPositions).filter(i => alternativesPerPosition(i).nonEmpty)

    if (projectablePositions.isEmpty) return LazyList.empty

    // All input KPs share the same partitionKeys by invariant; use the first as the key source.
    val keySource = kps.head
    val sharedKeys =
      if (projectablePositions.length == numPositions) keySource.partitionKeys
      else keySource.projectKeys(projectablePositions)._2

    val isGrouped = sharedKeys.distinct.size == sharedKeys.size
    // A KP is narrowed if this node drops positions, or if the input KPs were already narrowed
    // (i.e. came from a finer-grained partitioning). The flag must be sticky: a subsequent
    // PartitioningPreservingUnaryExecNode that passes all positions through would otherwise
    // recompute isNarrowed=false, silently dropping the protection.
    val isNarrowed = projectablePositions.length < numPositions || keySource.isNarrowed

    // Cross-product the per-position alternatives to produce all concrete KPs.
    // Note: generateCartesianProduct expects thunks () => Seq[T], but wrapping LazyLists in thunks
    // here is not strictly necessary since they are already lazy -- we do it only to match the API.
    // No deduplication is needed here: per-position alternatives are already canonically distinct,
    // so all cross-product combinations are distinct by construction.
    MultiTransform.generateCartesianProduct(
      projectablePositions.map(i => () => alternativesPerPosition(i)))
      .map(projectedExprs =>
        new KeyedPartitioning(projectedExprs, sharedKeys, isGrouped, isNarrowed))
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
