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

package org.apache.spark.sql.execution.bucketing

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

trait BucketJoinHelper {
  @tailrec
  final protected def hasScanOperation(plan: SparkPlan): Boolean = plan match {
    case f: FilterExec => hasScanOperation(f.child)
    case p: ProjectExec => hasScanOperation(p.child)
    case j: BroadcastHashJoinExec =>
      if (j.buildSide == BuildLeft) hasScanOperation(j.right) else hasScanOperation(j.left)
    case j: BroadcastNestedLoopJoinExec =>
      if (j.buildSide == BuildLeft) hasScanOperation(j.right) else hasScanOperation(j.left)
    case f: FileSourceScanExec => f.relation.bucketSpec.nonEmpty
    case _ => false
  }

  /**
   * The join keys should match with expressions for output partitioning. Note that
   * the ordering does not matter because it will be handled in `EnsureRequirements`.
   */
  protected def satisfiesOutputPartitioning(
      keys: Seq[Expression],
      partitioning: Partitioning): Boolean = {
    partitioning match {
      case HashPartitioning(exprs, _) if exprs.length == keys.length =>
        exprs.forall(e => keys.exists(_.semanticEquals(e)))
      case _ => false
    }
  }
}
