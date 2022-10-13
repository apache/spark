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

import org.apache.spark.sql.ExperimentalMethods
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions
import org.apache.spark.sql.execution.datasources.SchemaPruning
import org.apache.spark.sql.execution.datasources.v2.{V2ScanRelationPushDown, V2Writes}
import org.apache.spark.sql.execution.dynamicpruning.{CleanupDynamicPruningFilters, PartitionPruning}
import org.apache.spark.sql.execution.python.{ExtractGroupingPythonUDFFromAggregate, ExtractPythonUDFFromAggregate, ExtractPythonUDFs}

class SparkOptimizer(
    catalogManager: CatalogManager,
    catalog: SessionCatalog,
    experimentalMethods: ExperimentalMethods)
  extends Optimizer(catalogManager) {

  override def earlyScanPushDownRules: Seq[Rule[LogicalPlan]] =
    // TODO: move SchemaPruning into catalyst
    SchemaPruning :: V2ScanRelationPushDown :: V2Writes :: PruneFileSourcePartitions :: Nil

  override def defaultBatches: Seq[Batch] = (preOptimizationBatches ++ super.defaultBatches :+
    Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog)) :+
    Batch("PartitionPruning", Once,
      PartitionPruning) :+
    Batch("InjectRuntimeFilter", FixedPoint(1),
      InjectRuntimeFilter,
      RewritePredicateSubquery) :+
    Batch("Pushdown Filters from PartitionPruning", fixedPoint,
      PushDownPredicates) :+
    Batch("Cleanup filters that cannot be pushed down", Once,
      CleanupDynamicPruningFilters,
      PruneFilters)) ++
    postHocOptimizationBatches :+
    Batch("Extract Python UDFs", Once,
      ExtractPythonUDFFromJoinCondition,
      // `ExtractPythonUDFFromJoinCondition` can convert a join to a cartesian product.
      // Here, we rerun cartesian product check.
      CheckCartesianProducts,
      ExtractPythonUDFFromAggregate,
      // This must be executed after `ExtractPythonUDFFromAggregate` and before `ExtractPythonUDFs`.
      ExtractGroupingPythonUDFFromAggregate,
      ExtractPythonUDFs,
      // The eval-python node may be between Project/Filter and the scan node, which breaks
      // column pruning and filter push-down. Here we rerun the related optimizer rules.
      ColumnPruning,
      PushPredicateThroughNonJoin,
      RemoveNoopOperators) :+
    Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)

  override def nonExcludableRules: Seq[String] = super.nonExcludableRules :+
    ExtractPythonUDFFromJoinCondition.ruleName :+
    ExtractPythonUDFFromAggregate.ruleName :+ ExtractGroupingPythonUDFFromAggregate.ruleName :+
    ExtractPythonUDFs.ruleName :+
    V2ScanRelationPushDown.ruleName :+
    V2Writes.ruleName

  /**
   * Optimization batches that are executed before the regular optimization batches (also before
   * the finish analysis batch).
   */
  def preOptimizationBatches: Seq[Batch] = Nil

  /**
   * Optimization batches that are executed after the regular optimization batches, but before the
   * batch executing the [[ExperimentalMethods]] optimizer rules. This hook can be used to add
   * custom optimizer batches to the Spark optimizer.
   *
   * Note that 'Extract Python UDFs' batch is an exception and ran after the batches defined here.
   */
   def postHocOptimizationBatches: Seq[Batch] = Nil
}
