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
import org.apache.spark.sql.catalyst.analysis.RewriteCollationJoin
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.datasources.{FillStaticPartitions, PruneFileSourcePartitions, SchemaPruning, V1Writes}
import org.apache.spark.sql.execution.datasources.v2.{GroupBasedRowLevelOperationScanPlanning, OptimizeMetadataOnlyDeleteFromTable, V2ScanPartitioningAndOrdering, V2ScanRelationPushDown, V2Writes}
import org.apache.spark.sql.execution.dynamicpruning.{CleanupDynamicPruningFilters, PartitionPruning, RowLevelOperationRuntimeGroupFiltering}
import org.apache.spark.sql.execution.python.{ExtractGroupingPythonUDFFromAggregate, ExtractPythonUDFFromAggregate, ExtractPythonUDFs, ExtractPythonUDTFs}

class SparkOptimizer(
    catalogManager: CatalogManager,
    catalog: SessionCatalog,
    experimentalMethods: ExperimentalMethods)
  extends Optimizer(catalogManager) {

  override def earlyScanPushDownRules: Seq[Rule[LogicalPlan]] =
    // TODO: move SchemaPruning into catalyst
    Seq(SchemaPruning) :+
      GroupBasedRowLevelOperationScanPlanning :+
      V1Writes :+
      V2ScanRelationPushDown :+
      V2ScanPartitioningAndOrdering :+
      V2Writes :+
      PruneFileSourcePartitions

  override def preCBORules: Seq[Rule[LogicalPlan]] =
    OptimizeMetadataOnlyDeleteFromTable :: Nil

  override def defaultBatches: Seq[Batch] = (preOptimizationBatches ++ super.defaultBatches :+
    Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog)) :+
    Batch("PartitionPruning", Once,
      PartitionPruning,
      // We can't run `OptimizeSubqueries` in this batch, as it will optimize the subqueries
      // twice which may break some optimizer rules that can only be applied once. The rule below
      // only invokes `OptimizeSubqueries` to optimize newly added subqueries.
      new RowLevelOperationRuntimeGroupFiltering(OptimizeSubqueries)) :+
    Batch("InjectRuntimeFilter", FixedPoint(1),
      InjectRuntimeFilter) :+
    Batch("MergeScalarSubqueries", Once,
      MergeScalarSubqueries,
      RewriteDistinctAggregates) :+
    Batch("Pushdown Filters from PartitionPruning", fixedPoint,
      PushDownPredicates) :+
    Batch("Cleanup filters that cannot be pushed down", Once,
      CleanupDynamicPruningFilters,
      // cleanup the unnecessary TrueLiteral predicates
      BooleanSimplification,
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
      ExtractPythonUDTFs,
      // The eval-python node may be between Project/Filter and the scan node, which breaks
      // column pruning and filter push-down. Here we rerun the related optimizer rules.
      ColumnPruning,
      LimitPushDown,
      PushPredicateThroughNonJoin,
      PushProjectionThroughLimit,
      RemoveNoopOperators) :+
    Batch("Infer window group limit", Once,
      InferWindowGroupLimit,
      LimitPushDown,
      LimitPushDownThroughWindow,
      EliminateLimits,
      ConstantFolding) :+
    Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*) :+
    Batch("Replace CTE with Repartition", Once, ReplaceCTERefWithRepartition) :+
    Batch("RewriteCollationJoin", Once, RewriteCollationJoin) :+
    Batch("FillStaticPartitions", Once, FillStaticPartitions)

  override def nonExcludableRules: Seq[String] = super.nonExcludableRules :+
    ExtractPythonUDFFromJoinCondition.ruleName :+
    ExtractPythonUDFFromAggregate.ruleName :+ ExtractGroupingPythonUDFFromAggregate.ruleName :+
    ExtractPythonUDFs.ruleName :+
    GroupBasedRowLevelOperationScanPlanning.ruleName :+
    V2ScanRelationPushDown.ruleName :+
    V2ScanPartitioningAndOrdering.ruleName :+
    V2Writes.ruleName :+
    ReplaceCTERefWithRepartition.ruleName

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
