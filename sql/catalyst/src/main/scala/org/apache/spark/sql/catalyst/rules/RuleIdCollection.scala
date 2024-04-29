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

package org.apache.spark.sql.catalyst.rules

import scala.collection.mutable

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.util.Utils

// Represent unique rule ids for rules that are invoked multiple times.
case class RuleId(id: Int) {
  // Currently, there are more than 128 but less than 192 rules needing an id. However, the
  // requirement can be relaxed when we have more such rules. Note that increasing the max id can
  // result in increased memory consumption from every TreeNode.
  require(id >= -1 && id < 192)
}

// Unknown rule id which does not prune tree traversals. It is used as the default rule id for
// tree transformation functions.
object UnknownRuleId extends RuleId(-1)

// A collection of rules that use rule ids to prune tree traversals.
object RuleIdCollection {

  // The rules listed here need a rule id. Typically, rules that are in a fixed point batch or
  // invoked multiple times by Analyzer/Optimizer/Planner need a rule id to prune unnecessary
  // tree traversals in the transform function family. Note that those rules should not depend on
  // a changing, external state. Rules here are in alphabetical order.
  private var rulesNeedingIds: Seq[String] = {
      // Catalyst Analyzer rules
      "org.apache.spark.sql.catalyst.analysis.Analyzer$AddMetadataColumns" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ExtractGenerator" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ExtractWindowExpressions" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$GlobalAggregates" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveAggAliasInGroupBy" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveAggregateFunctions" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveAliases" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveBinaryArithmetic" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveEncodersInUDF" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveFunctions" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveGenerate" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveGroupingAnalytics" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveHigherOrderFunctions" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveInsertInto" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveNaturalAndUsingJoin" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveNewInstance" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveOrdinalInOrderByAndGroupBy" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveOutputRelation" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolvePivot" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRandomSeed" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveReferences" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveSubquery" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveSubqueryColumnAliases" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveTables" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveTempViews" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveUnpivot" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveUpCast" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveUserSpecifiedColumns" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveWindowFrame" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveWindowOrder" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$WindowsSubstitution" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$WrapLateralColumnAliasReference" ::
      "org.apache.spark.sql.catalyst.analysis.AnsiTypeCoercion$AnsiCombinedTypeCoercionRule" ::
      "org.apache.spark.sql.catalyst.analysis.ApplyCharTypePadding" ::
      "org.apache.spark.sql.catalyst.analysis.DeduplicateRelations" ::
      "org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases" ::
      "org.apache.spark.sql.catalyst.analysis.EliminateUnions" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveDefaultColumns" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveExpressionsWithNamePlaceholders" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveHints$ResolveCoalesceHints" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveHints$ResolveJoinStrategyHints" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveGroupByAll" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveInlineTables" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveLambdaVariables" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveLateralColumnAliasReference" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveOrderByAll" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveRowLevelCommandAssignments" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveSetVariable" ::
      "org.apache.spark.sql.catalyst.analysis.SubstituteExecuteImmediate" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveTableSpec" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveTimeZone" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveUnion" ::
      "org.apache.spark.sql.catalyst.analysis.ResolveWindowTime" ::
      "org.apache.spark.sql.catalyst.analysis.SessionWindowing" ::
      "org.apache.spark.sql.catalyst.analysis.SubstituteUnresolvedOrdinals" ::
      "org.apache.spark.sql.catalyst.analysis.TimeWindowing" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$CombinedTypeCoercionRule" ::
      "org.apache.spark.sql.catalyst.analysis.UpdateOuterReferences" ::
      "org.apache.spark.sql.catalyst.analysis.UpdateAttributeNullability" ::
      // Catalyst Optimizer rules
      "org.apache.spark.sql.catalyst.optimizer.BooleanSimplification" ::
      "org.apache.spark.sql.catalyst.optimizer.CollapseProject" ::
      "org.apache.spark.sql.catalyst.optimizer.CollapseRepartition" ::
      "org.apache.spark.sql.catalyst.optimizer.CollapseWindow" ::
      "org.apache.spark.sql.catalyst.optimizer.ColumnPruning" ::
      "org.apache.spark.sql.catalyst.optimizer.CombineConcats" ::
      "org.apache.spark.sql.catalyst.optimizer.CombineFilters" ::
      "org.apache.spark.sql.catalyst.optimizer.CombineTypedFilters" ::
      "org.apache.spark.sql.catalyst.optimizer.CombineUnions" ::
      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding" ::
      "org.apache.spark.sql.catalyst.optimizer.ConstantPropagation" ::
      "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation" ::
      "org.apache.spark.sql.catalyst.optimizer.CostBasedJoinReorder" ::
      "org.apache.spark.sql.catalyst.optimizer.DecimalAggregates" ::
      "org.apache.spark.sql.catalyst.optimizer.EliminateAggregateFilter" ::
      "org.apache.spark.sql.catalyst.optimizer.EliminateLimits" ::
      "org.apache.spark.sql.catalyst.optimizer.EliminateMapObjects" ::
      "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.EliminateSerialization" ::
      "org.apache.spark.sql.catalyst.optimizer.EliminateWindowPartitions" ::
      "org.apache.spark.sql.catalyst.optimizer.InferWindowGroupLimit" ::
      "org.apache.spark.sql.catalyst.optimizer.InsertMapSortInGroupingExpressions" ::
      "org.apache.spark.sql.catalyst.optimizer.LikeSimplification" ::
      "org.apache.spark.sql.catalyst.optimizer.LimitPushDown" ::
      "org.apache.spark.sql.catalyst.optimizer.LimitPushDownThroughWindow" ::
      "org.apache.spark.sql.catalyst.optimizer.NullDownPropagation" ::
      "org.apache.spark.sql.catalyst.optimizer.NullPropagation" ::
      "org.apache.spark.sql.catalyst.optimizer.ObjectSerializerPruning" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeCsvJsonExprs" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeIn" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeJoinCondition" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeRand" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeOneRowPlan" ::
      "org.apache.spark.sql.catalyst.optimizer.Optimizer$OptimizeSubqueries" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeRepartition" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeWindowFunctions" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeUpdateFields"::
      "org.apache.spark.sql.catalyst.optimizer.PropagateEmptyRelation" ::
      "org.apache.spark.sql.catalyst.optimizer.PruneFilters" ::
      "org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.PushExtraPredicateThroughJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.PushFoldableIntoBranches" ::
      "org.apache.spark.sql.catalyst.optimizer.PushLeftSemiLeftAntiThroughJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.ReassignLambdaVariableID" ::
      "org.apache.spark.sql.catalyst.optimizer.RemoveDispensableExpressions" ::
      "org.apache.spark.sql.catalyst.optimizer.RemoveLiteralFromGroupExpressions" ::
      "org.apache.spark.sql.catalyst.optimizer.GenerateOptimization" ::
      "org.apache.spark.sql.catalyst.optimizer.RemoveNoopOperators" ::
      "org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAggregates" ::
      "org.apache.spark.sql.catalyst.optimizer.RemoveRepetitionFromGroupExpressions" ::
      "org.apache.spark.sql.catalyst.optimizer.ReorderAssociativeOperator" ::
      "org.apache.spark.sql.catalyst.optimizer.ReorderJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithAntiJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithFilter" ::
      "org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate" ::
      "org.apache.spark.sql.catalyst.optimizer.ReplaceNullWithFalseInPredicate" ::
      "org.apache.spark.sql.catalyst.optimizer.ReplaceIntersectWithSemiJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.RewriteExceptAll" ::
      "org.apache.spark.sql.catalyst.optimizer.RewriteIntersectAll" ::
      "org.apache.spark.sql.catalyst.optimizer.RewriteAsOfJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.SimplifyBinaryComparison" ::
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCaseConversionExpressions" ::
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts" ::
      "org.apache.spark.sql.catalyst.optimizer.SimplifyConditionals" ::
      "org.apache.spark.sql.catalyst.optimizer.SimplifyExtractValueOps" ::
      "org.apache.spark.sql.catalyst.optimizer.TransposeWindow" ::
      "org.apache.spark.sql.catalyst.optimizer.EvalInlineTables" ::
      "org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison" ::  Nil
  }

  if(Utils.isTesting) {
    rulesNeedingIds = rulesNeedingIds ++ {
      // In the production code path, the following rules are run in CombinedTypeCoercionRule, and
      // hence we only need to add them for unit testing.
      "org.apache.spark.sql.catalyst.analysis.AnsiTypeCoercion$PromoteStringLiterals" ::
      "org.apache.spark.sql.catalyst.analysis.AnsiTypeCoercion$DateTimeOperations" ::
      "org.apache.spark.sql.catalyst.analysis.AnsiTypeCoercion$GetDateFieldOperations" ::
      "org.apache.spark.sql.catalyst.analysis.DecimalPrecision" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercion$BooleanEquality" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercion$DateTimeOperations" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$CaseWhenCoercion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$ConcatCoercion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$Division" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$EltCoercion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$FunctionArgumentConversion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$IfCoercion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$ImplicitTypeCasts" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$InConversion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$IntegralDivision" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$MapZipWithCoercion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercion$PromoteStrings" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$StackCoercion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$StringLiteralCoercion" ::
      "org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$WindowFrameCoercion" :: Nil
    }
  }

  // Maps rule names to ids. Rule ids are continuous natural numbers starting from 0.
  private val ruleToId = new mutable.HashMap[String, RuleId]

  // The total number of rules with ids.
  val NumRules: Int = {
    var id = 0
    rulesNeedingIds.foreach(ruleName => {
      ruleToId.put(ruleName, RuleId(id))
      id = id + 1
    })
    id
  }

  // Return the rule id for a rule name.
  def getRuleId(ruleName: String): RuleId = {
    val ruleIdOpt = ruleToId.get(ruleName)
    // Please add the rule name to `rulesWithIds` if rule id is not found.
    if (ruleIdOpt.isEmpty) {
      throw QueryExecutionErrors.ruleIdNotFoundForRuleError(ruleName)
    }
    ruleIdOpt.get
  }
}
