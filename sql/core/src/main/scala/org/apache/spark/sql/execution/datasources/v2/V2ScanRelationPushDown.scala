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

package org.apache.spark.sql.execution.datasources.v2

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.internal.LogKeys.{AGGREGATE_FUNCTIONS, COLUMN_NAMES, GROUP_BY_EXPRS, JOIN_CONDITION, JOIN_TYPE, POST_SCAN_FILTERS, PUSHED_FILTERS, RELATION_NAME, RELATION_OUTPUT}
import org.apache.spark.sql.catalyst.expressions.{aggregate, Alias, And, Attribute, AttributeMap, AttributeReference, AttributeSet, Cast, Expression, ExprId, IntegerLiteral, Literal, NamedExpression, PredicateHelper, ProjectionOverSchema, SortOrder, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ScanOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LeafNode, Limit, LimitAndOffset, LocalLimit, LogicalPlan, Offset, OffsetAndLimit, Project, Sample, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.expressions.{SortOrder => V2SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Avg, Count, CountStar, Max, Min, Sum}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownJoin, SupportsPushDownVariantExtractions, V1Scan, VariantExtraction}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, VariantInRelation}
import org.apache.spark.sql.internal.connector.VariantExtractionImpl
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils._
import org.apache.spark.util.ArrayImplicits._

object V2ScanRelationPushDown extends Rule[LogicalPlan] with PredicateHelper {
  import DataSourceV2Implicits._

  def apply(plan: LogicalPlan): LogicalPlan = {
    val pushdownRules = Seq[LogicalPlan => LogicalPlan] (
      createScanBuilder,
      pushDownSample,
      pushDownFilters,
      pushDownJoin,
      pushDownAggregates,
      pushDownVariants,
      pushDownLimitAndOffset,
      buildScanWithPushedAggregate,
      buildScanWithPushedJoin,
      buildScanWithPushedVariants,
      pruneColumns)

    pushdownRules.foldLeft(plan) { (newPlan, pushDownRule) =>
      pushDownRule(newPlan)
    }
  }

  private def createScanBuilder(plan: LogicalPlan) = plan.transform {
    case r: DataSourceV2Relation =>
      ScanBuilderHolder(r.output, r, r.table.asReadable.newScanBuilder(r.options))
  }

  private def pushDownFilters(plan: LogicalPlan) = plan.transform {
    // update the scan builder with filter push down and return a new plan with filter pushed
    case Filter(condition, sHolder: ScanBuilderHolder) =>
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters =
        DataSourceStrategy.normalizeExprs(filters, sHolder.relation.output)
      val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils.pushFilters(
        sHolder.builder, normalizedFiltersWithoutSubquery)
      val pushedFiltersStr = if (pushedFilters.isLeft) {
        pushedFilters.swap
          .getOrElse(throw new NoSuchElementException("The left node doesn't have pushedFilters"))
          .mkString(", ")
      } else {
        sHolder.pushedPredicates = pushedFilters
          .getOrElse(throw new NoSuchElementException("The right node doesn't have pushedFilters"))
        sHolder.pushedPredicates.mkString(", ")
      }

      val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery

      logInfo(
        log"""
            |Pushing operators to ${MDC(RELATION_NAME, sHolder.relation.name)}
            |Pushed Filters: ${MDC(PUSHED_FILTERS, pushedFiltersStr)}
            |Post-Scan Filters: ${MDC(POST_SCAN_FILTERS, postScanFilters.mkString(","))}
           """.stripMargin)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      filterCondition.map(Filter(_, sHolder)).getOrElse(sHolder)
  }

  def pushDownJoin(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    // Join can be attempted to be pushed down only if left and right side of join are
    // compatible (same data source, for example). Also, another requirement is that if
    // there are projections between Join and ScanBuilderHolder, these projections need to be
    // AttributeReferences. We could probably support Alias as well, but this should be on
    // TODO list.
    // Alias can exist between Join and sHolder node because the query below is not valid:
    // SELECT * FROM
    // (SELECT * FROM tbl t1 JOIN tbl2 t2) p
    // JOIN
    // (SELECT * FROM tbl t3 JOIN tbl3 t4) q
    // ON p.t1.col = q.t3.col (this is not possible)
    // It's because there are duplicated columns in both sides of top level join and it's not
    // possible to fully qualified the column names in condition. Therefore, query should be
    // rewritten so that each of the outputs of child joins are aliased, so there would be a
    // projection with aliases between top level join and scanBuilderHolder (that has pushed
    // child joins).
    case node @ Join(
      PhysicalOperation(
        leftProjections,
        Nil,
        leftHolder @ ScanBuilderHolder(_, _, lBuilder: SupportsPushDownJoin)
      ),
      PhysicalOperation(
        rightProjections,
        Nil,
        rightHolder @ ScanBuilderHolder(_, _, rBuilder: SupportsPushDownJoin)
      ),
      joinType,
      condition,
    _) if conf.dataSourceV2JoinPushdown &&
        // We do not support pushing down anything besides AttributeReference.
        leftProjections.forall(_.isInstanceOf[AttributeReference]) &&
        rightProjections.forall(_.isInstanceOf[AttributeReference]) &&
        // Cross joins are not supported because they increase the amount of data.
        condition.isDefined &&
        lBuilder.isOtherSideCompatibleForJoin(rBuilder) =>
      // Process left and right columns in original order
      val (leftSideRequiredColumnsWithAliases, rightSideRequiredColumnsWithAliases) =
        generateColumnAliasesForDuplicatedName(
          getRequiredColumnNames(leftProjections, leftHolder),
          getRequiredColumnNames(rightProjections, rightHolder))

      // Create the AttributeMap that holds (Attribute -> Attribute with up to date name) mapping.
      val pushedJoinOutputMap = AttributeMap[Expression](
        node.output
          .zip(leftSideRequiredColumnsWithAliases ++ rightSideRequiredColumnsWithAliases)
          .collect {
            case (attr, columnWithAlias) =>
              if (columnWithAlias.alias() != null) {
                (attr, attr.withName(columnWithAlias.alias()))
              } else {
                (attr, attr.withName(columnWithAlias.colName()))
              }
          }
          .toMap
      )

      // Reuse the previously calculated map to update the condition with attributes
      // with up-to-date names
      val normalizedCondition = condition.map { e =>
        DataSourceStrategy.normalizeExprs(
          Seq(e),
          (leftHolder.output ++ rightHolder.output).map { a =>
            pushedJoinOutputMap.getOrElse(a, a).asInstanceOf[AttributeReference]
          }
        ).head
      }

      val translatedCondition =
        normalizedCondition.flatMap(DataSourceV2Strategy.translateFilterV2(_))
      val translatedJoinType = DataSourceStrategy.translateJoinType(joinType)

      logInfo(log"DSv2 Join pushdown - translated join condition " +
        log"${MDC(JOIN_CONDITION, translatedCondition)}")
      logInfo(log"DSv2 Join pushdown - translated join type " +
        log"${MDC(JOIN_TYPE, translatedJoinType)}")

      logInfo(log"DSv2 Join pushdown - left side required columns with aliases: " +
        log"${MDC(
          COLUMN_NAMES,
          leftSideRequiredColumnsWithAliases.map(_.prettyString()).mkString(", ")
        )}")
      logInfo(log"DSv2 Join pushdown - right side required columns with aliases: " +
        log"${MDC(
          COLUMN_NAMES,
          rightSideRequiredColumnsWithAliases.map(_.prettyString()).mkString(", ")
        )}")

      if (translatedJoinType.isDefined &&
        translatedCondition.isDefined &&
        lBuilder.pushDownJoin(
          rBuilder,
          translatedJoinType.get,
          leftSideRequiredColumnsWithAliases,
          rightSideRequiredColumnsWithAliases,
          translatedCondition.get)
      ) {
        val leftSidePushedDownOperators = getPushedDownOperators(leftHolder)
        val rightSidePushedDownOperators = getPushedDownOperators(rightHolder)

        leftHolder.joinedRelations = leftHolder.joinedRelations ++ rightHolder.joinedRelations
        leftHolder.joinedRelationsPushedDownOperators =
          Seq(leftSidePushedDownOperators, rightSidePushedDownOperators)

        leftHolder.pushedPredicates = Seq(translatedCondition.get)
        leftHolder.pushedSample = None

        leftHolder.output = node.output.asInstanceOf[Seq[AttributeReference]]
        leftHolder.pushedJoinOutputMap = pushedJoinOutputMap

        // TODO: for cascade joins, already joined relations will still have the name of the
        // original(leaf) relation. It should be thought of if we want to change the name of the
        // relation when join is pushed down.
        logInfo(log"DSv2 Join pushdown - successfully pushed down join between relations " +
          log"${MDC(RELATION_NAME, leftHolder.relation.name)} and " +
          log"${MDC(RELATION_NAME, rightHolder.relation.name)}.")

        leftHolder
      } else {
        logInfo(log"DSv2 Join pushdown - failed to push down join.")
        node
      }
  }
  /**
   * Generates unique column aliases for join operations to avoid naming conflicts.
   * Handles case sensitivity issues across different databases (SQL Server, MySQL, etc.).
   *
   * @param leftSideRequiredColumnNames  Columns from the left side of the join
   * @param rightSideRequiredColumnNames Columns from the right side of the join
   * @return Tuple of (leftColumnsWithAliases, rightColumnsWithAliases)
   */
  private[v2] def generateColumnAliasesForDuplicatedName(
    leftSideRequiredColumnNames: Array[String],
    rightSideRequiredColumnNames: Array[String]
  ): (Array[SupportsPushDownJoin.ColumnWithAlias],
    Array[SupportsPushDownJoin.ColumnWithAlias]) = {
    // Normalize all column names to lowercase for case-insensitive comparison
    val normalizeCase: String => String = _.toLowerCase(Locale.ROOT)

    // Count occurrences of each column name (case-insensitive)
    val allRequiredColumnNames = leftSideRequiredColumnNames ++ rightSideRequiredColumnNames
    val allNameCounts: Map[String, Int] =
      allRequiredColumnNames.map(normalizeCase)
        .groupBy(identity)
        .view
        .mapValues(_.length)
        .toMap

    // Track claimed aliases using normalized names.
    // Use Set for O(1) lookups when checking existing column names, claim all names
    // that appears only once to ensure they have highest priority.
    val allClaimedAliases = mutable.Set.from(
      allNameCounts.filter(_._2 == 1).keys
    )

    // Track suffix index for each base column name (starts at 0) to avoid extreme worst
    // case of O(n^2) alias generation.
    val aliasSuffixIndex = mutable.HashMap[String, Int]().withDefaultValue(0)

    def processColumn(originalName: String): SupportsPushDownJoin.ColumnWithAlias = {
      val normalizedName = normalizeCase(originalName)

      // No alias needed for unique column names
      if (allNameCounts(normalizedName) == 1) {
        new SupportsPushDownJoin.ColumnWithAlias(originalName, null)
      } else {
        var attempt = aliasSuffixIndex(normalizedName)
        var candidate = if (attempt == 0) originalName else s"${originalName}_$attempt"
        var normalizedCandidate = normalizeCase(candidate)

        // Find first available unique alias, use original name for the first attempt, then append
        // suffix for more attempts.
        while (allClaimedAliases.contains(normalizedCandidate)) {
          attempt += 1
          candidate = s"${originalName}_$attempt"
          normalizedCandidate = normalizeCase(candidate)
        }

        // Update tracking state
        aliasSuffixIndex(normalizedName) = attempt + 1
        allClaimedAliases.add(normalizedCandidate)

        if (originalName == candidate) {
          new SupportsPushDownJoin.ColumnWithAlias(originalName, null)
        } else {
          new SupportsPushDownJoin.ColumnWithAlias(originalName, candidate)
        }
      }
    }

    (
      leftSideRequiredColumnNames.map(processColumn),
      rightSideRequiredColumnNames.map(processColumn)
    )
  }

  // Projections' names are maybe not up to date if the joins have been previously pushed down.
  // For this reason, we need to use pushedJoinOutputMap to get up to date names.
  def getRequiredColumnNames(
      projections: Seq[NamedExpression],
      sHolder: ScanBuilderHolder): Array[String] = {
    val normalizedProjections = DataSourceStrategy.normalizeExprs(
      projections,
      sHolder.output.map { a =>
        sHolder.pushedJoinOutputMap.getOrElse(a, a).asInstanceOf[AttributeReference]
      }
    ).asInstanceOf[Seq[AttributeReference]]

    normalizedProjections.map(_.name).toArray
  }

  def pushDownAggregates(plan: LogicalPlan): LogicalPlan = plan.transform {
    // update the scan builder with agg pushdown and return a new plan with agg pushed
    case agg: Aggregate => rewriteAggregate(agg)
  }

  def pushDownVariants(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case p@PhysicalOperation(projectList, filters, sHolder @ ScanBuilderHolder(_, _,
        builder: SupportsPushDownVariantExtractions))
        if conf.getConf(org.apache.spark.sql.internal.SQLConf.PUSH_VARIANT_INTO_SCAN) =>
      pushVariantExtractions(p, projectList, filters, sHolder, builder)
  }

  /**
   * Converts an ordinal path to a field name path.
   *
   * @param structType The top-level struct type
   * @param ordinals The ordinal path (e.g., [1, 1] for nested.field)
   * @return The field name path (e.g., ["nested", "field"])
   */
  private def getColumnName(structType: StructType, ordinals: Seq[Int]): Seq[String] = {
    ordinals match {
      case Seq() =>
        // Base case: no more ordinals
        Seq.empty
      case ordinal +: rest =>
        // Get the field at this ordinal
        val field = structType.fields(ordinal)
        if (rest.isEmpty) {
          // Last ordinal in the path
          Seq(field.name)
        } else {
          // Recurse into nested struct
          field.dataType match {
            case nestedStruct: StructType =>
              field.name +: getColumnName(nestedStruct, rest)
            case _ =>
              throw SparkException.internalError(
                s"Expected StructType at field '${field.name}' but got ${field.dataType}")
          }
        }
    }
  }

  private def pushVariantExtractions(
      originalPlan: LogicalPlan,
      projectList: Seq[NamedExpression],
      filters: Seq[Expression],
      sHolder: ScanBuilderHolder,
      builder: SupportsPushDownVariantExtractions): LogicalPlan = {
    val variants = new VariantInRelation

    // Extract schema attributes from scan builder holder
    val schemaAttributes = sHolder.output

    // Construct schema for default value resolution
    val structSchema = StructType(schemaAttributes.map(a =>
      StructField(a.name, a.dataType, a.nullable, a.metadata)))

    val defaultValues = org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.
      existenceDefaultValues(structSchema)

    // Add variant fields from the V2 scan schema
    for ((a, defaultValue) <- schemaAttributes.zip(defaultValues)) {
      variants.addVariantFields(a.exprId, a.dataType, defaultValue, Nil)
    }
    if (variants.mapping.isEmpty) return originalPlan

    // Collect requested fields from project list and filters
    projectList.foreach(variants.collectRequestedFields)
    filters.foreach(variants.collectRequestedFields)

    // If no variant columns remain after collection, return original plan
    if (variants.mapping.forall(_._2.isEmpty)) return originalPlan

    // Build individual VariantExtraction for each field access
    // Track which extraction corresponds to which (attr, field, ordinal)
    val extractionInfo = schemaAttributes.flatMap { topAttr =>
      val variantFields = variants.mapping.get(topAttr.exprId)
      if (variantFields.isEmpty || variantFields.get.isEmpty) {
        // No variant fields for this attribute
        Seq.empty
      } else {
        variantFields.get.toSeq.flatMap { case (pathToVariant, fields) =>
          val columnName = if (pathToVariant.isEmpty) {
            Seq(topAttr.name)
          } else {
            Seq(topAttr.name) ++
              getColumnName(topAttr.dataType.asInstanceOf[StructType], pathToVariant)
          }
          fields.toArray.sortBy(_._2).map { case (field, ordinal) =>
            val extraction = new VariantExtractionImpl(
              columnName.toArray,
              field.path.toMetadata,
              field.targetType
            )
            (extraction, topAttr, field, ordinal)
          }
        }
      }
    }

    // Call the API to push down variant extractions
    if (extractionInfo.isEmpty) return originalPlan

    val extractions: Array[VariantExtraction] = extractionInfo.map(_._1).toArray
    val pushedResults = builder.pushVariantExtractions(extractions)

    // Filter to only the accepted extractions
    val acceptedExtractions = extractionInfo.zip(pushedResults).filter(_._2).map(_._1)
    if (acceptedExtractions.isEmpty) return originalPlan

    // Group accepted extractions by attribute to rebuild the struct schemas
    val extractionsByAttr = acceptedExtractions.groupBy(_._2)
    val pushedColumnNames = extractionsByAttr.keys.map(_.name).toSet

    // Build new attribute mapping based on pushed variant extractions
    val attributeMap = schemaAttributes.map { a =>
      if (pushedColumnNames.contains(a.name) && variants.mapping.get(a.exprId).exists(_.nonEmpty)) {
        val newType = variants.rewriteType(a.exprId, a.dataType, Nil)
        val newAttr = AttributeReference(a.name, newType, a.nullable, a.metadata)(
          qualifier = a.qualifier)
        (a.exprId, newAttr)
      } else {
        (a.exprId, a.asInstanceOf[AttributeReference])
      }
    }.toMap

    val newOutput = sHolder.output.map(a => attributeMap.getOrElse(a.exprId, a))

    // Store the transformation info on the holder for later use
    sHolder.pushedVariants = Some(variants)
    sHolder.pushedVariantAttributeMap = attributeMap
    sHolder.output = newOutput

    // Return the original plan unchanged - transformation happens in buildScanWithPushedVariants
    originalPlan
  }

  private def rewriteAggregate(agg: Aggregate): LogicalPlan = agg.child match {
    case PhysicalOperation(project, Nil, holder @ ScanBuilderHolder(_, _,
        r: SupportsPushDownAggregates)) if CollapseProject.canCollapseExpressions(
        agg.aggregateExpressions, project, alwaysInline = true) =>
      val aliasMap = getAliasMap(project)
      val actualResultExprs = agg.aggregateExpressions.map(replaceAliasButKeepName(_, aliasMap))
      val actualGroupExprs = agg.groupingExpressions.map(replaceAlias(_, aliasMap))

      val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
      val normalizedAggExprs =
        normalizeExpressions(aggregates, holder).asInstanceOf[Seq[AggregateExpression]]
      val normalizedGroupingExpr = normalizeExpressions(actualGroupExprs, holder)
      val translatedAggOpt = DataSourceStrategy.translateAggregation(
        normalizedAggExprs, normalizedGroupingExpr)
      if (translatedAggOpt.isEmpty) {
        // Cannot translate the catalyst aggregate, return the query plan unchanged.
        return agg
      }

      val (finalResultExprs, finalAggExprs, translatedAgg, canCompletePushDown) = {
        if (r.supportCompletePushDown(translatedAggOpt.get)) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, true)
        } else if (!translatedAggOpt.get.aggregateExpressions().exists(_.isInstanceOf[Avg])) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
        } else {
          // scalastyle:off
          // The data source doesn't support the complete push-down of this aggregation.
          // Here we translate `AVG` to `SUM / COUNT`, so that it's more likely to be
          // pushed, completely or partially.
          // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
          // SELECT avg(c1) FROM t GROUP BY c2;
          // The original logical plan is
          // Aggregate [c2#10],[avg(c1#9) AS avg(c1)#19]
          // +- ScanOperation[...]
          //
          // After convert avg(c1#9) to sum(c1#9)/count(c1#9)
          // we have the following
          // Aggregate [c2#10],[sum(c1#9)/count(c1#9) AS avg(c1)#19]
          // +- ScanOperation[...]
          // scalastyle:on
          val newResultExpressions = actualResultExprs.map { expr =>
            expr.transform {
              case AggregateExpression(avg: aggregate.Average, _, isDistinct, _, _) =>
                val sum = aggregate.Sum(avg.child).toAggregateExpression(isDistinct)
                val count = aggregate.Count(avg.child).toAggregateExpression(isDistinct)
                avg.evaluateExpression transform {
                  case a: Attribute if a.semanticEquals(avg.sum) =>
                    addCastIfNeeded(sum, avg.sum.dataType)
                  case a: Attribute if a.semanticEquals(avg.count) =>
                    addCastIfNeeded(count, avg.count.dataType)
                }
            }
          }.asInstanceOf[Seq[NamedExpression]]
          // Because aggregate expressions changed, translate them again.
          aggExprToOutputOrdinal.clear()
          val newAggregates =
            collectAggregates(newResultExpressions, aggExprToOutputOrdinal)
          val newNormalizedAggExprs = DataSourceStrategy.normalizeExprs(
            newAggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
          val newTranslatedAggOpt = DataSourceStrategy.translateAggregation(
            newNormalizedAggExprs, normalizedGroupingExpr)
          if (newTranslatedAggOpt.isEmpty) {
            // Ideally we should never reach here. But if we end up with not able to translate
            // new aggregate with AVG replaced by SUM/COUNT, revert to the original one.
            (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
          } else {
            (newResultExpressions, newNormalizedAggExprs, newTranslatedAggOpt.get,
              r.supportCompletePushDown(newTranslatedAggOpt.get))
          }
        }
      }

      if (!canCompletePushDown && !supportPartialAggPushDown(translatedAgg)) {
        return agg
      }
      if (!r.pushAggregation(translatedAgg)) {
        return agg
      }

      // scalastyle:off
      // We name the output columns of group expressions and aggregate functions by
      // ordinal: `group_col_0`, `group_col_1`, ..., `agg_func_0`, `agg_func_1`, ...
      // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
      // SELECT min(c1), max(c1) FROM t GROUP BY c2;
      // Use group_col_0, agg_func_0, agg_func_1 as output for ScanBuilderHolder.
      // We want to have the following logical plan:
      // == Optimized Logical Plan ==
      // Aggregate [group_col_0#10], [min(agg_func_0#21) AS min(c1)#17, max(agg_func_1#22) AS max(c1)#18]
      // +- ScanBuilderHolder[group_col_0#10, agg_func_0#21, agg_func_1#22]
      // Later, we build the `Scan` instance and convert ScanBuilderHolder to DataSourceV2ScanRelation.
      // scalastyle:on
      val groupOutputMap = normalizedGroupingExpr.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"group_col_$i", e.dataType)() -> e
      }
      val groupOutput = groupOutputMap.map(_._1)
      val aggOutputMap = finalAggExprs.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"agg_func_$i", e.dataType)() -> e
      }
      val aggOutput = aggOutputMap.map(_._1)
      val newOutput = groupOutput ++ aggOutput
      val groupByExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      normalizedGroupingExpr.zipWithIndex.foreach { case (expr, ordinal) =>
        if (!groupByExprToOutputOrdinal.contains(expr.canonicalized)) {
          groupByExprToOutputOrdinal(expr.canonicalized) = ordinal
        }
      }

      holder.pushedAggregate = Some(translatedAgg)
      holder.pushedAggOutputMap = AttributeMap(groupOutputMap ++ aggOutputMap)
      holder.output = newOutput
      logInfo(
        log"""
            |Pushing operators to ${MDC(RELATION_NAME, holder.relation.name)}
            |Pushed Aggregate Functions:
            | ${MDC(AGGREGATE_FUNCTIONS, translatedAgg.aggregateExpressions().mkString(", "))}
            |Pushed Group by:
            | ${MDC(GROUP_BY_EXPRS, translatedAgg.groupByExpressions.mkString(", "))}
           """.stripMargin)

      if (canCompletePushDown) {
        val projectExpressions = finalResultExprs.map { expr =>
          expr.transformDown {
            case agg: AggregateExpression =>
              val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
              Alias(aggOutput(ordinal), agg.resultAttribute.name)(agg.resultAttribute.exprId)
            case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
              val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
              expr match {
                case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
                case _ => groupOutput(ordinal)
              }
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(projectExpressions, holder)
      } else {
        // scalastyle:off
        // Change the optimized logical plan to reflect the pushed down aggregate
        // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
        // SELECT min(c1), max(c1) FROM t GROUP BY c2;
        // The original logical plan is
        // Aggregate [c2#10],[min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c1#9, c2#10] ...
        //
        // After change the V2ScanRelation output to [c2#10, min(c1)#21, max(c1)#22]
        // we have the following
        // !Aggregate [c2#10], [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        //
        // We want to change it to
        // == Optimized Logical Plan ==
        // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        // scalastyle:on
        val aggExprs = finalResultExprs.map(_.transform {
          case agg: AggregateExpression =>
            val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
            val aggAttribute = aggOutput(ordinal)
            val aggFunction: aggregate.AggregateFunction =
              agg.aggregateFunction match {
                case max: aggregate.Max =>
                  max.copy(child = aggAttribute)
                case min: aggregate.Min =>
                  min.copy(child = aggAttribute)
                case sum: aggregate.Sum =>
                  // To keep the dataType of `Sum` unchanged, we need to cast the
                  // data-source-aggregated result to `Sum.child.dataType` if it's decimal.
                  // See `SumBase.resultType`
                  val newChild = if (sum.dataType.isInstanceOf[DecimalType]) {
                    addCastIfNeeded(aggAttribute, sum.child.dataType)
                  } else {
                    aggAttribute
                  }
                  sum.copy(child = newChild)
                case _: aggregate.Count =>
                  aggregate.Sum(aggAttribute)
                case other => other
              }
            agg.copy(aggregateFunction = aggFunction)
          case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
            val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
            expr match {
              case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
              case _ => groupOutput(ordinal)
            }
        }).asInstanceOf[Seq[NamedExpression]]
        Aggregate(groupOutput, aggExprs, holder)
      }

    case _ => agg
  }

  private def collectAggregates(
      resultExpressions: Seq[NamedExpression],
      aggExprToOutputOrdinal: mutable.HashMap[Expression, Int]): Seq[AggregateExpression] = {
    var ordinal = 0
    resultExpressions.flatMap { expr =>
      expr.collect {
        // Do not push down duplicated aggregate expressions. For example,
        // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
        // `max(a)` to the data source.
        case agg: AggregateExpression
          if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
          aggExprToOutputOrdinal(agg.canonicalized) = ordinal
          ordinal += 1
          agg
      }
    }
  }

  private def supportPartialAggPushDown(agg: Aggregation): Boolean = {
    // We can only partially push down min/max/sum/count without DISTINCT.
    agg.aggregateExpressions().isEmpty || agg.aggregateExpressions().forall {
      case sum: Sum => !sum.isDistinct
      case count: Count => !count.isDistinct
      case _: Min | _: Max | _: CountStar => true
      case _ => false
    }
  }

  private def addCastIfNeeded(expression: Expression, expectedDataType: DataType) =
    if (expression.dataType == expectedDataType) {
      expression
    } else {
      val cast = Cast(expression, expectedDataType)
      if (cast.timeZoneId.isEmpty && cast.needsTimeZone) {
        cast.withTimeZone(conf.sessionLocalTimeZone)
      } else {
        cast
      }
    }

  def buildScanWithPushedAggregate(plan: LogicalPlan): LogicalPlan = plan.transform {
    case holder: ScanBuilderHolder if holder.pushedAggregate.isDefined =>
      // No need to do column pruning because only the aggregate columns are used as
      // DataSourceV2ScanRelation output columns. All the other columns are not
      // included in the output.
      val scan = holder.builder.build()
      val realOutput = toAttributes(scan.readSchema())
      assert(realOutput.length == holder.output.length,
        "The data source returns unexpected number of columns")
      val wrappedScan = getWrappedScan(scan, holder)
      val scanRelation = DataSourceV2ScanRelation(holder.relation, wrappedScan, realOutput)
      val projectList = realOutput.zip(holder.output).map { case (a1, a2) =>
        // The data source may return columns with arbitrary data types and it's safer to cast them
        // to the expected data type.
        assert(Cast.canCast(a1.dataType, a2.dataType))
        Alias(addCastIfNeeded(a1, a2.dataType), a2.name)(a2.exprId)
      }
      Project(projectList, scanRelation)
  }

  def buildScanWithPushedJoin(plan: LogicalPlan): LogicalPlan = plan.transform {
    case holder: ScanBuilderHolder if holder.joinedRelations.length > 1 =>
      val scan = holder.builder.build()
      val realOutput = toAttributes(scan.readSchema())
      assert(realOutput.length == holder.output.length,
        "The data source returns unexpected number of columns")
      val wrappedScan = getWrappedScan(scan, holder)
      val scanRelation = DataSourceV2ScanRelation(holder.relation, wrappedScan, realOutput)

      // When join is pushed down, the real output is going to be, for example,
      // SALARY_01234#0, NAME_ab123#1, DEPT_cd123#2.
      // We should revert these names back to original names. For example,
      // SALARY#0, NAME#1, DEPT#1. This is done by adding projection with appropriate aliases.
      val projectList = realOutput.zip(holder.output).map { case (a1, a2) =>
        Alias(a1, a2.name)(a2.exprId)
      }
      Project(projectList, scanRelation)
  }

  def buildScanWithPushedVariants(plan: LogicalPlan): LogicalPlan = plan.transform {
    case p@PhysicalOperation(projectList, filters, holder: ScanBuilderHolder)
        if holder.pushedVariants.isDefined =>
      val variants = holder.pushedVariants.get
      val attributeMap = holder.pushedVariantAttributeMap

      // Build the scan
      val scan = holder.builder.build()
      val realOutput = toAttributes(scan.readSchema())
      val wrappedScan = getWrappedScan(scan, holder)
      val scanRelation = DataSourceV2ScanRelation(holder.relation, wrappedScan, realOutput)

      // Create projection to map real output to expected output (with transformed types)
      val outputProjection = realOutput.zip(holder.output).map { case (realAttr, expectedAttr) =>
        Alias(realAttr, expectedAttr.name)(expectedAttr.exprId)
      }

      // Rewrite filter expressions using the variant transformation
      val rewrittenFilters = if (filters.nonEmpty) {
        val rewrittenFilterExprs = filters.map(variants.rewriteExpr(_, attributeMap))
        Some(rewrittenFilterExprs.reduce(And))
      } else {
        None
      }

      // Rewrite project list expressions using the variant transformation
      val rewrittenProjectList = projectList.map { e =>
        val rewritten = variants.rewriteExpr(e, attributeMap)
        rewritten match {
          case n: NamedExpression => n
          // When the variant column is directly selected, we replace the attribute
          // reference with a struct access, which is not a NamedExpression. Wrap it with Alias.
          case _ => Alias(rewritten, e.name)(e.exprId, e.qualifier)
        }
      }

      // Build the plan: Project(outputProjection) -> [Filter?] -> scanRelation
      val withProjection = Project(outputProjection, scanRelation)
      val withFilter = rewrittenFilters.map(Filter(_, withProjection)).getOrElse(withProjection)
      Project(rewrittenProjectList, withFilter)
  }

  def pruneColumns(plan: LogicalPlan): LogicalPlan = plan.transform {
    case ScanOperation(project, filtersStayUp, filtersPushDown, sHolder: ScanBuilderHolder) =>
      // column pruning
      val normalizedProjects = DataSourceStrategy
        .normalizeExprs(project, sHolder.output)
        .asInstanceOf[Seq[NamedExpression]]
      val allFilters = filtersPushDown.reduceOption(And).toSeq ++ filtersStayUp
      val normalizedFilters = DataSourceStrategy.normalizeExprs(allFilters, sHolder.output)
      val (scan, output) = PushDownUtils.pruneColumns(
        sHolder.builder, sHolder.relation, normalizedProjects, normalizedFilters)

      logInfo(
        log"""
            |Output: ${MDC(RELATION_OUTPUT, output.mkString(", "))}
           """.stripMargin)

      val wrappedScan = getWrappedScan(scan, sHolder)

      val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

      val projectionOverSchema =
        ProjectionOverSchema(output.toStructType, AttributeSet(output))
      val projectionFunc = (expr: Expression) => expr transformDown {
        case projectionOverSchema(newExpr) => newExpr
      }

      val finalFilters = normalizedFilters.map(projectionFunc)
      // bottom-most filters are put in the left of the list.
      val withFilter = finalFilters.foldLeft[LogicalPlan](scanRelation)((plan, cond) => {
        Filter(cond, plan)
      })

      if (withFilter.output != project) {
        val newProjects = normalizedProjects
          .map(projectionFunc)
          .asInstanceOf[Seq[NamedExpression]]
        Project(restoreOriginalOutputNames(newProjects, project.map(_.name)), withFilter)
      } else {
        withFilter
      }
  }

  def pushDownSample(plan: LogicalPlan): LogicalPlan = plan.transform {
    case sample: Sample => sample.child match {
      case PhysicalOperation(_, Nil, sHolder: ScanBuilderHolder) =>
        val tableSample = TableSampleInfo(
          sample.lowerBound,
          sample.upperBound,
          sample.withReplacement,
          sample.seed)
        val pushed = PushDownUtils.pushTableSample(sHolder.builder, tableSample)
        if (pushed) {
          sHolder.pushedSample = Some(tableSample)
          sample.child
        } else {
          sample
        }

      case _ => sample
    }
  }

  private def pushDownLimit(plan: LogicalPlan, limit: Int): (LogicalPlan, Boolean) = plan match {
    case operation @ PhysicalOperation(_, Nil, sHolder: ScanBuilderHolder) =>
      val (isPushed, isPartiallyPushed) = PushDownUtils.pushLimit(sHolder.builder, limit)
      if (isPushed) {
        sHolder.pushedLimit = Some(limit)
      }
      (operation, isPushed && !isPartiallyPushed)
    case s @ Sort(order, _,
    operation @ PhysicalOperation(project, Nil, sHolder: ScanBuilderHolder), _)
      if CollapseProject.canCollapseExpressions(order, project, alwaysInline = true) =>
      val aliasMap = getAliasMap(project)
      val aliasReplacedOrder = order.map(replaceAlias(_, aliasMap))
      val newOrder = if (sHolder.pushedAggregate.isDefined) {
        // `ScanBuilderHolder` has different output columns after aggregate push-down. Here we
        // replace the attributes in ordering expressions with the original table output columns.
        aliasReplacedOrder.map {
          _.transform {
            case a: Attribute => sHolder.pushedAggOutputMap.getOrElse(a, a)
          }.asInstanceOf[SortOrder]
        }
      } else {
        aliasReplacedOrder.asInstanceOf[Seq[SortOrder]]
      }
      val normalizedOrders = normalizeExpressions(newOrder, sHolder).asInstanceOf[Seq[SortOrder]]
      val orders = DataSourceStrategy.translateSortOrders(normalizedOrders)
      if (orders.length == order.length) {
        val (isPushed, isPartiallyPushed) =
          PushDownUtils.pushTopN(sHolder.builder, orders.toArray, limit)
        if (isPushed) {
          sHolder.pushedLimit = Some(limit)
          sHolder.sortOrders = orders
          if (isPartiallyPushed) {
            (s, false)
          } else {
            (operation, true)
          }
        } else {
          (s, false)
        }
      } else {
        (s, false)
      }
    case p: Project =>
      val (newChild, isPartiallyPushed) = pushDownLimit(p.child, limit)
      (p.withNewChildren(Seq(newChild)), isPartiallyPushed)
    case other => (other, false)
  }

  @scala.annotation.tailrec
  private def pushDownOffset(
      plan: LogicalPlan,
      offset: Int): Boolean = plan match {
    case sHolder: ScanBuilderHolder =>
      val isPushed = PushDownUtils.pushOffset(sHolder.builder, offset)
      if (isPushed) {
        sHolder.pushedOffset = Some(offset)
      }
      isPushed
    case Project(projectList, child) if projectList.forall(_.deterministic) =>
      pushDownOffset(child, offset)
    case _ => false
  }

  def pushDownLimitAndOffset(plan: LogicalPlan): LogicalPlan = plan.transform {
    case offset @ LimitAndOffset(limit, offsetValue, child) =>
      val (newChild, canRemoveLimit) = pushDownLimit(child, limit)
      if (canRemoveLimit) {
        // Try to push down OFFSET only if the LIMIT operator has been pushed and can be removed.
        val isPushed = pushDownOffset(newChild, offsetValue)
        if (isPushed) {
          newChild
        } else {
          // Keep the OFFSET operator if we failed to push down OFFSET to the data source.
          offset.withNewChildren(Seq(newChild))
        }
      } else {
        // Keep the OFFSET operator if we can't remove LIMIT operator.
        offset
      }
    case globalLimit @ OffsetAndLimit(offset, limit, child) =>
      // For `df.offset(n).limit(m)`, we can push down `limit(m + n)` first.
      val (newChild, canRemoveLimit) = pushDownLimit(child, limit + offset)
      if (canRemoveLimit) {
        // Try to push down OFFSET only if the LIMIT operator has been pushed and can be removed.
        val isPushed = pushDownOffset(newChild, offset)
        if (isPushed) {
          newChild
        } else {
          // Still keep the OFFSET operator if we can't push it down.
          Offset(Literal(offset), newChild)
        }
      } else {
        // For `df.offset(n).limit(m)`, since we can't push down `limit(m + n)`,
        // try to push down `offset(n)` here.
        val isPushed = pushDownOffset(child, offset)
        if (isPushed) {
          // Keep the LIMIT operator if we can't push it down.
          Limit(Literal(limit, IntegerType), child)
        } else {
          // Keep the origin plan if we can't push OFFSET operator and LIMIT operator.
          globalLimit
        }
      }
    case globalLimit @ Limit(IntegerLiteral(limitValue), child) =>
      val (newChild, canRemoveLimit) = pushDownLimit(child, limitValue)
      if (canRemoveLimit) {
        newChild
      } else {
        val newLocalLimit =
          globalLimit.child.asInstanceOf[LocalLimit].withNewChildren(Seq(newChild))
        globalLimit.withNewChildren(Seq(newLocalLimit))
      }
    case offset @ Offset(IntegerLiteral(n), child) =>
      val isPushed = pushDownOffset(child, n)
      if (isPushed) {
        child
      } else {
        offset
      }
  }

  private def normalizeExpressions(
      expressions: Seq[Expression],
      sHolder: ScanBuilderHolder): Seq[Expression] = {
    val output = if (sHolder.joinedRelations.length == 1) {
      // Join is not pushed down
      sHolder.relation.output
    } else {
      // sHolder.output's names can be out of date if the joins has previously been pushed down.
      // For this reason, we need to use pushedJoinOutputMap to get up to date names.
      sHolder.output.map { a =>
        sHolder.pushedJoinOutputMap.getOrElse(a, a).asInstanceOf[AttributeReference]
      }
    }

    DataSourceStrategy.normalizeExprs(expressions, output)
  }

  private def getWrappedScan(scan: Scan, sHolder: ScanBuilderHolder): Scan = {
    scan match {
      case v1: V1Scan =>
        val pushedFilters = sHolder.builder match {
          case f: SupportsPushDownFilters =>
            f.pushedFilters()
          case _ => Array.empty[sources.Filter]
        }
        val pushedDownOperators = getPushedDownOperators(sHolder)
        V1ScanWrapper(v1, pushedFilters.toImmutableArraySeq, pushedDownOperators)
      case _ => scan
    }
  }

  private def getPushedDownOperators(sHolder: ScanBuilderHolder): PushedDownOperators = {
    val optRelationName = Option.when(sHolder.joinedRelations.length <= 1)(sHolder.relation.name)
    PushedDownOperators(sHolder.pushedAggregate, sHolder.pushedSample,
      sHolder.pushedLimit, sHolder.pushedOffset, sHolder.sortOrders, sHolder.pushedPredicates,
      sHolder.joinedRelationsPushedDownOperators, optRelationName)
  }
}

case class ScanBuilderHolder(
    var output: Seq[AttributeReference],
    relation: DataSourceV2Relation,
    builder: ScanBuilder) extends LeafNode {
  var pushedLimit: Option[Int] = None

  var pushedOffset: Option[Int] = None

  var sortOrders: Seq[V2SortOrder] = Seq.empty[V2SortOrder]

  var pushedSample: Option[TableSampleInfo] = None

  var pushedPredicates: Seq[Predicate] = Seq.empty[Predicate]

  var pushedAggregate: Option[Aggregation] = None

  var pushedAggOutputMap: AttributeMap[Expression] = AttributeMap.empty[Expression]

  var joinedRelations: Seq[DataSourceV2RelationBase] = Seq(relation)

  var joinedRelationsPushedDownOperators: Seq[PushedDownOperators] = Seq.empty[PushedDownOperators]

  var pushedJoinOutputMap: AttributeMap[Expression] = AttributeMap.empty[Expression]

  var pushedVariantAttributeMap: Map[ExprId, AttributeReference] = Map.empty

  var pushedVariants: Option[VariantInRelation] = None
}

// A wrapper for v1 scan to carry the translated filters and the handled ones, along with
// other pushed down operators. This is required by the physical v1 scan node.
case class V1ScanWrapper(
    v1Scan: V1Scan,
    handledFilters: Seq[sources.Filter],
    pushedDownOperators: PushedDownOperators) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}
