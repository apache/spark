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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Expression, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, InsertAction, LogicalPlan, MergeIntoTable, UpdateAction, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig.AnsiStoreAssignmentCastCheck
import org.apache.spark.sql.internal.connector.SupportsConfigurableSchemaAlignment

/**
 * A rule that resolves assignments in row-level commands.
 *
 * Note that this rule must be run before rewriting row-level commands into executable plans.
 * This rule does not apply to tables that accept any schema. Such tables must inject their own
 * rules to resolve assignments.
 */
object ResolveRowLevelCommandAssignments extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    case MetadataColumnAssignments(assignments, errors) =>
      throw QueryCompilationErrors.invalidRowLevelOperationAssignments(assignments, errors)

    case u: UpdateTable if !u.skipSchemaResolution && u.resolved && u.rewritable && !u.aligned =>
      validateStoreAssignmentPolicy()
      val (newTable, metadataAttrMap) = cleanAttrMetadata(u.table)
      val padded = padTargetReferences(u, u.table)
      val newAssignments = AssignmentUtils.alignUpdateAssignments(u.table.output,
        padded.assignments, fromStar = false, coerceNestedTypes = false,
        ansiStoreAssignmentCastCheck = ansiStoreAssignmentCastCheck(u.table))
      padded.copy(table = newTable, assignments = newAssignments).rewriteAttrs(metadataAttrMap)

    case u: UpdateTable if !u.skipSchemaResolution && u.resolved && !u.aligned =>
      resolveAssignments(u)

    case m: MergeIntoTable if m.rewritable && shouldAlignAssignments(m) && containsFinalSchema(m) =>
      validateStoreAssignmentPolicy()
      val coerceNestedTypes = conf.coerceMergeNestedTypes && m.withSchemaEvolution
      val castCheck = ansiStoreAssignmentCastCheck(m.targetTable)
      val (newTargetTable, metadataAttrMap) = cleanAttrMetadata(m.targetTable)
      val padded = padTargetReferences(m, m.targetTable)
      padded.copy(
        targetTable = newTargetTable,
        matchedActions = RowLevelAssignmentResolution.alignActions(
          m.targetTable.output,
          padded.matchedActions,
          coerceNestedTypes,
          castCheck),
        notMatchedActions = RowLevelAssignmentResolution.alignActions(
          m.targetTable.output,
          padded.notMatchedActions,
          coerceNestedTypes,
          castCheck),
        notMatchedBySourceActions = RowLevelAssignmentResolution.alignActions(
          m.targetTable.output,
          padded.notMatchedBySourceActions,
          coerceNestedTypes,
          castCheck)).rewriteAttrs(metadataAttrMap)

    case m: MergeIntoTable if shouldAlignAssignments(m) && containsFinalSchema(m) =>
      resolveAssignments(m)
  }

  // The values of metadata columns are produced by the data source and cannot be written, so
  // assignments to metadata columns are rejected unless the legacy behavior is enabled. The keys
  // are checked as soon as they are resolved, so that the command fails before schema evolution
  // changes the target table.
  private object MetadataColumnAssignments {
    def unapply(plan: LogicalPlan): Option[(Seq[Assignment], Seq[String])] = {
      val assignmentsToCheck = plan match {
        case _ if conf.getConf(SQLConf.LEGACY_IGNORE_METADATA_COLUMN_ASSIGNMENTS) =>
          Nil
        case u: UpdateTable if !u.skipSchemaResolution && u.rewritable =>
          Seq(u.assignments)
        case m: MergeIntoTable if !m.skipSchemaResolution && m.rewritable =>
          val actions = m.matchedActions ++ m.notMatchedActions ++ m.notMatchedBySourceActions
          actions.collect {
            case UpdateAction(_, assignments, _) => assignments
            case InsertAction(_, assignments) => assignments
          }
        case _ =>
          Nil
      }
      assignmentsToCheck.iterator
        .map { assignments =>
          (assignments, RowLevelAssignmentResolution.metadataColumnAssignmentErrors(assignments))
        }
        .find { case (_, errors) => errors.nonEmpty }
    }
  }

  private def shouldAlignAssignments(m: MergeIntoTable): Boolean = {
    !m.skipSchemaResolution && m.resolved && !m.aligned
  }

  private def containsFinalSchema(m: MergeIntoTable): Boolean = {
    !m.schemaEvolutionEnabled || (m.schemaEvolutionReady && m.pendingSchemaChanges.isEmpty)
  }

  private def ansiStoreAssignmentCastCheck(target: LogicalPlan): AnsiStoreAssignmentCastCheck = {
    target.collectFirst {
      case ExtractV2Table(table: SupportsConfigurableSchemaAlignment) =>
        table.schemaAlignmentConfig().ansiStoreAssignmentCastCheck()
    }.getOrElse(SchemaAlignmentConfig.DEFAULT.ansiStoreAssignmentCastCheck())
  }

  private def validateStoreAssignmentPolicy(): Unit = {
    // SPARK-28730: LEGACY store assignment policy is disallowed in data source v2
    if (conf.storeAssignmentPolicy == StoreAssignmentPolicy.LEGACY) {
      throw QueryCompilationErrors.legacyStoreAssignmentPolicyError()
    }
  }

  // A cleaned relation is a new instance whose metadata output has new expression IDs, so this
  // also returns a map from the metadata output of the original relation, which the command may
  // reference, to the new one. A relation with nothing to clean is kept as is.
  private def cleanAttrMetadata(table: LogicalPlan): (LogicalPlan, AttributeMap[Attribute]) = {
    val metadataAttrMapping = mutable.ArrayBuffer.empty[(Attribute, Attribute)]
    val newTable = table.transform {
      case r: DataSourceV2Relation =>
        val newOutput = r.output.map(CharVarcharUtils.cleanAttrMetadata)
        if (newOutput == r.output) {
          r
        } else {
          val newRelation = r.copy(output = newOutput)
          metadataAttrMapping ++= r.metadataOutput.zip(newRelation.metadataOutput)
          newRelation
        }
    }
    (newTable, AttributeMap(metadataAttrMapping.toSeq))
  }

  // A cleaned relation gets no read-side padding from ApplyCharTypePadding, which keeps carried
  // over rows and row IDs as they are stored. The conditions and assigned values of a command must
  // still see what any other read of the table sees, so the references in them are padded in
  // place. This is done before alignment, which adds references to the columns that are not
  // assigned, and after padding the comparisons, as ApplyCharTypePadding cannot match them once
  // their references are padded.
  private def padTargetReferences[T <: LogicalPlan](command: T, target: LogicalPlan): T = {
    // standardSemantics takes precedence over legacy charVarcharAsString.
    val readSidePadding = conf.charVarcharStandardSemantics ||
      (conf.readSideCharPadding && !conf.charVarcharAsString)
    val paddedAttrs = if (readSidePadding) {
      AttributeMap(target.output.map(attr => attr -> CharVarcharUtils.addPaddingForScan(attr))
        .filter { case (attr, padded) => padded != attr })
    } else {
      AttributeMap.empty[Expression]
    }

    def padOuterReferences(plan: LogicalPlan): LogicalPlan = plan.transformWithSubqueries {
      case p => p.transformExpressionsUp {
        case OuterReference(attr: AttributeReference) if paddedAttrs.contains(attr) =>
          paddedAttrs(attr).transform {
            case a: AttributeReference if a.semanticEquals(attr) => OuterReference(a)
          }
      }
    }

    def padReferences(expr: Expression): Expression = expr match {
      case assignment: Assignment => assignment.copy(value = padReferences(assignment.value))
      case attr: AttributeReference => paddedAttrs.getOrElse(attr, attr)
      case subquery: SubqueryExpression => subquery.withNewPlan(padOuterReferences(subquery.plan))
      case other => other.mapChildren(padReferences)
    }

    if (paddedAttrs.isEmpty) {
      command
    } else {
      ApplyCharTypePaddingHelper.paddingForStringComparison(command, padCharCol = false)
        .mapExpressions(padReferences).asInstanceOf[T]
    }
  }

  private def resolveAssignments(p: LogicalPlan): LogicalPlan = {
    p.transformExpressions {
      case assignment: Assignment =>
        RowLevelAssignmentResolution.resolveAssignment(assignment)
    }
  }

}
