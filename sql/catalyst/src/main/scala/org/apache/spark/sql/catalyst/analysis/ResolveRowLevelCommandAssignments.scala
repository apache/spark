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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Cast, MetadataAttribute}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, LogicalPlan, MergeAction, MergeIntoTable, UpdateAction, UpdateTable}
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
      val newAssignments = AssignmentUtils.alignUpdateAssignments(u.table.output, u.assignments,
        fromStar = false, coerceNestedTypes = false,
        ansiStoreAssignmentCastCheck = ansiStoreAssignmentCastCheck(u.table))
      u.copy(table = newTable, assignments = newAssignments).rewriteAttrs(metadataAttrMap)

    case u: UpdateTable if !u.skipSchemaResolution && u.resolved && !u.aligned =>
      resolveAssignments(u)

    case m: MergeIntoTable if m.rewritable && shouldAlignAssignments(m) && containsFinalSchema(m) =>
      validateStoreAssignmentPolicy()
      val coerceNestedTypes = conf.coerceMergeNestedTypes && m.withSchemaEvolution
      val castCheck = ansiStoreAssignmentCastCheck(m.targetTable)
      val (newTargetTable, metadataAttrMap) = cleanAttrMetadata(m.targetTable)
      m.copy(
        targetTable = newTargetTable,
        matchedActions = alignActions(
          m.targetTable.output,
          m.matchedActions,
          coerceNestedTypes,
          castCheck),
        notMatchedActions = alignActions(
          m.targetTable.output,
          m.notMatchedActions,
          coerceNestedTypes,
          castCheck),
        notMatchedBySourceActions = alignActions(
          m.targetTable.output,
          m.notMatchedBySourceActions,
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
        .map(assignments => (assignments, metadataColumnAssignmentErrors(assignments)))
        .find { case (_, errors) => errors.nonEmpty }
    }
  }

  private def metadataColumnAssignmentErrors(assignments: Seq[Assignment]): Seq[String] = {
    assignments.map(_.key).filter(_.resolved).flatMap(_.references)
      .filter(attr => MetadataAttribute.isValid(attr.metadata))
      .map(_.name)
      .distinct
      .map(name => s"Cannot assign to metadata column '$name'")
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

  private def resolveAssignments(p: LogicalPlan): LogicalPlan = {
    p.transformExpressions {
      case assignment: Assignment =>
        val nullHandled = if (!assignment.key.nullable && assignment.value.nullable) {
          AssertNotNull(assignment.value)
        } else {
          assignment.value
        }
        val casted = if (assignment.key.dataType != nullHandled.dataType) {
          val cast = Cast(nullHandled, assignment.key.dataType, ansiEnabled = true)
          cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
          cast
        } else {
          nullHandled
        }
        val rawKeyType = assignment.key.transform {
          case a: AttributeReference =>
            CharVarcharUtils.getRawType(a.metadata).map(a.withDataType).getOrElse(a)
        }.dataType
        val finalValue = if (CharVarcharUtils.hasCharVarchar(rawKeyType)) {
          CharVarcharUtils.stringLengthCheck(casted, rawKeyType)
        } else {
          casted
        }
        val cleanedKey = assignment.key.transform {
          case a: AttributeReference => CharVarcharUtils.cleanAttrMetadata(a)
        }
        Assignment(cleanedKey, finalValue)
    }
  }

  private def alignActions(
      attrs: Seq[Attribute],
      actions: Seq[MergeAction],
      coerceNestedTypes: Boolean,
      ansiStoreAssignmentCastCheck: AnsiStoreAssignmentCastCheck): Seq[MergeAction] = {
    actions.map {
      case u @ UpdateAction(_, assignments, fromStar) =>
        u.copy(assignments = AssignmentUtils.alignUpdateAssignments(attrs, assignments,
          fromStar, coerceNestedTypes, ansiStoreAssignmentCastCheck))
      case d: DeleteAction =>
        d
      case i @ InsertAction(_, assignments) =>
        i.copy(assignments = AssignmentUtils.alignInsertAssignments(attrs, assignments,
          coerceNestedTypes, ansiStoreAssignmentCastCheck))
      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3052",
          messageParameters = Map("other" -> other.toString))
    }
  }
}
