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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, NamedExpression, ProjectionOverSchema}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.EnhancedRequirementCollector.toRootFields
import org.apache.spark.sql.util.SchemaUtils.restoreOriginalOutputNames
import org.apache.spark.sql.catalyst.expressions.SchemaPruning
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.V1Scan
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, MetadataBuilder, StructType}

/**
 * SPARK-47230: Placeholder optimizer rule that will eventually consume [[PendingV2ScanRelation]]
 * nodes and finalize their schema once requirement analysis is complete.
 *
 * Currently acts as a no-op so the experimental pipeline remains gated until the
 * requirement-based pruning logic is implemented.
 */
object V2PendingScanFinalizer extends Rule[LogicalPlan] with Logging {
  private val GeneratorFullStructKey = "spark.generator.fullStruct"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    logDebug("V2PendingScanFinalizer executing")

    plan transform {
      case pendingPlan @ PendingScanPlan(projectList, _, _, pendingScan) =>
        logDebug(s"Finalizing PendingScanPlan id=${pendingScan.relationId}")
        logDebug(s"Finalizing PendingScanPlan for relation ${pendingScan.relation.table.name()}")
        val state = pendingScan.pushdownState
        val builder = pendingScan.withFreshBuilder()

        val normalizedProjects = state.normalizedProjects
        val normalizedFilters = state.normalizedFilters

        logDebug(s"Normalized projects for relation ${pendingScan.relationId}: " +
          normalizedProjects.map(_.toString()))
        logDebug(s"Normalized filters for relation ${pendingScan.relationId}: " +
          normalizedFilters.map(_.toString()))

        pendingScan.requirements.foreach { reqs =>
          logDebug(s"Requirements for relation ${pendingScan.relationId}: ${reqs.requirements}")
        }

        val rootFields =
          toRootFields(pendingScan.requirements, pendingScan.relation.schema)

        if (rootFields.nonEmpty) {
          val requirementDebug =
            rootFields
              .map(f => s"${f.field.name}:${f.field.dataType.catalogString}:${f.derivedFromAtt}")
              .mkString(",")
          logDebug(
            s"relation=${pendingScan.relationId} requirementRootFields=$requirementDebug")
        }

        val fallbackRootFields =
          SchemaPruning.identifyRootFields(normalizedProjects, normalizedFilters)
        if (fallbackRootFields.nonEmpty) {
          val fallbackDebug =
            fallbackRootFields
              .map(f => s"${f.field.name}:${f.field.dataType.catalogString}:${f.derivedFromAtt}")
              .mkString(",")
          logDebug(
            s"relation=${pendingScan.relationId} fallbackRootFields=$fallbackDebug")
        }

        val mergedRootFields = mergeRootFields(rootFields, fallbackRootFields)

        if (mergedRootFields.nonEmpty) {
          val mergedDebug =
            mergedRootFields
              .map(f => s"${f.field.name}:${f.field.dataType.catalogString}:${f.derivedFromAtt}")
              .mkString(",")
          logDebug(s"relation=${pendingScan.relationId} mergedRootFields=$mergedDebug")
        }

        val effectiveRootFields =
          if (mergedRootFields.nonEmpty) Some(mergedRootFields) else None

        val generatorFullStructs =
          pendingScan.requirements.map(_.generatorFullStructs).getOrElse(Map.empty)

        val (scan, output) = PushDownUtils.pruneColumns(
          builder,
          pendingScan.relation,
          normalizedProjects,
          normalizedFilters,
          effectiveRootFields)

        val adjustedOutput = output.map { attr =>
          generatorFullStructs.get(attr.exprId) match {
            case Some(struct) =>
              val metadata = new MetadataBuilder()
                .withMetadata(attr.metadata)
                .putString(GeneratorFullStructKey, struct.json)
                .build()
              attr.withMetadata(metadata)
            case None =>
              attr
          }
        }

        val effectiveSchema = scan.readSchema()
        println(s"!!!!! SCHEMA USED (relation=${pendingScan.relationId} " +
          s"${pendingScan.relation.table.name()}): ${effectiveSchema.catalogString}")

        val wrappedScan = wrapScanWithState(scan, pendingScan)
        val scanRelation = DataSourceV2ScanRelation(pendingScan.relation, wrappedScan, adjustedOutput)
        scanRelation.setTagValue(
          org.apache.spark.sql.execution.datasources.SchemaPruning.GeneratorFullStructTag,
          generatorFullStructs)

        val projectionOverSchema =
          ProjectionOverSchema(adjustedOutput.toStructType, AttributeSet(adjustedOutput))
        val projectionFunc = (expr: Expression) => expr transformDown {
          case projectionOverSchema(newExpr) => newExpr
        }

        val rewrittenFilters = normalizedFilters.map(projectionFunc)
        val withFilter = rewrittenFilters.foldLeft[LogicalPlan](scanRelation) {
          case (currentPlan, condition) => Filter(condition, currentPlan)
        }

        val finalPlan =
          if (withFilter.output != projectList) {
            val rewrittenProjects = normalizedProjects
              .map(projectionFunc)
              .asInstanceOf[Seq[NamedExpression]]
            val projectNode = Project(
              restoreOriginalOutputNames(rewrittenProjects, projectList.map(_.name)),
              withFilter)
            projectNode
          } else {
            withFilter
          }

        val columnPruned = ColumnPruning(finalPlan)

        columnPruned

      case pending: PendingV2ScanRelation =>
        pending.requirements match {
          case Some(reqs) =>
            logDebug(
              s"V2PendingScanFinalizer observed PendingV2ScanRelation id=${pending.relationId}, " +
                s"requirements=${reqs.requirements.size}")
          case None =>
            logWarning(
              s"V2PendingScanFinalizer missing requirements for PendingV2ScanRelation id=${pending.relationId}")
        }
        pending
    }
  }

  private def mergeRootFields(
      requirementFields: Seq[SchemaPruning.RootField],
      fallbackFields: Seq[SchemaPruning.RootField]): Seq[SchemaPruning.RootField] = {
    if (requirementFields.isEmpty) {
      return fallbackFields
    }

    val requirementByName = requirementFields
      .groupBy(_.field.name)
      .map { case (name, fields) => name -> collapseRootFields(fields) }
    val fallbackByName = fallbackFields
      .groupBy(_.field.name)
      .map { case (name, fields) => name -> collapseRootFields(fields) }

    val merged = mutable.ArrayBuffer[SchemaPruning.RootField]()

    requirementByName.foreach { case (name, reqField) =>
      val adjustedField = fallbackByName.get(name) match {
        case Some(fallbackField) =>
          val nullable = reqField.field.nullable || fallbackField.field.nullable
          SchemaPruning.RootField(
            reqField.field.copy(nullable = nullable),
            derivedFromAtt = reqField.derivedFromAtt)
        case None => reqField
      }
      merged += adjustedField
    }

    fallbackByName.foreach { case (name, fallbackField) =>
      if (!requirementByName.contains(name)) {
        merged += fallbackField
      }
    }

    merged.toSeq
  }

  private def collapseRootFields(
      fields: Seq[SchemaPruning.RootField]): SchemaPruning.RootField = {
    fields.reduce { (left, right) =>
      val mergedDataType = mergeDataTypes(left.field.dataType, right.field.dataType)
      val mergedField = left.field.copy(
        dataType = mergedDataType,
        nullable = left.field.nullable || right.field.nullable)
      SchemaPruning.RootField(
        mergedField,
        derivedFromAtt = left.derivedFromAtt || right.derivedFromAtt)
    }
  }

  private def mergeDataTypes(
      requirementType: DataType,
      fallbackType: DataType): DataType = (requirementType, fallbackType) match {
    case (reqStruct: StructType, fallbackStruct: StructType) =>
      val reqByName = reqStruct.fields.map(f => f.name -> f).toMap
      val fallbackByName = fallbackStruct.fields.map(f => f.name -> f).toMap
      val mergedPrimary = reqStruct.fields.map { field =>
        fallbackByName.get(field.name) match {
          case Some(fallbackField) =>
            val mergedChildType = mergeDataTypes(field.dataType, fallbackField.dataType)
            field.copy(
              dataType = mergedChildType,
              nullable = field.nullable || fallbackField.nullable)
          case None =>
            field
        }
      }

      val fallbackExtras =
        fallbackStruct.fields.filterNot(field => reqByName.contains(field.name))

      val mergedFields = mergedPrimary ++ fallbackExtras
      StructType(mergedFields)
    case (reqArray: ArrayType, fallbackArray: ArrayType) =>
      val mergedElementType = mergeDataTypes(reqArray.elementType, fallbackArray.elementType)
      ArrayType(mergedElementType, reqArray.containsNull || fallbackArray.containsNull)
    case (reqMap: MapType, fallbackMap: MapType) =>
      val mergedValueType = mergeDataTypes(reqMap.valueType, fallbackMap.valueType)
      MapType(
        reqMap.keyType,
        mergedValueType,
        reqMap.valueContainsNull || fallbackMap.valueContainsNull)
    case _ =>
      requirementType
  }

  private def wrapScanWithState(scan: Scan, pending: PendingV2ScanRelation): Scan = {
    scan match {
      case v1: V1Scan =>
        val state = pending.pushdownState
        val pushedDownOperators = PushedDownOperators(
          state.pushedAggregate,
          state.pushedSample,
          state.pushedLimit,
          state.pushedOffset,
          state.sortOrders,
          state.pushedPredicates)
        V1ScanWrapper(v1, state.pushedDataSourceFilters, pushedDownOperators)
      case other => other
    }
  }
}
