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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, ExprId, If, Literal, MetadataAttribute, NamedExpression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Expand, LogicalPlan, MergeRows, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{GeneratedColumn, ReplaceDataProjections,
  WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.write.{RowLevelOperation, RowLevelOperationInfoImpl, RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

trait RewriteRowLevelCommand extends Rule[LogicalPlan] {

  private final val OPERATIONS_WITH_ROW =
    Set(UPDATE_OPERATION, REINSERT_OPERATION, INSERT_OPERATION, COPY_OPERATION)
  private final val OPERATIONS_WITH_METADATA =
    Set(DELETE_OPERATION, UPDATE_OPERATION, REINSERT_OPERATION, COPY_OPERATION)
  private final val OPERATIONS_WITH_ROW_ID =
    Set(DELETE_OPERATION, UPDATE_OPERATION)

  protected def groupFilterEnabled: Boolean = conf.runtimeRowLevelOperationGroupFilterEnabled

  /**
   * Throws if the catalog supports auto-filling generated columns on write and the table
   * has generated columns. MERGE and UPDATE with generated columns are not yet supported.
   *
   * This is intentionally coarse-grained: the whole statement is rejected whenever the target
   * has any generated column, even if the operation does not assign to a generated column. This
   * is because Spark cannot yet recompute generated column values for the rewritten rows, so it
   * fails fast rather than risk writing stale values. It can be relaxed once recomputation is
   * implemented for these operations.
   */
  protected def checkNoGeneratedColumns(
      relation: DataSourceV2Relation,
      command: Command): Unit = {
    if (GeneratedColumn.supportsGeneratedColumnsOnWrite(
        relation.table, relation.table.columns())) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        relation.catalog.get, relation.identifier.get,
        s"${command.toString} with generated columns")
    }
  }

  protected def buildOperationTable(
      table: SupportsRowLevelOperations,
      command: Command,
      options: CaseInsensitiveStringMap): RowLevelOperationTable = {
    val info = RowLevelOperationInfoImpl(command, options)
    val operation = table.newRowLevelOperationBuilder(info).build()
    RowLevelOperationTable(table, operation)
  }

  protected def buildRelationWithAttrs(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      metadataAttrs: Seq[AttributeReference],
      rowIdAttrs: Seq[AttributeReference] = Nil): DataSourceV2Relation = {

    val attrs = dedupAttrs(relation.output ++ rowIdAttrs ++ metadataAttrs)
    relation.copy(table = table, output = attrs)
  }

  protected def dedupAttrs(attrs: Seq[AttributeReference]): Seq[AttributeReference] = {
    val exprIds = mutable.Set.empty[ExprId]
    attrs.flatMap { attr =>
      if (exprIds.contains(attr.exprId)) {
        None
      } else {
        exprIds += attr.exprId
        Some(attr)
      }
    }
  }

  protected case class ResolvedConnectorRefs(
      attrs: Seq[AttributeReference],
      extractionAliases: Seq[Alias],
      scanAttrs: Seq[AttributeReference]) {

    lazy val extractionAttrs: Seq[AttributeReference] = extractionAliases.map { alias =>
      attrs.find(_.exprId == alias.exprId).getOrElse {
        throw SparkException.internalError(s"Cannot find extracted attribute: $alias")
      }
    }

    def rowIdAttrs: Seq[AttributeReference] = attrs
  }

  protected case class ResolvedDeltaRefs(
      rowIdRefs: ResolvedConnectorRefs,
      metadataRefs: ResolvedConnectorRefs)

  protected case class RowDeltaPlan(
      plan: LogicalPlan,
      bindings: Map[ExprId, Attribute],
      originalRowIdBindings: Map[ExprId, Attribute] = Map.empty) {

    def projectionAttrs(attrs: Seq[Attribute]): Seq[Attribute] = {
      attrs.map(attr => binding(attr.exprId))
    }

    def rowIdProjectionAttrs(attrs: Seq[Attribute]): Seq[Attribute] = {
      attrs.map { attr =>
        originalRowIdBindings.getOrElse(attr.exprId, binding(attr.exprId))
      }
    }

    private def binding(exprId: ExprId): Attribute = {
      bindings.getOrElse(exprId,
        throw SparkException.internalError(s"Cannot find projected attribute: $exprId"))
    }
  }

  protected def buildRowDeltaPlan(
      baseAttrs: Seq[Attribute],
      outputs: Seq[Seq[Expression]],
      rowIdRefs: ResolvedConnectorRefs,
      originalRowIdValues: Seq[Alias] = Nil)(
      buildPlan: Seq[Attribute] => LogicalPlan): RowDeltaPlan = {
    val attrs = baseAttrs ++ rowIdRefs.extractionAttrs
    outputs.find(_.size != attrs.size).foreach { output =>
      throw SparkException.internalError(
        s"Expected ${attrs.size} row delta values, but found ${output.size}")
    }
    val output = generateExpandOutput(attrs, outputs)
    bindRowDeltaPlan(buildPlan(output), attrs, output, originalRowIdValues)
  }

  protected def bindRowDeltaPlan(
      plan: LogicalPlan,
      sourceAttrs: Seq[Attribute],
      outputAttrs: Seq[Attribute],
      originalRowIdValues: Seq[Alias] = Nil): RowDeltaPlan = {
    if (sourceAttrs.size != outputAttrs.size) {
      throw SparkException.internalError(
        s"Expected ${sourceAttrs.size} projected attributes, but found ${outputAttrs.size}")
    }
    val bindings = sourceAttrs.zip(outputAttrs).map { case (sourceAttr, outputAttr) =>
      sourceAttr.exprId -> outputAttr
    }.toMap
    val originalRowIdBindings = originalRowIdValues.map { alias =>
      val originalAttr = alias.child.asInstanceOf[Attribute]
      originalAttr.exprId -> bindings(alias.exprId)
    }.toMap
    RowDeltaPlan(plan, bindings, originalRowIdBindings)
  }

  private def resolveConnectorRefs(
      relation: DataSourceV2Relation,
      refs: Seq[NamedReference]): ResolvedConnectorRefs = {
    resolveConnectorRefs(relation, refs, V2ExpressionUtils.resolveMetadataRef(_, relation))
  }

  private def resolveConnectorRefs(
      relation: DataSourceV2Relation,
      refs: Seq[NamedReference],
      refResolver: NamedReference => NamedExpression): ResolvedConnectorRefs = {
    val attrs = mutable.ArrayBuffer.empty[AttributeReference]
    val extractionAliases = mutable.ArrayBuffer.empty[Alias]
    val scanAttrs = mutable.ArrayBuffer.empty[AttributeReference]
    refs.foreach { ref =>
      refResolver(ref) match {
        case attr: AttributeReference =>
          attrs += attr
          scanAttrs += attr
        case alias: Alias =>
          extractionAliases += alias
          scanAttrs ++= alias.references.collect { case scanRef: AttributeReference => scanRef }
          alias.toAttribute match {
            case attr: AttributeReference => attrs += attr
            case other =>
              throw SparkException.internalError(s"Connector reference did not resolve: $other")
          }
        case other =>
          throw SparkException.internalError("Unexpected resolved row-level reference: " + other)
      }
    }
    ResolvedConnectorRefs(attrs.toSeq, extractionAliases.toSeq, dedupAttrs(scanAttrs.toSeq))
  }

  protected def resolveRequiredMetadataRefs(
      relation: DataSourceV2Relation,
      operation: RowLevelOperation): ResolvedConnectorRefs = {
    resolveConnectorRefs(relation, operation.requiredMetadataAttributes.toImmutableArraySeq)
  }

  protected def resolveDeltaRefs(
      relation: DataSourceV2Relation,
      operation: SupportsDelta): ResolvedDeltaRefs = {
    val resolvedRefs = mutable.ArrayBuffer.empty[(NamedReference, NamedExpression)]
    // Resolve a reference declared as both row ID and metadata once so both roles share its exprId.
    def resolveRef(ref: NamedReference): NamedExpression = {
      resolvedRefs.collectFirst {
        case (resolvedRef, resolved) if sameRef(resolvedRef, ref) => resolved
      }.getOrElse {
        val resolved = V2ExpressionUtils.resolveMetadataRef(ref, relation)
        resolvedRefs += ref -> resolved
        resolved
      }
    }

    val rowIdRefs = resolveConnectorRefs(
      relation, operation.rowId.toImmutableArraySeq, resolveRef)
    val metadataRefs = resolveConnectorRefs(
      relation, operation.requiredMetadataAttributes.toImmutableArraySeq, resolveRef)
    val nullableRowIdAttrs = rowIdRefs.rowIdAttrs.filter(_.nullable)
    if (nullableRowIdAttrs.nonEmpty) {
      throw QueryCompilationErrors.nullableRowIdError(nullableRowIdAttrs)
    }
    ResolvedDeltaRefs(rowIdRefs, metadataRefs)
  }

  private def sameRef(left: NamedReference, right: NamedReference): Boolean = {
    left.fieldNames.length == right.fieldNames.length &&
      left.fieldNames.zip(right.fieldNames).forall { case (leftName, rightName) =>
        conf.resolver(leftName, rightName)
      }
  }

  protected def withExtractedRefs(
      plan: LogicalPlan,
      extractionAliases: Seq[Alias]): LogicalPlan = {
    val exprIds = mutable.Set.empty[ExprId]
    val aliases = extractionAliases.filter(alias => exprIds.add(alias.exprId))
    if (aliases.isEmpty) {
      plan
    } else {
      Project(plan.output ++ aliases, plan)
    }
  }

  protected def projectWithExtractedRefs(
      plan: LogicalPlan,
      output: Seq[Attribute],
      extractionAliases: Seq[Alias]): LogicalPlan = {
    Project(output, withExtractedRefs(plan, extractionAliases))
  }

  protected def deltaDeleteOutput(
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute],
      originalRowIdValues: Seq[Expression] = Seq.empty): Seq[Expression] = {
    val rowValues = buildDeltaDeleteRowValues(rowAttrs, rowIdAttrs)
    val metadataValues = nullifyMetadataOnDelete(metadataAttrs, rowIdAttrs)
    Seq(Literal(DELETE_OPERATION)) ++ rowValues ++ metadataValues ++ originalRowIdValues
  }

  protected def nullifyMetadataOnDelete(
      attrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute] = Nil): Seq[NamedExpression] = {
    nullifyMetadata(attrs, rowIdAttrs, MetadataAttribute.isPreservedOnDelete)
  }

  protected def nullifyMetadataOnUpdate(
      attrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute] = Nil): Seq[NamedExpression] = {
    nullifyMetadata(attrs, rowIdAttrs, MetadataAttribute.isPreservedOnUpdate)
  }

  private def nullifyMetadataOnReinsert(attrs: Seq[Attribute]): Seq[NamedExpression] = {
    nullifyMetadata(attrs, Nil, MetadataAttribute.isPreservedOnReinsert)
  }

  private def nullifyMetadata(
      attrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      shouldPreserve: Attribute => Boolean): Seq[NamedExpression] = {
    val rowIdAttrSet = AttributeSet(rowIdAttrs)
    attrs.map {
      // A row ID must remain available to identify the affected row.
      case attr if rowIdAttrSet.contains(attr) =>
        attr
      case MetadataAttribute(attr) if !shouldPreserve(attr) =>
        // keep the exprId so the projection binds this by id, not by a name a row id may share
        Alias(Literal(null, attr.dataType), attr.name)(
          exprId = attr.exprId, explicitMetadata = Some(attr.metadata))
      case attr =>
        attr
    }
  }

  private def buildDeltaDeleteRowValues(
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute]): Seq[Expression] = {

    // nullify all row attrs that don't belong to row ID
    val rowIdAttSet = AttributeSet(rowIdAttrs)
    rowAttrs.map {
      case attr if rowIdAttSet.contains(attr) => attr
      case attr => Literal(null, attr.dataType)
    }
  }

  protected def deltaInsertOutput(
      assignments: Seq[Assignment],
      metadataAttrs: Seq[Attribute],
      originalRowIdValues: Seq[Expression] = Seq.empty): Seq[Expression] = {
    val rowValues = assignments.map(_.value)
    val extraNullValues = (metadataAttrs ++ originalRowIdValues).map(e => Literal(null, e.dataType))
    Seq(Literal(INSERT_OPERATION)) ++ rowValues ++ extraNullValues
  }

  protected def deltaUpdateOutput(
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute],
      originalRowIdValues: Seq[Expression]): Seq[Expression] = {
    val rowValues = assignments.map(_.value)
    val metadataValues = nullifyMetadataOnUpdate(metadataAttrs, rowIdAttrs)
    Seq(Literal(UPDATE_OPERATION)) ++ rowValues ++ metadataValues ++ originalRowIdValues
  }

  protected def deltaReinsertOutput(
      assignments: Seq[Assignment],
      metadataAttrs: Seq[Attribute],
      originalRowIdValues: Seq[Expression] = Seq.empty): Seq[Expression] = {
    val rowValues = assignments.map(_.value)
    val metadataValues = nullifyMetadataOnReinsert(metadataAttrs)
    val extraNullValues = originalRowIdValues.map(e => Literal(null, e.dataType))
    Seq(Literal(REINSERT_OPERATION)) ++ rowValues ++ metadataValues ++ extraNullValues
  }

  protected def addOperationColumn(operation: Int, plan: LogicalPlan): LogicalPlan = {
    val operationType = Alias(Literal(operation, IntegerType), OPERATION_COLUMN)()
    Project(operationType +: plan.output, plan)
  }

  protected def buildReplaceDataProjections(
      plan: LogicalPlan,
      rowAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): ReplaceDataProjections = {
    val outputs = extractOutputs(plan)
    val rowProjectionAttrs = plan.output.slice(1, 1 + rowAttrs.size)
    val metadataProjectionAttrs =
      plan.output.slice(1 + rowAttrs.size, 1 + rowAttrs.size + metadataAttrs.size)

    val outputsWithRow = filterOutputs(outputs, OPERATIONS_WITH_ROW)
    val rowProjection = newLazyProjection(plan, outputsWithRow, rowProjectionAttrs, rowAttrs)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      val outputsWithMetadata = filterOutputs(outputs, OPERATIONS_WITH_METADATA)
      Some(newLazyProjection(
        plan, outputsWithMetadata, metadataProjectionAttrs, metadataAttrs))
    } else {
      None
    }

    ReplaceDataProjections(rowProjection, metadataProjection)
  }

  protected def buildWriteDeltaProjections(
      rowDelta: RowDeltaPlan,
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): WriteDeltaProjections = {
    val plan = rowDelta.plan
    val outputs = extractOutputs(plan)

    val rowProjection = if (rowAttrs.nonEmpty) {
      val outputsWithRow = filterOutputs(outputs, OPERATIONS_WITH_ROW)
      Some(newLazyProjection(
        plan, outputsWithRow, rowDelta.projectionAttrs(rowAttrs), rowAttrs))
    } else {
      None
    }

    val outputsWithRowId = filterOutputs(outputs, OPERATIONS_WITH_ROW_ID)
    val rowIdProjection = newLazyProjection(
      plan, outputsWithRowId, rowDelta.rowIdProjectionAttrs(rowIdAttrs), rowIdAttrs)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      val outputsWithMetadata = filterOutputs(outputs, OPERATIONS_WITH_METADATA)
      Some(newLazyProjection(
        plan, outputsWithMetadata, rowDelta.projectionAttrs(metadataAttrs), metadataAttrs))
    } else {
      None
    }

    WriteDeltaProjections(rowProjection, rowIdProjection, metadataProjection)
  }

  private def extractOutputs(plan: LogicalPlan): Seq[Seq[Expression]] = {
    plan match {
      case p: Project => Seq(p.projectList)
      case e: Expand => e.projections
      case m: MergeRows => m.outputs
      case u: Union => u.children.flatMap(extractOutputs)
      case _ => throw SparkException.internalError("Can't extract outputs from plan: " + plan)
    }
  }

  private def filterOutputs(
      outputs: Seq[Seq[Expression]],
      operations: Set[Int]): Seq[Seq[Expression]] = {
    def matches(expr: Expression): Boolean = expr match {
      case Literal(operation: Integer, _) => operations.contains(operation)
      case Alias(child, _) => matches(child)
      case If(_, trueValue, falseValue) => matches(trueValue) && matches(falseValue)
      case other => throw SparkException.internalError("Can't determine operation: " + other)
    }
    outputs.filter(output => matches(output.head))
  }

  private def newLazyProjection(
      plan: LogicalPlan,
      outputs: Seq[Seq[Expression]],
      projectionAttrs: Seq[Attribute],
      schemaAttrs: Seq[Attribute]): ProjectingInternalRow = {
    if (projectionAttrs.size != schemaAttrs.size) {
      throw SparkException.internalError(
        s"Expected ${schemaAttrs.size} projection attributes, but found ${projectionAttrs.size}")
    }
    val colOrdinals = projectionAttrs.map { attr =>
      val ordinal = findColOrdinalByExprId(plan, attr.exprId)
      if (ordinal == -1) {
        throw SparkException.internalError(s"Cannot find projection attribute: $attr")
      }
      ordinal
    }
    createProjectingInternalRow(outputs, colOrdinals, schemaAttrs)
  }

  private def createProjectingInternalRow(
      outputs: Seq[Seq[Expression]],
      colOrdinals: Seq[Int],
      attrs: Seq[Attribute]): ProjectingInternalRow = {
    val schema = StructType(attrs.zipWithIndex.map { case (attr, index) =>
      val nullable = outputs.exists(output => output(colOrdinals(index)).nullable)
      StructField(attr.name, attr.dataType, nullable, attr.metadata)
    })
    ProjectingInternalRow(schema, colOrdinals)
  }

  private def findColOrdinalByExprId(plan: LogicalPlan, exprId: ExprId): Int = {
    plan.output.indexWhere(_.exprId == exprId)
  }

  protected def buildOriginalRowIdValues(
      rowIdAttrs: Seq[Attribute],
      assignments: Seq[Assignment]): Seq[Alias] = {
    val rowIdAttrSet = AttributeSet(rowIdAttrs)
    assignments.flatMap { assignment =>
      val key = assignment.key.asInstanceOf[Attribute]
      val value = assignment.value
      if (rowIdAttrSet.contains(key) && !key.semanticEquals(value)) {
        Some(Alias(key, ORIGINAL_ROW_ID_VALUE_PREFIX + key.name)())
      } else {
        None
      }
    }
  }

  // generates output attributes with fresh expr IDs and correct nullability for nodes like Expand
  // and MergeRows where there are multiple outputs for each input row
  protected def generateExpandOutput(
      attrs: Seq[Attribute],
      outputs: Seq[Seq[Expression]]): Seq[Attribute] = {

    // build a correct nullability map for output attributes
    // an attribute is nullable if at least one output may produce null
    val nullabilityMap = attrs.indices.map { index =>
      index -> outputs.exists(output => output(index).nullable)
    }.toMap

    attrs.zipWithIndex.map { case (attr, index) =>
      AttributeReference(attr.name, attr.dataType, nullabilityMap(index), attr.metadata)()
    }
  }
}
