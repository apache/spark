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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsSchemaEvolution, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2CatalogAndIdentifier, ExtractV2Table}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, NullType, StructField, StructType}


/**
 * A rule that resolves schema evolution for V2 write commands (INSERT, MERGE INTO).
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND)) {
    // This rule should run only if all assignments are resolved, except those
    // that will be satisfied by schema evolution
    case write: WriteWithSchemaEvolution if write.pendingSchemaChanges.nonEmpty =>
      write.table match {
        case relation @ ExtractV2CatalogAndIdentifier(catalog, ident) =>
          evolveSchema(catalog, ident, write.pendingSchemaChanges)
          val newTable = catalog.loadTable(ident, write.writePrivileges.asJava)
          val writeWithNewTarget = replaceWriteTarget(write, relation, newTable)

          val remainingChanges = writeWithNewTarget.pendingSchemaChanges
          if (remainingChanges.nonEmpty) {
            throw QueryCompilationErrors.unsupportedAutoSchemaEvolutionChangesError(
              catalog, ident, remainingChanges)
          }

          writeWithNewTarget
        case _ =>
          throw QueryCompilationErrors.unsupportedAutoSchemaEvolutionError(write.table)
      }
  }

  private def evolveSchema(
      catalog: TableCatalog,
      ident: Identifier,
      changes: Seq[TableChange]): Unit = {
    try {
      catalog.alterTable(ident, changes: _*)
    } catch {
      case e: IllegalArgumentException if !e.isInstanceOf[SparkThrowable] =>
        throw QueryExecutionErrors.unsupportedTableChangeError(e)
      case NonFatal(e) =>
        throw QueryCompilationErrors.failedAutoSchemaEvolutionError(catalog, ident, e)
    }
  }

  private def replaceWriteTarget(
      write: WriteWithSchemaEvolution,
      relation: DataSourceV2Relation,
      newTable: Table): WriteWithSchemaEvolution = {
    val oldOutput = write.table.output
    val newOutput = DataTypeUtils.toAttributes(newTable.columns)
    val newRelation = relation.copy(table = newTable, output = newOutput)
    val writeWithNewTargetTable = write.withNewTable(newRelation)
    rewriteAttrs(writeWithNewTargetTable, oldOutput, newOutput)
  }

  private def rewriteAttrs[T <: LogicalPlan](
      plan: T,
      oldOutput: Seq[Attribute],
      newOutput: Seq[Attribute]): T = {
    val attrMap = AttributeMap(oldOutput.zip(newOutput))
    plan.rewriteAttrs(attrMap).asInstanceOf[T]
  }

  /**
   * Computes the set of table changes needed to evolve `targetTable`'s schema
   * to accommodate the `originalSource` schema. Only returns schema changes that are supported by
   * `targetTable`. When `isByName` is true, fields are matched by name. When false, fields are
   * matched by position.
   */
  def computeSupportedSchemaChanges(
      targetTable: LogicalPlan,
      originalSource: StructType,
      isByName: Boolean): Array[TableChange] = {
    val candidateChanges = computeSchemaChanges(
      targetTable.schema,
      originalSource,
      targetTable.schema,
      originalSource,
      fieldPath = Nil,
      isByName)
    targetTable match {
      case ExtractV2Table(t: SupportsSchemaEvolution) =>
        candidateChanges.filter {
          case change: ColumnChange => t.supportsColumnChange(change)
          // Reject other table changes.
          case _ => false
        }
      case r: DataSourceV2Relation if r.autoSchemaEvolution =>
        // If a table reports capability [[TableCapability.AUTOMATIC_SCHEMA_EVOLUTION]] but
        // doesn't implement [[SupportsSchemaEvolution]], attempt to apply all changes.
        candidateChanges
      case _ =>
        Array.empty
    }
  }

  private def computeSchemaChanges(
      currentType: DataType,
      newType: DataType,
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: List[String],
      isByName: Boolean): Array[TableChange] = {
    (currentType, newType) match {
      case (StructType(currentFields), StructType(newFields)) =>
        if (isByName) {
          computeSchemaChangesByName(
            currentFields, newFields, originalTarget, originalSource, fieldPath)
        } else {
          computeSchemaChangesByPosition(
            currentFields, newFields, originalTarget, originalSource, fieldPath)
        }

      case (ArrayType(currentElementType, _), ArrayType(newElementType, _)) =>
        computeSchemaChanges(
          currentElementType,
          newElementType,
          originalTarget,
          originalSource,
          fieldPath :+ "element",
          isByName)

      case (MapType(currentKeyType, currentValueType, _),
            MapType(newKeyType, newValueType, _)) =>
        val keyChanges = computeSchemaChanges(
          currentKeyType,
          newKeyType,
          originalTarget,
          originalSource,
          fieldPath :+ "key",
          isByName)
        val valueChanges = computeSchemaChanges(
          currentValueType,
          newValueType,
          originalTarget,
          originalSource,
          fieldPath :+ "value",
          isByName)
        keyChanges ++ valueChanges

      case (currentType: AtomicType, newType: AtomicType) if currentType != newType =>
        Array(TableChange.updateColumnType(fieldPath.toArray, newType))

      case (currentType, newType) if currentType == newType =>
        // No change needed
        Array.empty[TableChange]

      case (_, NullType) =>
        // Don't try to change to NullType.
        Array.empty[TableChange]

      case (_: AtomicType | NullType, newType: AtomicType) =>
        Array(TableChange.updateColumnType(fieldPath.toArray, newType))

      case _ =>
        // Do not support change between atomic and complex types for now
        throw QueryExecutionErrors.failedToMergeIncompatibleSchemasError(
          originalTarget, originalSource, null)
    }
  }

  /**
   * Match fields by name: look up each target field in the source by name to collect schema
   * differences. Nested struct fields are also matched by name.
   */
  private def computeSchemaChangesByName(
      currentFields: Array[StructField],
      newFields: Array[StructField],
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: List[String]): Array[TableChange] = {
    val currentFieldMap = toFieldMap(currentFields)
    val newFieldMap = toFieldMap(newFields)

    // Collect field updates
    val updates = currentFields
      .filter(f => newFieldMap.contains(f.name))
      .flatMap { f =>
        computeSchemaChanges(
          f.dataType,
          newFieldMap(f.name).dataType,
          originalTarget,
          originalSource,
          fieldPath :+ f.name,
          isByName = true)
      }

    // Collect newly added fields
    val adds = newFields
      .filterNot(f => currentFieldMap.contains(f.name))
      .map { f =>
        // Make the type nullable, since existing rows in the table will have NULLs for this column.
        TableChange.addColumn((fieldPath :+ f.name).toArray, f.dataType.asNullable)
      }

    updates ++ adds
  }

  /**
   * Match fields by position: pair target and source fields in order to collect schema
   * differences. Nested struct fields are also matched by position.
   */
  private def computeSchemaChangesByPosition(
      currentFields: Array[StructField],
      newFields: Array[StructField],
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: List[String]): Array[TableChange] = {
    // Update existing field types by pairing fields at the same position.
    val updates = currentFields.zip(newFields).flatMap { case (currentField, newField) =>
      computeSchemaChanges(
        currentField.dataType,
        newField.dataType,
        originalTarget,
        originalSource,
        fieldPath :+ currentField.name,
        isByName = false)
    }

    // Extra source fields beyond the target's field count are new additions.
    val adds = newFields.drop(currentFields.length)
      .map { f =>
        // Make the type nullable, since existing rows in the table will have NULLs for this column.
        TableChange.addColumn((fieldPath :+ f.name).toArray, f.dataType.asNullable)
      }

    updates ++ adds
  }

  private def toFieldMap(fields: Array[StructField]): Map[String, StructField] = {
    val fieldMap = fields.map(field => field.name -> field).toMap
    if (SQLConf.get.caseSensitiveAnalysis) {
      fieldMap
    } else {
      CaseInsensitiveMap(fieldMap)
    }
  }
}
