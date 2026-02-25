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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructField, StructType}


/**
 * A rule that resolves schema evolution for MERGE INTO.
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveMergeIntoSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // This rule should run only if all assignments are resolved, except those
    // that will be satisfied by schema evolution
    case m@MergeIntoTable(_, _, _, _, _, _, _) if m.evaluateSchemaEvolution =>
      val changes = m.changesForSchemaEvolution
      if (changes.isEmpty) {
        m
      } else {
        val finalAttrMapping = ArrayBuffer.empty[(Attribute, Attribute)]
        val newTarget = m.targetTable.transform {
          case r: DataSourceV2Relation =>
            val referencedSourceSchema = MergeIntoTable.sourceSchemaForSchemaEvolution(m)
            val newTarget =
              ResolveSchemaEvolution.performSchemaEvolution(r, referencedSourceSchema, changes)
            val oldTargetOutput = m.targetTable.output
            val newTargetOutput = newTarget.output
            val attributeMapping = oldTargetOutput.zip(newTargetOutput)
            finalAttrMapping ++= attributeMapping
            newTarget
        }
        val res = m.copy(targetTable = newTarget)
        res.rewriteAttrs(AttributeMap(finalAttrMapping.toSeq))
      }
  }
}

/**
 * A rule that resolves schema evolution for V2 INSERT commands.
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveInsertSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case v2Write: V2WriteCommand
        if v2Write.table.resolved && v2Write.query.resolved && v2Write.schemaEvolutionEnabled =>
      val changes = v2Write.changesForSchemaEvolution
      if (changes.isEmpty) {
        v2Write
      } else {
        EliminateSubqueryAliases(v2Write.table) match {
          case r: DataSourceV2Relation =>
            val newRelation = ResolveSchemaEvolution.performSchemaEvolution(
              r, v2Write.query.schema, changes, isByName = v2Write.isByName)
            val attrMapping: Seq[(Attribute, Attribute)] =
              r.output.zip(newRelation.output)
            v2Write.withNewTable(newRelation).rewriteAttrs(AttributeMap(attrMapping))
          case _ => v2Write
        }
      }
  }
}

/**
 * Shared schema evolution utilities used by both MERGE INTO and INSERT schema evolution rules.
 */
object ResolveSchemaEvolution extends Logging {

  /**
   * Applies schema evolution changes to a DSV2 relation by altering the table schema
   * through the catalog, then verifying all changes were applied.
   */
  def performSchemaEvolution(
      relation: DataSourceV2Relation,
      referencedSourceSchema: StructType,
      changes: Array[TableChange],
      isByName: Boolean = true): DataSourceV2Relation = {
    (relation.catalog, relation.identifier) match {
      case (Some(c: TableCatalog), Some(i)) =>
        c.alterTable(i, changes: _*)
        val newTable = c.loadTable(i)
        val newSchema = CatalogV2Util.v2ColumnsToStructType(newTable.columns())
        // Check if there are any remaining changes not applied.
        val remainingChanges =
          schemaChanges(newSchema, referencedSourceSchema, isByName = isByName)
        if (remainingChanges.nonEmpty) {
          throw QueryCompilationErrors.unsupportedTableChangesInAutoSchemaEvolutionError(
            remainingChanges, i.toQualifiedNameParts(c))
        }
        relation.copy(table = newTable, output = DataTypeUtils.toAttributes(newSchema))
      case _ => logWarning(s"Schema Evolution enabled but data source $relation " +
        s"does not support it, skipping.")
        relation
    }
  }

  /**
   * Computes the set of table changes needed to evolve `originalTarget` schema
   * to accommodate `originalSource` schema. When `isByName` is true, fields are matched
   * by name. When false, fields are matched by position.
   */
  def schemaChanges(
      originalTarget: StructType,
      originalSource: StructType,
      isByName: Boolean): Array[TableChange] =
    schemaChanges(originalTarget, originalSource, originalTarget, originalSource,
      fieldPath = Array(), isByName = isByName)

  private def schemaChanges(
      current: DataType,
      newType: DataType,
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: Array[String],
      isByName: Boolean): Array[TableChange] = {
    (current, newType) match {
      case (StructType(currentFields), StructType(newFields)) =>
        if (isByName) {
          schemaChangesByName(
            currentFields, newFields, originalTarget, originalSource, fieldPath)
        } else {
          schemaChangesByPosition(
            currentFields, newFields, originalTarget, originalSource, fieldPath)
        }

      case (ArrayType(currentElementType, _), ArrayType(newElementType, _)) =>
        schemaChanges(currentElementType, newElementType,
          originalTarget, originalSource, fieldPath ++ Seq("element"), isByName)

      case (MapType(currentKeyType, currentElementType, _),
      MapType(updateKeyType, updateElementType, _)) =>
        schemaChanges(currentKeyType, updateKeyType, originalTarget, originalSource,
          fieldPath ++ Seq("key"), isByName) ++
          schemaChanges(currentElementType, updateElementType,
            originalTarget, originalSource, fieldPath ++ Seq("value"), isByName)

      case (currentType: AtomicType, newType: AtomicType) if currentType != newType =>
        Array(TableChange.updateColumnType(fieldPath, newType))

      case (currentType, newType) if currentType == newType =>
        // No change needed
        Array.empty[TableChange]

      case _ =>
        // Do not support change between atomic and complex types for now
        throw QueryExecutionErrors.failedToMergeIncompatibleSchemasError(
          originalTarget, originalSource, null)
    }
  }

  /** Match fields by name: look up each target field in the source by name to collect schema
   * differences. Nested struct fields are also matched by name.
   */
  private def schemaChangesByName(
      currentFields: Array[StructField],
      newFields: Array[StructField],
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: Array[String]): Array[TableChange] = {
    val newFieldMap = toFieldMap(newFields)

    // Update existing field types
    val updates = currentFields.collect {
      case currentField: StructField if newFieldMap.contains(currentField.name) =>
        schemaChanges(currentField.dataType, newFieldMap(currentField.name).dataType,
          originalTarget, originalSource, fieldPath ++ Seq(currentField.name), isByName = true)
    }.flatten

    // Identify the newly added fields and append to the end
    val currentFieldMap = toFieldMap(currentFields)
    val adds = newFields.filterNot(f => currentFieldMap.contains(f.name))
      // Make the type nullable, since existing rows in the table will have NULLs for this column.
      .map(f => TableChange.addColumn(fieldPath ++ Set(f.name), f.dataType.asNullable))

    updates ++ adds
  }

  /**
   * Match fields by position: pair target and source fields in order to collect schema
   * differences. Nested struct fields are also matched by position.
   */
  private def schemaChangesByPosition(
      currentFields: Array[StructField],
      newFields: Array[StructField],
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: Array[String]): Array[TableChange] = {
    // Update existing field types by pairing fields at the same position.
    val updates = currentFields.zip(newFields).flatMap { case (currentField, newField) =>
      schemaChanges(currentField.dataType, newField.dataType,
        originalTarget, originalSource,
        fieldPath ++ Seq(currentField.name), isByName = false)
    }

    // Extra source fields beyond the target's field count are new additions.
    val adds = newFields.drop(currentFields.length)
      // Make the type nullable, since existing rows in the table will have NULLs for this column.
      .map(f => TableChange.addColumn(fieldPath ++ Set(f.name), f.dataType.asNullable))

    updates ++ adds
  }

  def toFieldMap(fields: Array[StructField]): Map[String, StructField] = {
    val fieldMap = fields.map(field => field.name -> field).toMap
    if (SQLConf.get.caseSensitiveAnalysis) {
      fieldMap
    } else {
      CaseInsensitiveMap(fieldMap)
    }
  }
}
