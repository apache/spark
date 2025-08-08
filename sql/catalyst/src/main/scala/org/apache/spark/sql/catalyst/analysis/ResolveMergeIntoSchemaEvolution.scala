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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{TableCatalog, TableChange}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}


/**
 * A rule that resolves schema evolution for MERGE INTO.
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveMergeIntoSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case m @ MergeIntoTable(aliasedTable, source, _, _, _, _, _)
      if m.needSchemaEvolution =>
        val newTarget = aliasedTable.transform {
          case r : DataSourceV2Relation => schemaEvolution(r, source)
        }
        m.copy(targetTable = newTarget)
  }

  private def schemaEvolution(relation: DataSourceV2Relation, source: LogicalPlan):
    DataSourceV2Relation = {
    (relation.catalog, relation.identifier) match {
      case (Some(c: TableCatalog), Some(i)) =>
        val changes = schemaChanges(relation.schema, source.schema)
        c.alterTable(i, changes: _*)
        val evolvedSchema = mergeSchema(relation.schema, source.schema)
        val newTable = c.loadTable(i)
        relation.copy(table = newTable, output = DataTypeUtils.toAttributes(evolvedSchema))
      case _ => logWarning("Schema Evolution enabled but data source does not support it"
        + s"data source: $relation)")
      relation
    }
  }

  private def schemaChanges(originalTarget: StructType, originalSource: StructType,
                            fieldPath: Array[String] = Array()): Array[TableChange] = {
    schemaChanges(originalTarget.asInstanceOf[DataType], originalSource.asInstanceOf[DataType],
      originalTarget, originalSource, fieldPath)
  }

  private def schemaChanges(current: DataType,
      newType: DataType,
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: Array[String]): Array[TableChange] = {
    (current, newType) match {
      case (StructType(currentFields), StructType(newFields)) =>
        val newFieldMap = toFieldMap(newFields)

        // Update existing field types
        val updates = {
          currentFields collect {
            case currentField: StructField if newFieldMap.contains(currentField.name) &&
              newFieldMap.get(currentField.name).exists(_.dataType != currentField.dataType) =>
              schemaChanges(currentField.dataType, newFieldMap(currentField.name).dataType,
                originalTarget, originalSource, fieldPath ++ Seq(currentField.name))
          }}.flatten

        // Identify the newly added fields and append to the end
        val currentFieldMap = toFieldMap(currentFields)
        val adds = newFields.filterNot (f => currentFieldMap.contains (f.name))
          .map(f => TableChange.addColumn(fieldPath ++ Set(f.name), f.dataType))

        updates ++ adds

      case (ArrayType(currentElementType, _), ArrayType(newElementType, _)) =>
        schemaChanges(currentElementType, newElementType,
          originalTarget, originalSource, fieldPath ++ Seq("element"))

      case (MapType(currentKeyType, currentElementType, _),
      MapType(updateKeyType, updateElementType, _)) =>
        schemaChanges(currentKeyType, updateKeyType, originalTarget, originalSource,
          fieldPath ++ Seq("key")) ++
          schemaChanges(currentElementType, updateElementType,
            originalTarget, originalSource, fieldPath ++ Seq("value"))

      case (_, _: DataType) =>
        // For now do not support type widening
        throw QueryExecutionErrors.failedToMergeIncompatibleSchemasError(
          originalTarget, originalSource, null)
    }
  }

  private def mergeSchema(targetSchema: StructType, sourceSchema: StructType): StructType = {
    mergeTypes(targetSchema, sourceSchema, targetSchema, sourceSchema).asInstanceOf[StructType]
  }

  private def mergeTypes(current: DataType, newType: DataType,
      originalTarget: StructType, originalSource: StructType): DataType = {
    (current, newType) match {
      case (StructType(currentFields), StructType(newFields)) =>
        val newFieldMap = toFieldMap(newFields)

        // Update existing field types
        val updatedCurrentFields = currentFields.map { currentField =>
          newFieldMap.get(currentField.name) match {
            case Some(newField) if newField.dataType != currentField.dataType =>
              val newType = mergeTypes(currentField.dataType, newField.dataType,
                originalTarget, originalSource)
              StructField(currentField.name, newType, currentField.nullable,
                currentField.metadata)
            case _ => currentField
          }
        }

        // Identify the newly added fields and append to the end
        val currentFieldMap = toFieldMap(currentFields)
        val remainingNewFields = newFields.filterNot (f => currentFieldMap.contains (f.name) )
        StructType( updatedCurrentFields ++ remainingNewFields )

      case (ArrayType(currentElementType, currentContainsNull), ArrayType(newElementType, _)) =>
        ArrayType(mergeTypes(currentElementType, newElementType, originalTarget, originalSource),
          currentContainsNull)

      case (MapType(currentKeyType, currentElementType, currentContainsNull),
      MapType(updateKeyType, updateElementType, _)) =>
        MapType(
          mergeTypes(currentKeyType, updateKeyType, originalTarget, originalSource),
          mergeTypes(currentElementType, updateElementType, originalTarget, originalSource),
          currentContainsNull)

      case (_, _) =>
        // For now do not support type widening
        throw QueryExecutionErrors.failedToMergeIncompatibleSchemasError(
          originalTarget, originalSource, null)
    }

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
