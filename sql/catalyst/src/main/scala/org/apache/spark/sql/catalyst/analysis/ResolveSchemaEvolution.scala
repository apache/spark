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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Table, TableChange, TableWritePrivilege}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.TableWritePrivilege.{DELETE, INSERT}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2CatalogAndIdentifier}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructField, StructType}


/**
 * A rule that resolves schema evolution for V2 write commands (INSERT, MERGE INTO).
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveSchemaEvolution extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    case write: V2WriteSchemaEvolution
      if write.canEvaluateSchemaEvolution && write.pendingSchemaChanges.nonEmpty =>
      write.table match {
        case ExtractV2CatalogAndIdentifier(catalog, ident) =>
          catalog.alterTable(ident, write.pendingSchemaChanges: _*)
          val writePrivileges = getWritePrivileges(write)
          val newTable = catalog.loadTable(ident, writePrivileges.asJava)
          val writeWithNewTarget = replaceWriteTargetTable(write, newTable)

          // Check if there are any remaining changes not applied.
          if (writeWithNewTarget.pendingSchemaChanges.nonEmpty) {
            throw QueryCompilationErrors.unsupportedTableChangesInAutoSchemaEvolutionError(
              writeWithNewTarget.pendingSchemaChanges, ident.toQualifiedNameParts(catalog))
          }
          writeWithNewTarget
        case _ => write
      }
  }

  private def getWritePrivileges(write: V2WriteSchemaEvolution): Set[TableWritePrivilege] =
    write match {
      case m: MergeIntoTable =>
        MergeIntoTable.getWritePrivileges(
          m.matchedActions,
          m.notMatchedActions,
          m.notMatchedBySourceActions
        ).toSet
      case _: AppendData => Set(INSERT)
      case _: OverwriteByExpression | _: OverwritePartitionsDynamic => Set(INSERT, DELETE)
      case _ =>
        throw SparkException.internalError(
          s"Attempting schema evolution on a command that does not support it: $write")
    }

  private def replaceWriteTargetTable(write: V2WriteSchemaEvolution, newTable: Table)
    : V2WriteSchemaEvolution = {
    val newRelation = write.table match {
      case r: DataSourceV2Relation =>
        val newSchema = CatalogV2Util.v2ColumnsToStructType(newTable.columns())
        r.copy(table = newTable, output = DataTypeUtils.toAttributes(newSchema))
    }
    val attrMapping = write.table.output.zip(newRelation.output)

    write match {
      case m: MergeIntoTable =>
        val newTarget = m.targetTable.transform {
          case _: DataSourceV2Relation => newRelation
        }
        m.copy(targetTable = newTarget)
          .rewriteAttrs(AttributeMap(attrMapping))
          .asInstanceOf[V2WriteSchemaEvolution]
      case w: V2WriteCommand =>
        w.withNewTable(newRelation)
          .rewriteAttrs(AttributeMap(attrMapping))
          .asInstanceOf[V2WriteSchemaEvolution]
    }
  }

  /**
   * Computes the set of table changes needed to evolve `originalTarget` schema
   * to accommodate `originalSource` schema. When `isByName` is true, fields are matched
   * by name. When false, fields are matched by position.
   */
  def computeSchemaChanges(
      originalTarget: StructType,
      originalSource: StructType,
      isByName: Boolean): Array[TableChange] =
    computeSchemaChanges(
      originalTarget,
      originalSource,
      originalTarget,
      originalSource,
      fieldPath = Nil,
      isByName = isByName
    )

  private def computeSchemaChanges(
      current: DataType,
      newType: DataType,
      originalTarget: StructType,
      originalSource: StructType,
      fieldPath: Seq[String],
      isByName: Boolean): Array[TableChange] = {
    (current, newType) match {
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
          isByName
        )

      case (MapType(currentKeyType, currentElementType, _),
      MapType(updateKeyType, updateElementType, _)) =>
        val keyChanges = computeSchemaChanges(
          currentKeyType,
          updateKeyType,
          originalTarget,
          originalSource,
          fieldPath :+ "key",
          isByName
        )
        val valueChanges = computeSchemaChanges(
          currentElementType,
          updateElementType,
          originalTarget,
          originalSource,
          fieldPath :+ "value",
          isByName
        )
        keyChanges ++ valueChanges

      case (currentType: AtomicType, newType: AtomicType) if currentType != newType =>
        Array(TableChange.updateColumnType(fieldPath.toArray, newType))

      case (currentType, newType) if currentType == newType =>
        // No change needed
        Array.empty[TableChange]

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
      fieldPath: Seq[String]): Array[TableChange] = {
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
          isByName = true
        )
      }

    // Collect newly added fields
    val adds = newFields
      .filterNot(f => currentFieldMap.contains(f.name))
      .map(f => TableChange.addColumn((fieldPath :+ f.name).toArray, f.dataType.asNullable))

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
      fieldPath: Seq[String]): Array[TableChange] = {
    // Update existing field types by pairing fields at the same position.
    val updates = currentFields.zip(newFields).flatMap { case (currentField, newField) =>
      computeSchemaChanges(
        currentField.dataType,
        newField.dataType,
        originalTarget,
        originalSource,
        fieldPath :+ currentField.name,
        isByName = false
      )
    }

    // Extra source fields beyond the target's field count are new additions.
    val adds = newFields.drop(currentFields.length)
      // Make the type nullable, since existing rows in the table will have NULLs for this column.
      .map(f => TableChange.addColumn((fieldPath :+ f.name).toArray, f.dataType.asNullable))

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
