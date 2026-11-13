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

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterColumnSpec, AlterTableClusterBy, AlterTableCommand, QualifiedColType, RenameColumn, ReplaceColumns}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, TypeUtils}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils

/** Validates ALTER TABLE commands after their tables and columns have been resolved. */
private[sql] object AlterTableValidation {
  def validate(
      alter: AlterTableCommand,
      errors: QueryErrorsBase): Unit = {
    def checkColumnNotExists(op: String, fieldNames: Seq[String], struct: StructType): Unit = {
      if (struct.findNestedField(
          fieldNames, includeCollections = true, alter.conf.resolver).isDefined) {
        alter.failAnalysis(
          errorClass = "FIELD_ALREADY_EXISTS",
          messageParameters = Map(
            "op" -> op,
            "fieldNames" -> errors.toSQLId(fieldNames),
            "struct" -> errors.toSQLType(struct)))
      }
    }

    def checkColumnNameDuplication(colsToAdd: Seq[QualifiedColType]): Unit = {
      SchemaUtils.checkColumnNameDuplication(
        colsToAdd.map(_.name.quoted),
        alter.conf.resolver)
    }

    def checkNoCollationsInMapKeys(colsToAdd: Seq[QualifiedColType]): Unit = {
      if (!alter.conf.allowCollationsInMapKeys) {
        colsToAdd.foreach(col => SchemaUtils.checkNoCollationsInMapKeys(col.dataType))
      }
    }

    alter match {
      case AddColumns(table: ResolvedTable, colsToAdd) =>
        colsToAdd.foreach { colToAdd =>
          checkColumnNotExists("add", colToAdd.name, table.schema)
        }
        checkColumnNameDuplication(colsToAdd)
        checkNoCollationsInMapKeys(colsToAdd)

      case ReplaceColumns(_: ResolvedTable, colsToAdd) =>
        checkColumnNameDuplication(colsToAdd)
        checkNoCollationsInMapKeys(colsToAdd)

      case RenameColumn(table: ResolvedTable, col: ResolvedFieldName, newName) =>
        checkColumnNotExists("rename", col.path :+ newName, table.schema)

      case AlterColumns(table: ResolvedTable, specs, _) =>
        AlterColumns.findRepeatedColumn(specs).foreach { name =>
          alter.failAnalysis(
            errorClass = "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
            messageParameters = Map(
              "table" -> errors.toSQLId(table.name),
              "fieldName" -> errors.toSQLId(name)))
        }
        specs.foreach {
          case AlterColumnSpec(col: ResolvedFieldName, dataType, nullable, _, _, _, _, _) =>
            val fieldName = col.name.quoted
            if (dataType.isDefined) {
              val field = CharVarcharUtils.getRawType(col.field.metadata)
                .map(dt => col.field.copy(dataType = dt))
                .getOrElse(col.field)
              val newDataType = dataType.get
              newDataType match {
                case _: StructType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.STRUCT_TYPE",
                  Map(
                  "table" -> errors.toSQLId(table.name),
                  "fieldName" -> errors.toSQLId(fieldName)))
                case _: MapType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.MAP_TYPE",
                  Map(
                  "table" -> errors.toSQLId(table.name),
                  "fieldName" -> errors.toSQLId(fieldName)))
                case _: ArrayType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.ARRAY_TYPE",
                  Map(
                  "table" -> errors.toSQLId(table.name),
                  "fieldName" -> errors.toSQLId(fieldName)))
                case u: UserDefinedType[_] => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.USER_DEFINED_TYPE",
                  Map(
                    "table" -> errors.toSQLId(table.name),
                    "fieldName" -> errors.toSQLId(fieldName),
                    "udtSql" -> errors.toSQLType(u)))
                case _: CalendarIntervalType | _: AnsiIntervalType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.INTERVAL_TYPE",
                  Map(
                  "table" -> errors.toSQLId(table.name),
                  "fieldName" -> errors.toSQLId(fieldName)))
                case _ => // update is okay
              }

              // We don't need to handle nested types here which shall fail before.
              def canAlterColumnType(from: DataType, to: DataType): Boolean = (from, to) match {
                case (c1: CharType, c2: CharType) => c1.length == c2.length
                case (c: CharType, v: VarcharType) => c.length <= v.length
                case (v1: VarcharType, v2: VarcharType) => v1.length <= v2.length
                case _ => Cast.canUpCast(from, to)
              }
              if (!canAlterColumnType(field.dataType, newDataType)) {
                alter.failAnalysis(
                  errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
                  messageParameters = Map(
                    "table" -> errors.toSQLId(table.name),
                    "originName" -> errors.toSQLId(fieldName),
                    "originType" -> errors.toSQLType(field.dataType),
                    "newName" -> errors.toSQLId(fieldName),
                    "newType" -> errors.toSQLType(newDataType)))
              }
            }
            if (nullable.isDefined) {
              if (!nullable.get && col.field.nullable) {
                alter.failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2330",
                  messageParameters = Map("fieldName" -> fieldName))
              }
            }
          case _ =>
        }

      // FILE is not comparable, so it cannot be a clustering column.
      case AlterTableClusterBy(table: ResolvedTable, Some(clusterBySpec)) =>
        TypeUtils.checkNoFileTypeClusteringColumn(table.schema, clusterBySpec, alter.conf.resolver)
      case _ =>
    }
  }
}
