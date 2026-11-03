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

import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils

abstract class ResolveInsertionBase extends Rule[LogicalPlan] {
  def resolver: Resolver = conf.resolver

  /** Add a project to use the table column names for INSERT INTO BY NAME */
  protected def createProjectForByNameQuery(
      tblName: String,
      i: InsertIntoStatement): LogicalPlan = {
    SchemaUtils.checkColumnNameDuplication(i.userSpecifiedCols, resolver)

    if (i.userSpecifiedCols.size != i.query.output.size) {
      if (i.userSpecifiedCols.size > i.query.output.size) {
        throw QueryCompilationErrors.cannotWriteNotEnoughColumnsToTableError(
          tblName, i.userSpecifiedCols, i.query.output)
      } else {
        throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
          tblName, i.userSpecifiedCols, i.query.output)
      }
    }
    val projectByName = i.userSpecifiedCols.zip(i.query.output)
      .map { case (userSpecifiedCol, queryOutputCol) =>
        val resolvedCol = i.table.resolve(Seq(userSpecifiedCol), resolver)
          .getOrElse(
            throw QueryCompilationErrors.unresolvedAttributeError(
              "UNRESOLVED_COLUMN", userSpecifiedCol, i.table.output.map(_.name), i.origin))
        (queryOutputCol.dataType, resolvedCol.dataType) match {
          case (input: StructType, expected: StructType) =>
            // Rename inner fields of the input column to pass the by-name INSERT analysis.
            val renamedType = renameFieldsInStruct(tblName, input, expected, Seq(resolvedCol.name))
            Alias(Cast(queryOutputCol, renamedType), resolvedCol.name)()
          case (input: ArrayType, expected: ArrayType) =>
            val renamedType = renameFieldsInType(tblName, input, expected, Seq(resolvedCol.name))
            Alias(Cast(queryOutputCol, renamedType), resolvedCol.name)()
          case (input: MapType, expected: MapType) =>
            val renamedType = renameFieldsInType(tblName, input, expected, Seq(resolvedCol.name))
            Alias(Cast(queryOutputCol, renamedType), resolvedCol.name)()
          case _ =>
            Alias(queryOutputCol, resolvedCol.name)()
        }
      }
    Project(projectByName, i.query)
  }

  private def renameFieldsInStruct(
      tableName: String,
      input: StructType,
      expected: StructType,
      colPath: Seq[String]): StructType = {
    if (input.length == expected.length || resolveDifferentArityByPosition) {
      val renamedFields = input.zip(expected).map { case (f1, f2) =>
        f1.copy(
          name = f2.name,
          dataType = renameFieldsInType(
            tableName, f1.dataType, f2.dataType, colPath :+ f2.name))
      }
      val retainedFields = input.drop(expected.length)
      findRenameCreatedCollision(input, renamedFields, retainedFields) match {
        case Some(collidingField) =>
          throw QueryCompilationErrors.incompatibleDataToTableAmbiguousColumnNameError(
            tableName, (colPath :+ collidingField.name).quoted)
        case None =>
      }
      StructType(renamedFields ++ retainedFields)
    } else {
      input
    }
  }

  // Positional renaming can introduce duplicates when the source has surplus fields. For
  // example, source {y, x, z} and target {x} produces {x, x, z}. Reject these collisions.
  private def findRenameCreatedCollision(
      originalFields: Seq[StructField],
      renamedFields: Seq[StructField],
      retainedFields: Seq[StructField]): Option[StructField] = {
    originalFields.zip(renamedFields).collectFirst {
      case (originalField, renamedField) if retainedFields.exists { retainedField =>
        !resolver(originalField.name, retainedField.name) &&
          resolver(renamedField.name, retainedField.name)
      } => renamedField
    }
  }

  private def resolveDifferentArityByPosition: Boolean =
    conf.getConf(SQLConf.INSERT_COLUMN_LIST_DIFFERENT_ARITY_STRUCTS_RESOLVE_BY_POSITION)

  // Recursively rename fields so positional INSERT analysis applies inside arrays and maps.
  private def renameFieldsInType(
      tableName: String,
      input: DataType,
      expected: DataType,
      colPath: Seq[String]): DataType =
    (input, expected) match {
      case (s1: StructType, s2: StructType) =>
        renameFieldsInStruct(tableName, s1, s2, colPath)
      case (ArrayType(e1, n1), ArrayType(e2, _)) =>
        ArrayType(renameFieldsInType(tableName, e1, e2, colPath :+ "element"), n1)
      case (MapType(k1, v1, n1), MapType(k2, v2, _)) =>
        MapType(
          renameFieldsInType(tableName, k1, k2, colPath :+ "key"),
          renameFieldsInType(tableName, v1, v2, colPath :+ "value"),
          n1)
      case _ =>
        input
    }
}
