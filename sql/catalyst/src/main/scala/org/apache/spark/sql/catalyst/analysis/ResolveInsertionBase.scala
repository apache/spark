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
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
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
          tblName, i.userSpecifiedCols, i.query)
      } else {
        throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
          tblName, i.userSpecifiedCols, i.query)
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
            Alias(Cast(queryOutputCol, renameFieldsInStruct(input, expected)), resolvedCol.name)()
          case _ =>
            Alias(queryOutputCol, resolvedCol.name)()
        }
      }
    Project(projectByName, i.query)
  }

  private def renameFieldsInStruct(input: StructType, expected: StructType): StructType = {
    if (input.length == expected.length) {
      val newFields = input.zip(expected).map { case (f1, f2) =>
        (f1.dataType, f2.dataType) match {
          case (s1: StructType, s2: StructType) =>
            f1.copy(name = f2.name, dataType = renameFieldsInStruct(s1, s2))
          case _ =>
            f1.copy(name = f2.name)
        }
      }
      StructType(newFields)
    } else {
      input
    }
  }
}
