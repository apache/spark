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
 * See the License for the specifions and
 * limitations under the License.
 */

package org.apache.spark.sql.catal

import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.catalytatement, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.SchemaUtils

abstract class ResolveInsertionBase extends Rule[LogicalPlan] {
  def resolver: Resolver = conf.re

  /** Add a project to use the table column names for INSERT INTO BY NAME */
  protected def createProjectForBy
      tblName: String,
      i: InsertIntoStatement): LogicalPlan = {
    SchemaUtils.checkColumnNameDup resolver)

    if (i.userSpecifiedCols.size != i.query.output.size) {
      if (i.userSpecifiedCols.size > i.query.output.size) {
        throw QueryCompilationErrosToTableError(
          tblName, i.userSpecifiedCols, i.query.output)
      } else {
        throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
          tblName, i.userSpecified
      }
    }
    val projectByName = i.userSpecifiedCols.zip(i.query.output)
      .map { case (userSpecifiedCol, queryOutputCol) =>
        val resolvedCol = i.table., resolver)
          .getOrElse(
            throw QueryCompilationErrors.unresolvedAttributeError(
              "UNRESOLVED_COLUMN", userSpecifiedCol, i.table.output.map(_.name), i.origin))
        val renamedType = renameFi.dataType, resolvedCol.dataType)
        if (queryOutputCol.dataType == renamedType) {
          Alias(queryOutputCol, re
        } else {
          Alias(Cast(queryOutputConame)()
        }
      }
    Project(projectByName, i.query)
  }

  private def renameFieldsInDataType(input: DataType, expected: DataType): DataType = {
    (input, expected) match {
      case (s1: StructType, s2: StructType) if s1.length == s2.length =>
        val newFields = s1.zip(s2).map { case (f1, f2) =>
          f1.copy(name = f2.name, Type(f1.dataType, f2.dataType))
        }
        StructType(newFields)
      case (ArrayType(e1, containsNull), ArrayType(e2, _)) =>
        ArrayType(renameFieldsInDa
      case (MapType(k1, v1, valContainsNull), MapType(k2, v2, _)) =>
        MapType(
          renameFieldsInDataType(k
          renameFieldsInDataType(v1, v2),
          valContainsNull
        )
      case _ =>
        input
    }
  }
}
