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

import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, Attribute, Cast, CreateStruct, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.{DataType, StructType}

object TableOutputResolver {
  def resolveOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean,
      conf: SQLConf): LogicalPlan = {

    if (expected.size < query.output.size) {
      throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(tableName, expected, query)
    }

    val errors = new mutable.ArrayBuffer[String]()
    val resolved: Seq[NamedExpression] = if (byName) {
      reorderColumnsByName(query.output, expected, conf, errors += _)
    } else {
      if (expected.size > query.output.size) {
        throw QueryCompilationErrors.cannotWriteNotEnoughColumnsToTableError(
          tableName, expected, query)
      }

      query.output.zip(expected).flatMap {
        case (queryExpr, tableAttr) =>
          checkField(tableAttr, queryExpr, byName, conf, err => errors += err)
      }
    }

    if (errors.nonEmpty) {
      throw QueryCompilationErrors.cannotWriteIncompatibleDataToTableError(tableName, errors.toSeq)
    }

    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
    }
  }

  private def reorderColumnsByName(
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String] = Nil): Seq[NamedExpression] = {
    expectedCols.flatMap { expectedCol =>
      val matched = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
      val newColPath = colPath :+ expectedCol.name
      if (matched.isEmpty) {
        addError(s"Cannot find data for output column '${newColPath.quoted}'")
        None
      } else if (matched.length > 1) {
        addError(s"Ambiguous column name in the input data: '${newColPath.quoted}'")
        None
      } else {
        val matchedCol = matched.head match {
          case a: Attribute => a.withName(expectedCol.name)
          case a: Alias => a.withName(expectedCol.name)
          case other => other
        }
        (matchedCol.dataType, expectedCol.dataType) match {
          case (input: StructType, expected: StructType) =>
            val fields = input.zipWithIndex.map { case (f, i) =>
              Alias(GetStructField(matchedCol, i, Some(f.name)), f.name)()
            }
            val reordered = reorderColumnsByName(
              fields, expected.toAttributes, conf, addError, newColPath)
            Some(Alias(CreateStruct(reordered), expectedCol.name)())
          case _ =>
            checkField(expectedCol, matchedCol, byName = true, conf, addError)
        }
      }
    }
  }

  private def checkField(
      tableAttr: Attribute,
      queryExpr: NamedExpression,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit): Option[NamedExpression] = {

    val storeAssignmentPolicy = conf.storeAssignmentPolicy
    lazy val outputField = if (tableAttr.dataType.sameType(queryExpr.dataType) &&
      tableAttr.name == queryExpr.name &&
      tableAttr.metadata == queryExpr.metadata) {
      Some(queryExpr)
    } else {
      val casted = storeAssignmentPolicy match {
        case StoreAssignmentPolicy.ANSI =>
          AnsiCast(queryExpr, tableAttr.dataType, Option(conf.sessionLocalTimeZone))
        case _ =>
          Cast(queryExpr, tableAttr.dataType, Option(conf.sessionLocalTimeZone))
      }
      val exprWithStrLenCheck = if (conf.charVarcharAsString) {
        casted
      } else {
        CharVarcharUtils.stringLengthCheck(casted, tableAttr)
      }
      // Renaming is needed for handling the following cases like
      // 1) Column names/types do not match, e.g., INSERT INTO TABLE tab1 SELECT 1, 2
      // 2) Target tables have column metadata
      Some(Alias(exprWithStrLenCheck, tableAttr.name)(explicitMetadata = Some(tableAttr.metadata)))
    }

    storeAssignmentPolicy match {
      case StoreAssignmentPolicy.LEGACY =>
        outputField

      case StoreAssignmentPolicy.STRICT | StoreAssignmentPolicy.ANSI =>
        // run the type check first to ensure type errors are present
        val canWrite = DataType.canWrite(
          queryExpr.dataType, tableAttr.dataType, byName, conf.resolver, tableAttr.name,
          storeAssignmentPolicy, addError)
        if (queryExpr.nullable && !tableAttr.nullable) {
          addError(s"Cannot write nullable values to non-null column '${tableAttr.name}'")
          None

        } else if (!canWrite) {
          None

        } else {
          outputField
        }
    }
  }
}
