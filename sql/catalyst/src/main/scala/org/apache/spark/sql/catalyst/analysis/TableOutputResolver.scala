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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, Attribute, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.DataType

object TableOutputResolver {
  def resolveOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean,
      conf: SQLConf): LogicalPlan = {

    if (expected.size < query.output.size) {
      throw new AnalysisException(
        s"""Cannot write to '$tableName', too many data columns:
           |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
           |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}""".stripMargin)
    }

    val errors = new mutable.ArrayBuffer[String]()
    val resolved: Seq[NamedExpression] = if (byName) {
      expected.flatMap { tableAttr =>
        query.resolve(Seq(tableAttr.name), conf.resolver) match {
          case Some(queryExpr) =>
            checkField(tableAttr, queryExpr, byName, conf, err => errors += err)
          case None =>
            errors += s"Cannot find data for output column '${tableAttr.name}'"
            None
        }
      }

    } else {
      if (expected.size > query.output.size) {
        throw new AnalysisException(
          s"""Cannot write to '$tableName', not enough data columns:
             |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
             |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}"""
            .stripMargin)
      }

      query.output.zip(expected).flatMap {
        case (queryExpr, tableAttr) =>
          checkField(tableAttr, queryExpr, byName, conf, err => errors += err)
      }
    }

    if (errors.nonEmpty) {
      throw new AnalysisException(
        s"Cannot write incompatible data to table '$tableName':\n- ${errors.mkString("\n- ")}")
    }

    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
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
