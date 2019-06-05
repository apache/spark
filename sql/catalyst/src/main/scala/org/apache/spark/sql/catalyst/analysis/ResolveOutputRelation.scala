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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression, UpCast}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Resolves columns of an output table from the data in a logical plan. This rule will:
 *
 * - Reorder columns when the write is by name
 * - Insert safe casts when data types do not match
 * - Insert aliases when column names do not match
 * - Detect plans that are not compatible with the output table and throw AnalysisException
 */
object ResolveOutputRelation extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case append @ AppendData(table, query, isByName)
        if table.resolved && query.resolved && !append.outputResolved =>
      val projection = resolveOutputColumns(table.name, table.output, query, isByName)

      if (projection != query) {
        append.copy(query = projection)
      } else {
        append
      }

    case overwrite @ OverwriteByExpression(table, _, query, isByName)
        if table.resolved && query.resolved && !overwrite.outputResolved =>
      val projection = resolveOutputColumns(table.name, table.output, query, isByName)

      if (projection != query) {
        overwrite.copy(query = projection)
      } else {
        overwrite
      }

    case overwrite @ OverwritePartitionsDynamic(table, query, isByName)
        if table.resolved && query.resolved && !overwrite.outputResolved =>
      val projection = resolveOutputColumns(table.name, table.output, query, isByName)

      if (projection != query) {
        overwrite.copy(query = projection)
      } else {
        overwrite
      }
  }

  def resolveOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean): LogicalPlan = {

    if (expected.size < query.output.size) {
      throw new AnalysisException(
        s"""Cannot write to '$tableName', too many data columns:
           |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
           |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}""".stripMargin)
    }

    val resolver = SQLConf.get.resolver
    val errors = new mutable.ArrayBuffer[String]()
    val resolved: Seq[NamedExpression] = if (byName) {
      expected.flatMap { tableAttr =>
        query.resolveQuoted(tableAttr.name, resolver) match {
          case Some(queryExpr) =>
            checkField(tableAttr, queryExpr, byName, resolver, err => errors += err)
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
          checkField(tableAttr, queryExpr, byName, resolver, err => errors += err)
      }
    }

    if (errors.nonEmpty) {
      throw new AnalysisException(
        s"Cannot write incompatible data to table '$tableName':\n- ${errors.mkString("\n- ")}")
    }

    Project(resolved, query)
  }

  def checkField(
      tableAttr: Attribute,
      queryExpr: NamedExpression,
      byName: Boolean,
      resolver: Resolver,
      addError: String => Unit): Option[NamedExpression] = {

    // run the type check first to ensure type errors are present
    val canWrite = DataType.canWrite(
      queryExpr.dataType, tableAttr.dataType, byName, resolver, tableAttr.name, addError)

    if (queryExpr.nullable && !tableAttr.nullable) {
      addError(s"Cannot write nullable values to non-null column '${tableAttr.name}'")
      None

    } else if (!canWrite) {
      None

    } else {
      // always add an UpCast. it will be removed in the optimizer if it is unnecessary.
      Some(Alias(
        UpCast(queryExpr, tableAttr.dataType), tableAttr.name
      )(
        explicitMetadata = Option(tableAttr.metadata)
      ))
    }
  }
}
