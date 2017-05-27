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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructField

/**
 * This class provides arguments and body expression of the macro function.
 */
case class MacroFunctionWrapper(columns: Seq[StructField], macroFunction: Expression)

/**
 * The DDL command that creates a macro.
 * To create a temporary macro, the syntax of using this command in SQL is:
 * {{{
 *    CREATE TEMPORARY MACRO macro_name([col_name col_type, ...]) expression;
 * }}}
 */
case class CreateMacroCommand(
    macroName: String,
    funcWrapper: MacroFunctionWrapper)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val columns = funcWrapper.columns.map { col =>
      AttributeReference(col.name, col.dataType, col.nullable, col.metadata)() }
    val colToIndex: Map[String, Int] = columns.map(_.name).zipWithIndex.toMap
    if (colToIndex.size != columns.size) {
      throw new AnalysisException(s"Cannot support duplicate colNames " +
        s"for CREATE TEMPORARY MACRO $macroName, actual columns: ${columns.mkString(",")}")
    }
    val macroFunction = funcWrapper.macroFunction.transform {
      case u: UnresolvedAttribute =>
        val index = colToIndex.get(u.name).getOrElse(
          throw new AnalysisException(s"Cannot find colName: ${u} " +
            s"for CREATE TEMPORARY MACRO $macroName, actual columns: ${columns.mkString(",")}"))
        BoundReference(index, columns(index).dataType, columns(index).nullable)
      case u: UnresolvedFunction =>
        sparkSession.sessionState.catalog.lookupFunction(u.name, u.children)
      case s: SubqueryExpression =>
        throw new AnalysisException(s"Cannot support Subquery: ${s} " +
          s"for CREATE TEMPORARY MACRO $macroName")
      case u: UnresolvedGenerator =>
        throw new AnalysisException(s"Cannot support Generator: ${u} " +
          s"for CREATE TEMPORARY MACRO $macroName")
    }

    val macroInfo = columns.mkString(",") + " -> " + funcWrapper.macroFunction.toString
    val info = new ExpressionInfo(macroInfo, macroName, true)
    val builder = (children: Seq[Expression]) => {
      if (children.size != columns.size) {
        throw new AnalysisException(s"Actual number of columns: ${children.size} != " +
          s"expected number of columns: ${columns.size} for Macro $macroName")
      }
      macroFunction.transform {
        // Skip to validate the input type because check it at runtime.
        case b: BoundReference => children(b.ordinal)
      }
    }
    catalog.createTempMacro(macroName, info, builder)
    Seq.empty[Row]
  }
}

/**
 * The DDL command that drops a macro.
 * ifExists: returns an error if the macro doesn't exist, unless this is true.
 * {{{
 *    DROP TEMPORARY MACRO [IF EXISTS] macro_name;
 * }}}
 */
case class DropMacroCommand(macroName: String, ifExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    catalog.dropTempMacro(macroName, ifExists)
    Seq.empty[Row]
  }
}
