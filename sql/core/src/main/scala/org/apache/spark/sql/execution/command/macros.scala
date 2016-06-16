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
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._

/**
 * This class provides arguments and body expression of the macro.
 */
case class MacroFunctionWrapper(arguments: Seq[AttributeReference], body: Expression)

/**
 * The DDL command that creates a macro.
 * To create a temporary macro, the syntax of using this command in SQL is:
 * {{{
 *    CREATE TEMPORARY MACRO macro_name([col_name col_type, ...]) expression;
 * }}}
 */
case class CreateMacroCommand(macroName: String, macroFunction: MacroFunctionWrapper)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val inputSet = AttributeSet(macroFunction.arguments)
    val colNames = macroFunction.arguments.map(_.name)
    val colToIndex: Map[String, Int] = colNames.zipWithIndex.toMap
    macroFunction.body.transformUp {
      case u @ UnresolvedAttribute(nameParts) =>
        assert(nameParts.length == 1)
        colToIndex.get(nameParts.head).getOrElse(
          throw new AnalysisException(s"Cannot create temporary macro '$macroName', " +
            s"cannot resolve: [${u}] given input columns: [${inputSet}]"))
        u
      case _: SubqueryExpression =>
        throw new AnalysisException(s"Cannot create temporary macro '$macroName', " +
          s"cannot support subquery for macro.")
    }

    val macroInfo = macroFunction.arguments.mkString(",") + "->" + macroFunction.body.toString
    val info = new ExpressionInfo(macroInfo, macroName)
    val builder = (children: Seq[Expression]) => {
      if (children.size != colNames.size) {
        throw new AnalysisException(s"actual number of arguments: ${children.size} != " +
          s"expected number of arguments: ${colNames.size} for Macro $macroName")
      }
      macroFunction.body.transformUp {
        case u @ UnresolvedAttribute(nameParts) =>
          assert(nameParts.length == 1)
          colToIndex.get(nameParts.head).map(children(_)).getOrElse(
            throw new AnalysisException(s"Macro '$macroInfo' cannot resolve '$u' " +
              s"given input expressions: [${children.mkString(",")}]"))
      }
    }
    catalog.createTempFunction(macroName, info, builder, ignoreIfExists = false)
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
    if (FunctionRegistry.builtin.functionExists(macroName)) {
      throw new AnalysisException(s"Cannot drop native function '$macroName'")
    }
    catalog.dropTempFunction(macroName, ifExists)
    Seq.empty[Row]
  }
}
