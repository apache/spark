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
import org.apache.spark.sql.catalyst.expressions._

/**
 * The DDL command that creates a macro.
 * To create a temporary macro, the syntax of using this command in SQL is:
 * {{{
 *    CREATE TEMPORARY MACRO macro_name([col_name col_type, ...]) expression;
 * }}}
 */
case class CreateMacroCommand(
    macroName: String,
    columns: Seq[AttributeReference],
    macroFunction: Expression)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val macroInfo = columns.mkString(",") + " -> " + macroFunction.toString
    val info = new ExpressionInfo(macroInfo, macroName)
    val builder = (children: Seq[Expression]) => {
      if (children.size != columns.size) {
        throw new AnalysisException(s"Actual number of columns: ${children.size} != " +
          s"expected number of columns: ${columns.size} for Macro $macroName")
      }
      macroFunction.transformUp {
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
