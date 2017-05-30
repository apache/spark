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

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.StructType

/**
 * This class provides arguments and body expression of the macro function.
 */
case class MacroFunctionWrapper(columns: StructType, macroFunction: Expression)


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
    val columns = funcWrapper.columns.map(_.name)
    val columnAttrs = funcWrapper.columns.toAttributes
    def formatName = if (sparkSession.sessionState.conf.caseSensitiveAnalysis) {
        (name: String) => name
      } else {
        (name: String) => name.toLowerCase
      }
    val colToIndex: Map[String, Int] = columns.map(formatName).zipWithIndex.toMap
    if (colToIndex.size != columns.size) {
      throw new AnalysisException(s"Failed to CREATE TEMPORARY MACRO $macroName, because " +
        s"at least one parameter name was used more than once : ${columns.mkString(",")}")
    }

    val resolvedMacroFunction = try {
      val plan = Project(Seq(Alias(funcWrapper.macroFunction, "m")()), LocalRelation(columnAttrs))
      val analyzed @ Project(Seq(named), _) = sparkSession.sessionState.analyzer.execute(plan)
      sparkSession.sessionState.analyzer.checkAnalysis(analyzed)
      named.children.head
    } catch {
      case a: AnalysisException =>
        throw new AnalysisException(s"Failed to CREATE TEMPORARY MACRO $macroName, because of " +
          s"exception: ${a.getMessage}")
    }

    val foundColumns: mutable.Set[String] = new mutable.HashSet()
    val macroFunction = resolvedMacroFunction.transform {
      case u: AttributeReference =>
        val index = colToIndex.get(formatName(u.name)).getOrElse(
          throw new AnalysisException(s"Failed to CREATE TEMPORARY MACRO $macroName, because " +
            s"it cannot find colName: ${u.name}, actual columns: ${columns.mkString(",")}"))
        foundColumns.add(formatName(u.name))
        BoundReference(index, u.dataType, u.nullable)
    }
    if (foundColumns.size != columns.size) {
      throw new AnalysisException(s"Failed to CREATE TEMPORARY MACRO $macroName, because " +
        s"expected columns ${foundColumns.mkString(",")} but found ${columns.mkString(",")}")
    }

    val columnLength: Int = columns.length
    val info = new ExpressionInfo(macroName, macroName)
    val builder = (children: Seq[Expression]) => {
      if (children.size != columnLength) {
        throw new AnalysisException(s"Arguments length: ${children.size} != " +
          s"expected number: ${columnLength} of arguments for Macro $macroName")
      }
      macroFunction.transform {
        // Skip to validate the input type because check it before.
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
