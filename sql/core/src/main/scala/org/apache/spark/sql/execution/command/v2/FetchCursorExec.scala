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

package org.apache.spark.sql.execution.command.v2

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, VariableReference}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for fetching from cursors.
 */
case class FetchCursorExec(
    cursorName: String,
    targetVariables: Seq[VariableReference]) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val scriptingContextManager = SqlScriptingContextManager.get()
      .getOrElse(throw new AnalysisException(
        errorClass = "CURSOR_OUTSIDE_SCRIPT",
        messageParameters = Map("cursorName" -> cursorName)))

    val scriptingContext = scriptingContextManager.getContext
      .asInstanceOf[org.apache.spark.sql.scripting.SqlScriptingExecutionContext]

    // Parse and normalize cursor name for case sensitivity
    val cursorNameParts = parseCursorName(cursorName)

    // Find cursor in scope hierarchy
    val cursorDef = scriptingContext.currentFrame.findCursorByNameParts(cursorNameParts)
      .getOrElse(
        throw new AnalysisException(
          errorClass = "CURSOR_NOT_FOUND",
          messageParameters = Map("cursorName" -> cursorName)))

    // Validate cursor is open
    if (!cursorDef.isOpen) {
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_OPEN",
        messageParameters = Map("cursorName" -> cursorName))
    }

    // Get next row from iterator
    val iterator = cursorDef.resultIterator.get
    if (!iterator.hasNext) {
      throw new AnalysisException(
        errorClass = "CURSOR_NO_MORE_ROWS",
        messageParameters = Map("cursorName" -> cursorName))
    }

    val externalRow = iterator.next()
    val variableManager = scriptingContextManager.getVariableManager

    // Convert Row to InternalRow for processing
    val schema = org.apache.spark.sql.catalyst.types.DataTypeUtils
      .fromAttributes(cursorDef.query.output)
    val converter = org.apache.spark.sql.catalyst.CatalystTypeConverters
      .createToCatalystConverter(schema)
    val currentRow = converter(externalRow).asInstanceOf[InternalRow]

    // SQL Standard special case: FETCH multiple columns INTO single STRUCT variable
    if (shouldFetchIntoStruct(targetVariables, currentRow)) {
      fetchIntoStruct(
        targetVariables.head,
        targetVariables.head.dataType.asInstanceOf[org.apache.spark.sql.types.StructType],
        currentRow,
        cursorDef,
        variableManager)
    } else {
      // Regular case: one-to-one column-to-variable assignment
      fetchIntoVariables(targetVariables, currentRow, cursorDef, variableManager)
    }

    Nil
  }

  /**
   * Parses and normalizes cursor name parts based on case sensitivity configuration.
   */
  private def parseCursorName(cursorName: String): Seq[String] = {
    cursorName.split("\\.").toSeq.map { part =>
      if (session.sessionState.conf.caseSensitiveAnalysis) {
        part
      } else {
        part.toLowerCase(Locale.ROOT)
      }
    }
  }

  /**
   * Determines if the fetch should use the struct special case.
   * Returns true if there's exactly one target variable, it's a struct type,
   * and there are multiple cursor columns.
   */
  private def shouldFetchIntoStruct(
      targetVariables: Seq[VariableReference],
      currentRow: InternalRow): Boolean = {
    targetVariables.length == 1 &&
      currentRow.numFields > 1 &&
      targetVariables.head.dataType.isInstanceOf[org.apache.spark.sql.types.StructType]
  }

  /**
   * Performs regular one-to-one assignment of cursor columns to variables.
   * Applies ANSI store assignment rules with type casting.
   */
  private def fetchIntoVariables(
      targetVariables: Seq[VariableReference],
      currentRow: InternalRow,
      cursorDef: org.apache.spark.sql.scripting.CursorDefinition,
      variableManager: org.apache.spark.sql.catalyst.catalog.VariableManager): Unit = {
    // Validate arity
    if (targetVariables.length != currentRow.numFields) {
      throw new AnalysisException(
        errorClass = "ASSIGNMENT_ARITY_MISMATCH",
        messageParameters = Map(
          "numTarget" -> targetVariables.length.toString,
          "numExpr" -> currentRow.numFields.toString))
    }

    // Assign each column to its corresponding variable
    targetVariables.zipWithIndex.foreach { case (varRef, idx) =>
      val sourceValue = currentRow.get(idx, cursorDef.query.output(idx).dataType)
      val castedValue = applyCastIfNeeded(
        sourceValue,
        cursorDef.query.output(idx).dataType,
        varRef.dataType)

      assignToVariable(varRef, castedValue, variableManager)
    }
  }

  /**
   * Applies ANSI store assignment cast if source and target types differ.
   */
  private def applyCastIfNeeded(
      sourceValue: Any,
      sourceType: org.apache.spark.sql.types.DataType,
      targetType: org.apache.spark.sql.types.DataType): Any = {
    if (sourceType == targetType) {
      sourceValue
    } else {
      val cast = org.apache.spark.sql.catalyst.expressions.Cast(
        org.apache.spark.sql.catalyst.expressions.Literal(sourceValue, sourceType),
        targetType,
        Option(session.sessionState.conf.sessionLocalTimeZone),
        ansiEnabled = true)
      cast.eval(org.apache.spark.sql.catalyst.InternalRow.empty)
    }
  }

  /**
   * Assigns a value to a variable, handling case sensitivity properly.
   */
  private def assignToVariable(
      varRef: VariableReference,
      value: Any,
      variableManager: org.apache.spark.sql.catalyst.catalog.VariableManager): Unit = {
    val namePartsCaseAdjusted = if (session.sessionState.conf.caseSensitiveAnalysis) {
      varRef.originalNameParts
    } else {
      varRef.originalNameParts.map(_.toLowerCase(Locale.ROOT))
    }

    val varDef = VariableDefinition(
      varRef.identifier,
      varRef.varDef.defaultValueSQL,
      Literal(value, varRef.dataType)
    )

    variableManager.set(namePartsCaseAdjusted, varDef)
  }

  /**
   * Fetches multiple cursor columns into a single STRUCT variable.
   * This is a SQL Standard special case that allows:
   *   FETCH cursor_with_multiple_columns INTO single_struct_variable
   *
   * Each cursor column is cast to the corresponding struct field type using
   * ANSI store assignment rules.
   */
  private def fetchIntoStruct(
      targetVar: VariableReference,
      structType: org.apache.spark.sql.types.StructType,
      currentRow: InternalRow,
      cursorDef: org.apache.spark.sql.scripting.CursorDefinition,
      variableManager: org.apache.spark.sql.catalyst.catalog.VariableManager): Unit = {
    import org.apache.spark.sql.catalyst.expressions.{Cast, CreateStruct, Literal}
    import org.apache.spark.sql.catalyst.InternalRow

    // Validate struct field count matches cursor column count
    if (structType.length != currentRow.numFields) {
      throw new AnalysisException(
        errorClass = "ASSIGNMENT_ARITY_MISMATCH",
        messageParameters = Map(
          "numTarget" -> structType.length.toString,
          "numExpr" -> currentRow.numFields.toString))
    }

    // Build struct fields by extracting and casting cursor columns
    val fieldExpressions = structType.fields.zipWithIndex.map { case (field, idx) =>
      val sourceValue = currentRow.get(idx, cursorDef.query.output(idx).dataType)
      val sourceLiteral = Literal(sourceValue, cursorDef.query.output(idx).dataType)

      // Apply ANSI cast if types differ
      if (cursorDef.query.output(idx).dataType == field.dataType) {
        sourceLiteral
      } else {
        Cast(
          sourceLiteral,
          field.dataType,
          Option(session.sessionState.conf.sessionLocalTimeZone),
          ansiEnabled = true)
      }
    }

    // Create and evaluate struct
    val structExpr = CreateStruct(fieldExpressions.toSeq)
    val structValue = structExpr.eval(InternalRow.empty)

    // Assign struct to variable
    assignToVariable(targetVar, structValue, variableManager)
  }

  override def output: Seq[Attribute] = Nil
}
