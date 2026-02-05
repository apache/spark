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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.analysis.{FakeLocalCatalog, FakeSystemCatalog}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{Attribute, CursorReference, Expression, Literal, VariableReference}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.scripting.{CursorFetching, CursorOpened}

/**
 * Physical plan node for fetching from cursors.
 *
 * Transitions cursor from Opened to Fetching state on first fetch (creating result iterator),
 * then fetches rows from the iterator on subsequent calls. Assigns fetched values to target
 * variables with ANSI store assignment rules.
 *
 * @param cursor CursorReference resolved during analysis phase
 * @param targetVariables Variables to fetch into
 */
case class FetchCursorExec(
    cursor: Expression,
    targetVariables: Seq[VariableReference]) extends LeafV2CommandExec with DataTypeErrorsBase {

  override protected def run(): Seq[InternalRow] = {
    // Extract CursorReference from the resolved cursor expression
    val cursorRef = cursor.asInstanceOf[CursorReference]

    val scriptingContext = CursorCommandUtils.getScriptingContext(cursorRef.definition.name)

    // Get current cursor state
    val currentState = scriptingContext.getCursorState(cursorRef).getOrElse(
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_FOUND",
        messageParameters = Map("cursorName" -> toSQLId(cursorRef.definition.name))))

    // Get or create iterator based on current state
    val (iterator, analyzedQuery) = currentState match {
      case CursorOpened(query) =>
        // First fetch - create iterator and transition to Fetching state
        // Use executeToIterator() to get InternalRow directly, avoiding conversion overhead
        val df = Dataset.ofRows(
          session.asInstanceOf[org.apache.spark.sql.classic.SparkSession],
          query)
        val iter = df.queryExecution.executedPlan.executeToIterator()
        scriptingContext.updateCursorState(
          cursorRef.normalizedName,
          cursorRef.scopeLabel,
          CursorFetching(query, iter))
        (iter, query)

      case CursorFetching(query, iter) =>
        // Subsequent fetch - use existing iterator
        (iter, query)

      case _ =>
        throw new AnalysisException(
          errorClass = "CURSOR_NOT_OPEN",
          messageParameters = Map("cursorName" -> toSQLId(cursorRef.definition.name)))
    }

    // Get next row from iterator
    if (!iterator.hasNext) {
      throw new AnalysisException(
        errorClass = "CURSOR_NO_MORE_ROWS",
        messageParameters = Map("cursorName" -> toSQLId(cursorRef.definition.name)))
    }

    // Get InternalRow directly - no conversion needed
    val currentRow = iterator.next()

    // SQL Standard special case: FETCH multiple columns INTO single STRUCT variable
    if (shouldFetchIntoStruct(targetVariables, currentRow)) {
      fetchIntoStruct(
        targetVariables.head,
        targetVariables.head.dataType.asInstanceOf[org.apache.spark.sql.types.StructType],
        currentRow,
        analyzedQuery)
    } else {
      // Regular case: one-to-one column-to-variable assignment
      fetchIntoVariables(targetVariables, currentRow, analyzedQuery)
    }

    Nil
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
      analyzedQuery: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Unit = {
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
      val sourceValue = currentRow.get(idx, analyzedQuery.output(idx).dataType)
      val castedValue = applyCastIfNeeded(
        sourceValue,
        analyzedQuery.output(idx).dataType,
        varRef.dataType)

      assignToVariable(varRef, castedValue)
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
      value: Any): Unit = {
    val namePartsCaseAdjusted = if (session.sessionState.conf.caseSensitiveAnalysis) {
      varRef.originalNameParts
    } else {
      varRef.originalNameParts.map(_.toLowerCase(Locale.ROOT))
    }

    // Select the appropriate variable manager based on the catalog
    // This logic matches SetVariableExec.setVariable()
    val tempVariableManager = session.sessionState.catalogManager.tempVariableManager
    val scriptingVariableManager = SqlScriptingContextManager.get().map(_.getVariableManager)

    val variableManager = varRef.catalog match {
      case FakeLocalCatalog if scriptingVariableManager.isEmpty =>
        throw SparkException.internalError("FetchCursorExec: Variable has FakeLocalCatalog, " +
          "but ScriptingVariableManager is None.")

      case FakeLocalCatalog if scriptingVariableManager.get.get(namePartsCaseAdjusted).isEmpty =>
        throw SparkException.internalError("Local variable should be present in FetchCursorExec " +
          "because ResolveFetchCursor has already determined it exists.")

      case FakeLocalCatalog => scriptingVariableManager.get

      case FakeSystemCatalog if tempVariableManager.get(namePartsCaseAdjusted).isEmpty =>
        throw unresolvedVariableError(namePartsCaseAdjusted, Seq("SYSTEM", "SESSION"))

      case FakeSystemCatalog => tempVariableManager

      case c => throw SparkException.internalError("Unexpected catalog in FetchCursorExec: " + c)
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
      analyzedQuery: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Unit = {
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
      val sourceValue = currentRow.get(idx, analyzedQuery.output(idx).dataType)
      val sourceLiteral = Literal(sourceValue, analyzedQuery.output(idx).dataType)

      // Apply ANSI cast if types differ
      if (analyzedQuery.output(idx).dataType == field.dataType) {
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
    assignToVariable(targetVar, structValue)
  }

  override def output: Seq[Attribute] = Nil
}
