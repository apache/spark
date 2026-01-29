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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, VariableReference}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

/**
 * Physical plan node for SELECT INTO.
 * Behaves identically to DECLARE + OPEN + FETCH + CLOSE cursor sequence:
 * - When query returns zero rows, raises NO DATA condition (SQLSTATE 02000)
 * - When query returns more than one row, an error is thrown
 * - When query returns exactly one row, values are assigned to variables
 * @param variables The variables to set
 * @param query The query that produces the values
 */
case class SelectIntoExec(
    variables: Seq[VariableReference],
    query: SparkPlan)
  extends V2CommandExec with UnaryLike[SparkPlan] {

  override protected def run(): Seq[InternalRow] = {
    val values = query.executeCollect()

    if (values.length == 0) {
      // SELECT INTO: raise NO DATA condition (SQLSTATE 02000)
      // This allows handlers to catch the condition, matching FETCH cursor INTO behavior
      throw new AnalysisException(
        errorClass = "SELECT_INTO_NO_DATA",
        messageParameters = Map.empty)
    } else if (values.length > 1) {
      throw new SparkException(
        errorClass = "ROW_SUBQUERY_TOO_MANY_ROWS",
        messageParameters = Map.empty,
        cause = null)
    } else {
      // Exactly one row: assign values to variables
      val row = values(0)

      // Validate arity
      if (variables.length != row.numFields) {
        throw new AnalysisException(
          errorClass = "ASSIGNMENT_ARITY_MISMATCH",
          messageParameters = Map(
            "numTarget" -> variables.length.toString,
            "numExpr" -> row.numFields.toString))
      }

      variables.zipWithIndex.foreach { case (v, index) =>
        val sourceValue = row.get(index, query.output(index).dataType)
        val sourceType = query.output(index).dataType
        val targetType = v.dataType

        // Apply ANSI cast if source and target types differ
        val castedValue = if (sourceType == targetType) {
          sourceValue
        } else {
          val cast = Cast(
            Literal(sourceValue, sourceType),
            targetType,
            Option(session.sessionState.conf.sessionLocalTimeZone),
            ansiEnabled = true)
          cast.eval(InternalRow.empty)
        }

        VariableAssignmentUtils.assignVariable(
          v,
          castedValue,
          session.sessionState.catalogManager.tempVariableManager,
          session.sessionState.conf)
      }
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
  override def child: SparkPlan = query
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(query = newChild)
  }
}
