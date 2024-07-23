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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project, Transpose}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.TRANSPOSE
import org.apache.spark.sql.types.{DataType, StringType}


class ResolveTranspose(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private def leastCommonType(dataTypes: Seq[DataType]): DataType = {
    dataTypes.reduce(TypeCoercion.findTightestCommonType(_, _).getOrElse(StringType))
  }

  private def collectAndTranspose(
    child: LogicalPlan,
    nonIndexColumnNames: Seq[String]): Array[Array[Any]] = {
    // scalastyle:off println

    // Collect rows from the child plan
    val queryExecution = sparkSession.sessionState.executePlan(child)
    val collectedRows = queryExecution.executedPlan.executeCollect()

    println(s"collectedRows ${collectedRows.mkString("Array(", ", ", ")")}")

    // Determine the number of columns and rows in the original matrix
    val matrixNumCols = if (collectedRows.nonEmpty) collectedRows.head.numFields else 0
    val matrixNumRows = collectedRows.length

    println(s"matrixNumCols ${matrixNumCols}")
    println(s"matrixNumRows ${matrixNumRows}")

    // Initialize the original matrix
    val originalMatrix = Array.ofDim[Any](matrixNumRows, matrixNumCols)
    for (i <- 0 until matrixNumRows) {
      val row = collectedRows(i)
      for (j <- 0 until matrixNumCols) {
        originalMatrix(i)(j) = row.get(j, child.output(j).dataType)
      }
    }
    println(s"originalMatrix:")
    printMatrix(originalMatrix)

    // Initialize the transposed matrix
    val transposedMatrix = Array.ofDim[Any](matrixNumCols, matrixNumRows)
    for (i <- 0 until matrixNumRows) {
      for (j <- 0 until matrixNumCols) {
        println(s"Transposing element at row $i, col $j: ${originalMatrix(i)(j)}")
        transposedMatrix(j)(i) = originalMatrix(i)(j)
        println(s"transposedMatrix row $j, col $i: ${transposedMatrix(j)(i)}")
      }
    }

    println(s"transposedMatrix:")
    printMatrix(transposedMatrix)

    // Insert the first column with originalColNames
    val finalMatrix = Array.ofDim[Any](matrixNumCols, matrixNumRows + 1)
    for (i <- 0 until matrixNumCols) {
      finalMatrix(i)(0) = nonIndexColumnNames(i)
    }
    for (i <- 0 until matrixNumCols) {
      for (j <- 1 until matrixNumRows + 1) {
        finalMatrix(i)(j) = transposedMatrix(i)(j - 1)
      }
    }

    println(s"finalMatrix:")
    printMatrix(finalMatrix)

    finalMatrix
  }

  private def printMatrix(matrix: Array[Array[Any]]): Unit = {
    matrix.foreach { row =>
      println(row.mkString("[", ", ", "]"))
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(TRANSPOSE)) {
    case t @ Transpose(indexColumn, child, _, _) if !t.resolved =>
      // Cast the index column to StringType
      val indexColumnAsString = Cast(indexColumn, StringType)

      // Collect index column values (as new column names in transposed frame)
      val namedIndexColumnAsString = Alias(indexColumnAsString, "__indexColumnAsString")()
      val projectPlan = Project(
        Seq(namedIndexColumnAsString.asInstanceOf[NamedExpression]), child)
      val queryExecution = sparkSession.sessionState.executePlan(projectPlan)
      val collectedValues = queryExecution.executedPlan
        .executeCollect().map(row => row.getUTF8String(0)).toSeq

      // Determine the least common type of the non-index columns
      val nonIndexColumnsAttr = child.output.filterNot(
        _.exprId == indexColumn.asInstanceOf[Attribute].exprId)
      val nonIndexTypes = nonIndexColumnsAttr.map(_.dataType)
      val commonType = leastCommonType(nonIndexTypes)

      println(s"commonType: ${commonType}")

      // Cast non-index columns to the least common type
      val castedNonIndexColumns = nonIndexColumnsAttr.map { attr =>
        Alias(Cast(attr, commonType), attr.name)()
      }
      val castedChild = Project(castedNonIndexColumns, child)

      // Collect as matrix and flip
      val nonIndexColumnNames = nonIndexColumnsAttr.map(_.name)
      val transposedData = collectAndTranspose(castedChild, nonIndexColumnNames)
      val transposedInternalRows = transposedData.map { row =>
        InternalRow.fromSeq(row.toIndexedSeq)
      }

      // Construct output attributes
      val keyAttr = AttributeReference("key", StringType, nullable = false)()
      println(s"keyAttr: ${keyAttr}")
      val valueAttrs = collectedValues.map { value =>
        AttributeReference(
          value,
          commonType // Use the common type determined earlier
        )()
      }
      println(s"valueAttrs: ${valueAttrs}")

      LocalRelation(
        keyAttr +: valueAttrs, transposedInternalRows.toIndexedSeq)
  }
}
