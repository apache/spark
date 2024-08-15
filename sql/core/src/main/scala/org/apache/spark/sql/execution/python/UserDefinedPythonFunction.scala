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

package org.apache.spark.sql.execution.python

import java.io.{DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import net.razorvine.pickle.Pickler

import org.apache.spark.api.python.{PythonEvalType, PythonFunction, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, Expression, FunctionTableSubqueryArgumentExpression, NamedArgumentExpression, NullsFirst, NullsLast, PythonUDAF, PythonUDF, PythonUDTF, PythonUDTFAnalyzeResult, PythonUDTFSelectedExpression, SortOrder, UnresolvedPolymorphicPythonUDTF}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, NamedParametersSupport, OneRowRelation}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.ExpressionUtils.{column, expression}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A user-defined Python function. This is used by the Python API.
 */
case class UserDefinedPythonFunction(
    name: String,
    func: PythonFunction,
    dataType: DataType,
    pythonEvalType: Int,
    udfDeterministic: Boolean) {

  def builder(e: Seq[Expression]): Expression = {
    if (pythonEvalType == PythonEvalType.SQL_BATCHED_UDF
        || pythonEvalType ==PythonEvalType.SQL_ARROW_BATCHED_UDF
        || pythonEvalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF
        || pythonEvalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF) {
      /*
       * Check if the named arguments:
       * - don't have duplicated names
       * - don't contain positional arguments after named arguments
       */
      NamedParametersSupport.splitAndCheckNamedArguments(e, name)
    } else if (e.exists(_.isInstanceOf[NamedArgumentExpression])) {
      throw QueryCompilationErrors.namedArgumentsNotSupported(name)
    }

    if (pythonEvalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF) {
      PythonUDAF(name, func, dataType, e, udfDeterministic)
    } else {
      PythonUDF(name, func, dataType, e, pythonEvalType, udfDeterministic)
    }
  }

  /** Returns a [[Column]] that will evaluate to calling this UDF with the given input. */
  def apply(exprs: Column*): Column = builder(exprs.map(expression))

  /**
   * Returns a [[Column]] that will evaluate the UDF expression with the given input.
   */
  // TODO this is used in PySpark!
  def fromUDFExpr(expr: Expression): Column = {
    expr match {
      case udaf: PythonUDAF => Column(udaf.toAggregateExpression())
      case _ => Column(expr)
    }
  }
}

/**
 * A user-defined Python table function. This is used by the Python API.
 */
case class UserDefinedPythonTableFunction(
    name: String,
    func: PythonFunction,
    returnType: Option[StructType],
    pythonEvalType: Int,
    udfDeterministic: Boolean) {

  def this(
      name: String,
      func: PythonFunction,
      returnType: StructType,
      pythonEvalType: Int,
      udfDeterministic: Boolean) = {
    this(name, func, Some(returnType), pythonEvalType, udfDeterministic)
  }

  def this(
      name: String,
      func: PythonFunction,
      pythonEvalType: Int,
      udfDeterministic: Boolean) = {
    this(name, func, None, pythonEvalType, udfDeterministic)
  }

  def builder(exprs: Seq[Expression], parser: => ParserInterface): LogicalPlan = {
    /*
     * Check if the named arguments:
     * - don't have duplicated names
     * - don't contain positional arguments after named arguments
     */
    NamedParametersSupport.splitAndCheckNamedArguments(exprs, name)

    val udtf = returnType match {
      case Some(rt) =>
        PythonUDTF(
          name = name,
          func = func,
          elementSchema = rt,
          pickledAnalyzeResult = None,
          children = exprs,
          evalType = pythonEvalType,
          udfDeterministic = udfDeterministic)
      case _ =>
        // Check which argument is a table argument here since it will be replaced with
        // `UnresolvedAttribute` to construct lateral join.
        val tableArgs = exprs.map {
          case _: FunctionTableSubqueryArgumentExpression => true
          case NamedArgumentExpression(_, _: FunctionTableSubqueryArgumentExpression) => true
          case _ => false
        }
        val runAnalyzeInPython = (func: PythonFunction, exprs: Seq[Expression]) => {
          val runner =
            new UserDefinedPythonTableFunctionAnalyzeRunner(name, func, exprs, tableArgs, parser)
          runner.runInPython()
        }
        UnresolvedPolymorphicPythonUDTF(
          name = name,
          func = func,
          children = exprs,
          evalType = pythonEvalType,
          udfDeterministic = udfDeterministic,
          resolveElementMetadata = runAnalyzeInPython)
    }
    Generate(
      udtf,
      unrequiredChildIndex = Nil,
      outer = false,
      qualifier = None,
      generatorOutput = Nil,
      child = OneRowRelation()
    )
  }

  /** Returns a [[DataFrame]] that will evaluate to calling this UDTF with the given input. */
  def apply(session: SparkSession, exprs: Column*): DataFrame = {
    val udtf = builder(exprs.map(session.expression), session.sessionState.sqlParser)
    Dataset.ofRows(session, udtf)
  }
}

/**
 * Runs the Python UDTF's `analyze` static method.
 *
 * When the Python UDTF is defined without a static return type,
 * the analyzer will call this while resolving table-valued functions.
 *
 * This expects the Python UDTF to have `analyze` static method that take arguments:
 *
 * - The number and order of arguments are the same as the UDTF inputs
 * - Each argument is an `AnalyzeArgument`, containing:
 *   - dataType: DataType
 *   - value: Any: if the argument is foldable; otherwise None
 *   - isTable: bool: True if the argument is TABLE
 *
 * and that return an `AnalyzeResult`.
 *
 * It serializes/deserializes the data types via JSON,
 * and the values for the case the argument is foldable are pickled.
 *
 * `AnalysisException` with the error class "TABLE_VALUED_FUNCTION_FAILED_TO_ANALYZE_IN_PYTHON"
 * will be thrown when an exception is raised in Python.
 */
class UserDefinedPythonTableFunctionAnalyzeRunner(
    name: String,
    func: PythonFunction,
    exprs: Seq[Expression],
    tableArgs: Seq[Boolean],
    parser: ParserInterface)
  extends PythonPlannerRunner[PythonUDTFAnalyzeResult](func) {

  override val workerModule = "pyspark.sql.worker.analyze_udtf"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send Python UDTF
    PythonWorkerUtils.writeUTF(name, dataOut)
    PythonWorkerUtils.writePythonFunction(func, dataOut)

    // Send arguments
    dataOut.writeInt(exprs.length)
    exprs.zip(tableArgs).foreach { case (expr, isTable) =>
      PythonWorkerUtils.writeUTF(expr.dataType.json, dataOut)
      val (key, value) = expr match {
        case NamedArgumentExpression(k, v) => (Some(k), v)
        case _ => (None, expr)
      }
      if (value.foldable) {
        dataOut.writeBoolean(true)
        val obj = pickler.dumps(EvaluatePython.toJava(value.eval(), value.dataType))
        PythonWorkerUtils.writeBytes(obj, dataOut)
      } else {
        dataOut.writeBoolean(false)
      }
      dataOut.writeBoolean(isTable)
      // If the expr is NamedArgumentExpression, send its name.
      key match {
        case Some(key) =>
          dataOut.writeBoolean(true)
          PythonWorkerUtils.writeUTF(key, dataOut)
        case _ =>
          dataOut.writeBoolean(false)
      }
    }
  }

  override protected def receiveFromPython(dataIn: DataInputStream): PythonUDTFAnalyzeResult = {
    // Receive the schema or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
        // Remove the leading traceback stack trace from the error message string, if any, since it
        // usually only includes the "analyze_udtf.py" filename and a line number.
        .split("PySparkValueError:").last.strip()
      throw QueryCompilationErrors.tableValuedFunctionFailedToAnalyseInPythonError(msg)
    }

    val schema = DataType.fromJson(
      PythonWorkerUtils.readUTF(length, dataIn)).asInstanceOf[StructType]

    // Receive the pickled AnalyzeResult buffer, if any.
    val pickledAnalyzeResult: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)

    // Receive whether the "with single partition" property is requested.
    val withSinglePartition = dataIn.readInt() == 1
    // Receive the list of requested partitioning expressions, if any.
    val partitionByExpressions = ArrayBuffer.empty[Expression]
    val numPartitionByExpressions = dataIn.readInt()
    for (_ <- 0 until numPartitionByExpressions) {
      val expressionSql: String = PythonWorkerUtils.readUTF(dataIn)
      val parsed: Expression = parser.parseExpression(expressionSql)
      partitionByExpressions.append(parsed)
    }
    // Receive the list of requested ordering expressions, if any.
    val orderBy = ArrayBuffer.empty[SortOrder]
    val numOrderByItems = dataIn.readInt()
    for (_ <- 0 until numOrderByItems) {
      val expressionSql: String = PythonWorkerUtils.readUTF(dataIn)
      val parsed: Expression = parser.parseExpression(expressionSql)
      // Perform a basic check that the requested ordering column string does not include an alias,
      // since it is possible to accidentally try to specify a sort order like ASC or DESC or NULLS
      // FIRST/LAST in this manner leading to confusing results.
      parsed match {
        case a: Alias =>
          throw QueryCompilationErrors
            .invalidSortOrderInUDTFOrderingColumnFromAnalyzeMethodHasAlias(aliasName = a.name)
        case _ =>
      }
      val direction = if (dataIn.readInt() == 1) Ascending else Descending
      val overrideNullsFirst = dataIn.readInt()
      overrideNullsFirst match {
        case 0 => orderBy.append(SortOrder(parsed, direction))
        case 1 => orderBy.append(SortOrder(parsed, direction, NullsFirst, Seq.empty))
        case 2 => orderBy.append(SortOrder(parsed, direction, NullsLast, Seq.empty))
      }
    }
    // Receive the list of requested input columns to select, if specified.
    val numSelectedInputExpressions = dataIn.readInt()
    val selectedInputExpressions = ArrayBuffer.empty[PythonUDTFSelectedExpression]
    for (_ <- 0 until numSelectedInputExpressions) {
      val expressionSql: String = PythonWorkerUtils.readUTF(dataIn)
      val parsed: Expression = parser.parseExpression(expressionSql)
      val alias: String = PythonWorkerUtils.readUTF(dataIn)
      selectedInputExpressions.append(
        PythonUDTFSelectedExpression(
          parsed,
          if (alias.nonEmpty) Some(alias) else None))
    }
    PythonUDTFAnalyzeResult(
      schema = schema,
      withSinglePartition = withSinglePartition,
      partitionByExpressions = partitionByExpressions.toSeq,
      orderByExpressions = orderBy.toSeq,
      selectedInputExpressions = selectedInputExpressions.toSeq,
      pickledAnalyzeResult = pickledAnalyzeResult)
  }
}
