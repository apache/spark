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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.HashMap

import scala.collection.JavaConverters._

import net.razorvine.pickle.Pickler

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.api.python.{PythonEvalType, PythonFunction, PythonRDD, SpecialLengths}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, FunctionTableSubqueryArgumentExpression, PythonUDAF, PythonUDF, PythonUDTF}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.internal.SQLConf
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
    if (pythonEvalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF) {
      PythonUDAF(name, func, dataType, e, udfDeterministic)
    } else {
      PythonUDF(name, func, dataType, e, pythonEvalType, udfDeterministic)
    }
  }

  /** Returns a [[Column]] that will evaluate to calling this UDF with the given input. */
  def apply(exprs: Column*): Column = {
    fromUDFExpr(builder(exprs.map(_.expr)))
  }

  /**
   * Returns a [[Column]] that will evaluate the UDF expression with the given input.
   */
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
    udfDeterministic: Boolean) {

  def this(
      name: String,
      func: PythonFunction,
      returnType: StructType,
      udfDeterministic: Boolean) = {
    this(name, func, Some(returnType), udfDeterministic)
  }

  def this(
      name: String,
      func: PythonFunction,
      udfDeterministic: Boolean) = {
    this(name, func, None, udfDeterministic)
  }

  def builder(e: Seq[Expression]): LogicalPlan = {
    val udtf = PythonUDTF(
      name = name,
      func = func,
      elementSchema = returnType.getOrElse(UserDefinedPythonTableFunction.analyzeInPython(func, e)),
      children = e,
      udfDeterministic = udfDeterministic)
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
    val udtf = builder(exprs.map(_.expr))
    Dataset.ofRows(session, udtf)
  }
}

object UserDefinedPythonTableFunction {

  private[this] val workerModule = "pyspark.sql.worker.analyze_udtf"

  def analyzeInPython(func: PythonFunction, e: Seq[Expression]): StructType = {
    val env = SparkEnv.get
    val bufferSize: Int = env.conf.get(BUFFER_SIZE)
    val authSocketTimeout = env.conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)
    val reuseWorker = env.conf.get(PYTHON_WORKER_REUSE)
    val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

    val envVars = new HashMap[String, String](func.envVars)
    val pythonExec = func.pythonExec
    val pythonVer = func.pythonVer

    if (reuseWorker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    if (simplifiedTraceback) {
      envVars.put("SPARK_SIMPLIFIED_TRACEBACK", "1")
    }
    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)

    val pickler = new Pickler(/* useMemo = */ true,
      /* valueCompare = */ false)

    try {
      val (worker: Socket, _) =
        env.createPythonWorker(pythonExec, workerModule, envVars.asScala.toMap)

      val dataOut =
        new DataOutputStream(new BufferedOutputStream(worker.getOutputStream, bufferSize))
      val dataIn = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

      // Python version of driver
      PythonRDD.writeUTF(pythonVer, dataOut)

      // Send Python UDTF
      dataOut.writeInt(func.command.length)
      dataOut.write(func.command.toArray)

      // Send arguments
      dataOut.writeInt(e.length)
      e.foreach { expr =>
        PythonRDD.writeUTF(expr.dataType.json, dataOut)
        if (expr.foldable) {
          dataOut.writeBoolean(true)
          val obj = pickler.dumps(EvaluatePython.toJava(expr.eval(), expr.dataType))
          dataOut.writeInt(obj.length)
          dataOut.write(obj)
        } else {
          dataOut.writeBoolean(false)
        }
        dataOut.writeBoolean(expr.isInstanceOf[FunctionTableSubqueryArgumentExpression])
      }

      dataOut.writeInt(SpecialLengths.END_OF_STREAM)
      dataOut.flush()

      // Receive the schema
      val schema = dataIn.readInt() match {
        case length if length >= 0 =>
          val obj = new Array[Byte](length)
          dataIn.readFully(obj)
          DataType.fromJson(new String(obj, StandardCharsets.UTF_8)).asInstanceOf[StructType]

        case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
          val exLength = dataIn.readInt()
          val obj = new Array[Byte](exLength)
          dataIn.readFully(obj)
          throw new AnalysisException(new String(obj, StandardCharsets.UTF_8))
      }

      dataIn.readInt() match {
        case SpecialLengths.END_OF_STREAM if reuseWorker =>
          env.releasePythonWorker(pythonExec, workerModule, envVars.asScala.toMap, worker)
        case _ =>
          env.destroyPythonWorker(pythonExec, workerModule, envVars.asScala.toMap, worker)
      }

      schema
    } catch {
      case eof: EOFException =>
        throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
    }
  }
}
