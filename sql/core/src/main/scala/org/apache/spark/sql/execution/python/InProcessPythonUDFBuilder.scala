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

import java.util.{Collections, List => JList}

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.internal.config.PLUGINS
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.logical.NamedParametersSupport
import org.apache.spark.sql.classic.{ColumnNodeExpression, ExpressionUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

/**
 * JVM-side builder for in-process [[PythonUDF]] expressions, called from the Python API
 * via py4j's JVM reflection bridge (``sc._jvm.org.apache.spark...InProcessPythonUDFBuilder``).
 *
 * Accepts Java-typed arguments as passed by PySpark's ``sc._jvm`` proxy and returns a
 * [[Column]] backed by a [[PythonUDF]] with the in-process evaluation type.
 */
object InProcessPythonUDFBuilder {

  /**
   * Build a [[Column]] backed by an in-process [[PythonUDF]] expression.
   *
   * @param name            display name (Python function ``__name__``)
   * @param serializedFunc  cloudpickle bytes of the Python UDF
   * @param returnTypeJson  JSON string of the Spark SQL return type
   * @param jColumns        Java List of JVM [[Column]] objects (the UDF inputs)
   * @param deterministic   whether the UDF always returns the same output for the same input;
   *                        set to false for UDFs that use randomness or external state
   * @param pythonVersion   driver's Python major.minor version
   * @return                [[Column]] backed by an in-process [[PythonUDF]] expression
   */
  def build(
      name: String,
      serializedFunc: Array[Byte],
      returnTypeJson: String,
      jColumns: JList[Column],
      deterministic: Boolean,
      pythonVersion: String): Column = {
    val returnType = DataType.fromJson(returnTypeJson)
    val inputExprs = jColumns.asScala.map(col => ColumnNodeExpression(col.node)).toSeq
    NamedParametersSupport.splitAndCheckNamedArguments(inputExprs, name, SQLConf.get.resolver)
    val function = new SimplePythonFunction(
      serializedFunc,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      "",
      pythonVersion,
      Collections.emptyList(),
      null)
    ExpressionUtils.column(PythonUDF(
      name, function, returnType, inputExprs,
      PythonEvalType.SQL_SCALAR_ARROW_INPROCESS_UDF, deterministic))
  }

  private val UnsupportedSessionConfiguration =
    "INVALID_SPARK_CONFIG.UNSUPPORTED_IN_PROCESS_PYTHON_UDF"

  /**
   * Whether `checkConfiguration` rejected the session's settings, which can differ between the
   * session that planned an in-process UDF and another one that re-plans it.
   */
  private[sql] def isUnsupportedSessionConfiguration(e: Throwable): Boolean = e match {
    case e: SparkException => e.getCondition == UnsupportedSessionConfiguration
    case _ => false
  }

  private[sql] def checkConfiguration(conf: SQLConf): Unit = {
    val sparkConf = Option(SparkEnv.get).map(_.conf)
    // The legacy profilers wrap the function with an accumulator, which is not supported.
    val legacyProfilers = Seq("spark.python.profile", "spark.python.profile.memory")
      .filter(key => sparkConf.exists(_.getBoolean(key, false)))
    val unsupported = Seq(
      Option.when(PythonWorkerEnvironment.read(conf).nonEmpty)("spark.pythonWorkerEnv.*"),
      conf.pythonUDFProfiler.map(_ => SQLConf.PYTHON_UDF_PROFILER.key),
      sparkConf.flatMap(_.get(PYSPARK_EXECUTOR_MEMORY)).filter(_ > 0)
        .map(_ => PYSPARK_EXECUTOR_MEMORY.key)).flatten ++ legacyProfilers
    unsupported.headOption.foreach { config =>
      throw new SparkException(
        errorClass = UnsupportedSessionConfiguration,
        messageParameters = Map("config" -> config),
        cause = null)
    }
    // Subclasses inherit the executor plugin that initializes the interpreter.
    val plugin = classOf[InProcessPythonPlugin]
    val configured = Option(SparkEnv.get).toSeq.flatMap(_.conf.get(PLUGINS))
    if (!configured.exists { name =>
        Try(plugin.isAssignableFrom(Utils.classForName(name, initialize = false)))
          .getOrElse(false)
      }) {
      throw new SparkException(
        errorClass = "INVALID_SPARK_CONFIG.MISSING_IN_PROCESS_PYTHON_PLUGIN",
        messageParameters = Map("plugin" -> plugin.getName),
        cause = null)
    }
  }

}
