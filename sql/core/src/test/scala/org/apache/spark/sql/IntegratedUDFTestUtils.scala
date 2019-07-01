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

package org.apache.spark.sql

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.TestUtils
import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, PythonFunction, PythonUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.config.Tests
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.types.StringType

/**
 * This object targets to integrate various UDF test cases so that Scalar UDF, Python UDF and
 * Scalar Pandas UDFs can be tested in SBT & Maven tests.
 *
 * The available UDFs cast input to strings, which take one column as input and return a string
 * type column as output.
 *
 * To register Scala UDF in SQL:
 * {{{
 *   val scalaTestUDF = TestScalaUDF(name = "udf_name")
 *   registerTestUDF(scalaTestUDF, spark)
 * }}}
 *
 * To register Python UDF in SQL:
 * {{{
 *   val pythonTestUDF = TestPythonUDF(name = "udf_name")
 *   registerTestUDF(pythonTestUDF, spark)
 * }}}
 *
 * To register Scalar Pandas UDF in SQL:
 * {{{
 *   val pandasTestUDF = TestScalarPandasUDF(name = "udf_name")
 *   registerTestUDF(pandasTestUDF, spark)
 * }}}
 *
 * To use it in Scala API and SQL:
 * {{{
 *   sql("SELECT udf_name(1)")
 *   spark.range(10).select(expr("udf_name(id)")
 *   spark.range(10).select(pandasTestUDF($"id"))
 * }}}
 */
object IntegratedUDFTestUtils extends SQLHelper {
  import scala.sys.process._

  private lazy val pythonPath = sys.env.getOrElse("PYTHONPATH", "")
  private lazy val sparkHome = if (sys.props.contains(Tests.IS_TESTING.key)) {
    assert(sys.props.contains("spark.test.home") ||
      sys.env.contains("SPARK_HOME"), "spark.test.home or SPARK_HOME is not set.")
    sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
  } else {
    assert(sys.env.contains("SPARK_HOME"), "SPARK_HOME is not set.")
    sys.env("SPARK_HOME")
  }
  // Note that we will directly refer pyspark's source, not the zip from a regular build.
  // It is possible the test is being ran without the build.
  private lazy val sourcePath = Paths.get(sparkHome, "python").toAbsolutePath
  private lazy val py4jPath = Paths.get(
    sparkHome, "python", "lib", PythonUtils.PY4J_ZIP_NAME).toAbsolutePath
  private lazy val pysparkPythonPath = s"$py4jPath:$sourcePath"

  private lazy val isPythonAvailable: Boolean = TestUtils.testCommandAvailable(pythonExec)

  private lazy val isPySparkAvailable: Boolean = isPythonAvailable && Try {
    Process(
      Seq(pythonExec, "-c", "import pyspark"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
    true
  }.getOrElse(false)

  private lazy val isPandasAvailable: Boolean = isPythonAvailable && isPySparkAvailable && Try {
    Process(
      Seq(
        pythonExec,
        "-c",
        "from pyspark.sql.utils import require_minimum_pandas_version;" +
          "require_minimum_pandas_version()"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
    true
  }.getOrElse(false)

  private lazy val isPyArrowAvailable: Boolean = isPythonAvailable && isPySparkAvailable  && Try {
    Process(
      Seq(
        pythonExec,
        "-c",
        "from pyspark.sql.utils import require_minimum_pyarrow_version;" +
          "require_minimum_pyarrow_version()"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
    true
  }.getOrElse(false)

  private lazy val pythonVer = if (isPythonAvailable) {
    Process(
      Seq(pythonExec, "-c", "import sys; print('%d.%d' % sys.version_info[:2])"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!.trim()
  } else {
    throw new RuntimeException(s"Python executable [$pythonExec] is unavailable.")
  }

  // Dynamically pickles and reads the Python instance into JVM side in order to mimic
  // Python native function within Python UDF.
  private lazy val pythonFunc: Array[Byte] = if (shouldTestPythonUDFs) {
    var binaryPythonFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          pythonExec,
          "-c",
          "from pyspark.sql.types import StringType; " +
            "from pyspark.serializers import CloudPickleSerializer; " +
            s"f = open('$path', 'wb');" +
            s"f.write(CloudPickleSerializer().dumps((lambda x: str(x), StringType())))"),
        None,
        "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
      binaryPythonFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryPythonFunc != null)
    binaryPythonFunc
  } else {
    throw new RuntimeException(s"Python executable [$pythonExec] and/or pyspark are unavailable.")
  }

  private lazy val pandasFunc: Array[Byte] = if (shouldTestScalarPandasUDFs) {
    var binaryPandasFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          pythonExec,
          "-c",
          "from pyspark.sql.types import StringType; " +
            "from pyspark.serializers import CloudPickleSerializer; " +
            s"f = open('$path', 'wb');" +
            s"f.write(CloudPickleSerializer().dumps((lambda x: x.apply(str), StringType())))"),
        None,
        "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
      binaryPandasFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryPandasFunc != null)
    binaryPandasFunc
  } else {
    throw new RuntimeException(s"Python executable [$pythonExec] and/or pyspark are unavailable.")
  }

  // Make sure this map stays mutable - this map gets updated later in Python runners.
  private val workerEnv = new java.util.HashMap[String, String]()
  workerEnv.put("PYTHONPATH", s"$pysparkPythonPath:$pythonPath")

  lazy val pythonExec: String = {
    val pythonExec = sys.env.getOrElse(
      "PYSPARK_DRIVER_PYTHON", sys.env.getOrElse("PYSPARK_PYTHON", "python3.6"))
    if (TestUtils.testCommandAvailable(pythonExec)) {
      pythonExec
    } else {
      "python"
    }
  }

  lazy val shouldTestPythonUDFs: Boolean = isPythonAvailable && isPySparkAvailable

  lazy val shouldTestScalarPandasUDFs: Boolean =
    isPythonAvailable && isPandasAvailable && isPyArrowAvailable

  /**
   * A base trait for various UDFs defined in this object.
   */
  sealed trait TestUDF {
    def apply(exprs: Column*): Column

    val prettyName: String
  }

  /**
   * A Python UDF that takes one column and returns a string column.
   * Equivalent to `udf(lambda x: str(x), "string")`
   */
  case class TestPythonUDF(name: String) extends TestUDF {
    private[IntegratedUDFTestUtils] lazy val udf = UserDefinedPythonFunction(
      name = name,
      func = PythonFunction(
        command = pythonFunc,
        envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
        pythonIncludes = List.empty[String].asJava,
        pythonExec = pythonExec,
        pythonVer = pythonVer,
        broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
        accumulator = null),
      dataType = StringType,
      pythonEvalType = PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)

    def apply(exprs: Column*): Column = udf(exprs: _*)

    val prettyName: String = "Regular Python UDF"
  }

  /**
   * A Scalar Pandas UDF that takes one column and returns a string column.
   * Equivalent to `pandas_udf(lambda x: x.apply(str), "string", PandasUDFType.SCALAR)`.
   */
  case class TestScalarPandasUDF(name: String) extends TestUDF {
    private[IntegratedUDFTestUtils] lazy val udf = UserDefinedPythonFunction(
      name = name,
      func = PythonFunction(
        command = pandasFunc,
        envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
        pythonIncludes = List.empty[String].asJava,
        pythonExec = pythonExec,
        pythonVer = pythonVer,
        broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
        accumulator = null),
      dataType = StringType,
      pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      udfDeterministic = true)

    def apply(exprs: Column*): Column = udf(exprs: _*)

    val prettyName: String = "Scalar Pandas UDF"
  }

  /**
   * A Scala UDF that takes one column and returns a string column.
   * Equivalent to `udf((input: Any) => input.toString)`.
   */
  case class TestScalaUDF(name: String) extends TestUDF {
    private[IntegratedUDFTestUtils] lazy val udf = SparkUserDefinedFunction(
      (input: Any) => input.toString,
      StringType,
      inputSchemas = Seq.fill(1)(None))

    def apply(exprs: Column*): Column = udf(exprs: _*)

    val prettyName: String = "Scala UDF"
  }

  /**
   * Register UDFs used in this test case.
   */
  def registerTestUDF(testUDF: TestUDF, session: SparkSession): Unit = testUDF match {
    case udf: TestPythonUDF => session.udf.registerPython(udf.name, udf.udf)
    case udf: TestScalarPandasUDF => session.udf.registerPython(udf.name, udf.udf)
    case udf: TestScalaUDF => session.udf.register(udf.name, udf.udf)
    case other => throw new RuntimeException(s"Unknown UDF class [${other.getClass}]")
  }
}
