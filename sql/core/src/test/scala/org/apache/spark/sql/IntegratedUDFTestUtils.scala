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

import org.scalatest.Assertions._

import org.apache.spark.TestUtils
import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, PythonFunction, PythonUtils, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExprId, PythonUDF}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/**
 * This object targets to integrate various UDF test cases so that Scalar UDF, Python UDF,
 * Scalar Pandas UDF and Grouped Aggregate Pandas UDF can be tested in SBT & Maven tests.
 *
 * The available UDFs are special. For Scalar UDF, Python UDF and Scalar Pandas UDF,
 * it defines an UDF wrapped by cast. So, the input column is casted into string,
 * UDF returns strings as are, and then output column is casted back to the input column.
 * In this way, UDF is virtually no-op.
 *
 * Note that, due to this implementation limitation, complex types such as map, array and struct
 * types do not work with this UDFs because they cannot be same after the cast roundtrip.
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
 *   val df = spark.range(10)
 *   df.select(expr("udf_name(id)")
 *   df.select(pandasTestUDF(df("id")))
 * }}}
 *
 * For Grouped Aggregate Pandas UDF, it defines an UDF that calculates the count using pandas.
 * The UDF returns the count of the given column. In this way, UDF is virtually not no-op.
 *
 * To register Grouped Aggregate Pandas UDF in SQL:
 * {{{
 *   val groupedAggPandasTestUDF = TestGroupedAggPandasUDF(name = "udf_name")
 *   registerTestUDF(groupedAggPandasTestUDF, spark)
 * }}}
 *
 * To use it in Scala API and SQL:
 * {{{
 *   sql("SELECT udf_name(1)")
 *   val df = Seq(
 *     (536361, "85123A", 2, 17850),
 *     (536362, "85123B", 4, 17850),
 *     (536363, "86123A", 6, 17851)
 *   ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
 *
 *   df.groupBy("CustomerID").agg(expr("udf_name(Quantity)"))
 *   df.groupBy("CustomerID").agg(groupedAggPandasTestUDF(df("Quantity")))
 * }}}
 */
object IntegratedUDFTestUtils extends SQLHelper {
  import scala.sys.process._

  private lazy val pythonPath = sys.env.getOrElse("PYTHONPATH", "")

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
        "from pyspark.sql.pandas.utils import require_minimum_pandas_version;" +
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
        "from pyspark.sql.pandas.utils import require_minimum_pyarrow_version;" +
          "require_minimum_pyarrow_version()"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
    true
  }.getOrElse(false)

  lazy val pythonVer: String = if (isPythonAvailable) {
    Process(
      Seq(pythonExec, "-c", "import sys; print('%d.%d' % sys.version_info[:2])"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!.trim()
  } else {
    throw new RuntimeException(s"Python executable [$pythonExec] is unavailable.")
  }

  lazy val pandasVer: String = if (isPandasAvailable) {
    Process(
      Seq(pythonExec, "-c", "import pandas; print(pandas.__version__)"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!.trim()
  } else {
    throw new RuntimeException("Pandas is unavailable.")
  }

  lazy val pyarrowVer: String = if (isPyArrowAvailable) {
    Process(
      Seq(pythonExec, "-c", "import pyarrow; print(pyarrow.__version__)"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!.trim()
  } else {
    throw new RuntimeException("PyArrow is unavailable.")
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
            "f.write(CloudPickleSerializer().dumps((" +
            "lambda x: None if x is None else str(x), StringType())))"),
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
            "f.write(CloudPickleSerializer().dumps((" +
            "lambda x: x.apply(" +
            "lambda v: None if v is None else str(v)), StringType())))"),
        None,
        "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
      binaryPandasFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryPandasFunc != null)
    binaryPandasFunc
  } else {
    throw new RuntimeException(s"Python executable [$pythonExec] and/or pyspark are unavailable.")
  }

  private lazy val pandasGroupedAggFunc: Array[Byte] = if (shouldTestGroupedAggPandasUDFs) {
    var binaryPandasFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          pythonExec,
          "-c",
          "from pyspark.sql.types import IntegerType; " +
            "from pyspark.serializers import CloudPickleSerializer; " +
            s"f = open('$path', 'wb');" +
            "f.write(CloudPickleSerializer().dumps((" +
            "lambda x: x.agg('count'), IntegerType())))"),
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
      "PYSPARK_DRIVER_PYTHON", sys.env.getOrElse("PYSPARK_PYTHON", "python3"))
    if (TestUtils.testCommandAvailable(pythonExec)) {
      pythonExec
    } else {
      "python"
    }
  }

  lazy val shouldTestPythonUDFs: Boolean = isPythonAvailable && isPySparkAvailable

  lazy val shouldTestScalarPandasUDFs: Boolean =
    isPythonAvailable && isPandasAvailable && isPyArrowAvailable

  lazy val shouldTestGroupedAggPandasUDFs: Boolean = shouldTestScalarPandasUDFs

  /**
   * A base trait for various UDFs defined in this object.
   */
  sealed trait TestUDF {
    def apply(exprs: Column*): Column

    val prettyName: String
  }

  class PythonUDFWithoutId(
      name: String,
      func: PythonFunction,
      dataType: DataType,
      children: Seq[Expression],
      evalType: Int,
      udfDeterministic: Boolean,
      resultId: ExprId)
    extends PythonUDF(name, func, dataType, children, evalType, udfDeterministic, resultId) {

    def this(pudf: PythonUDF) = {
      this(pudf.name, pudf.func, pudf.dataType, pudf.children,
        pudf.evalType, pudf.udfDeterministic, pudf.resultId)
    }

    override def toString: String = s"$name(${children.mkString(", ")})"

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): PythonUDFWithoutId = {
      new PythonUDFWithoutId(super.withNewChildrenInternal(newChildren))
    }
  }

  /**
   * A Python UDF that takes one column, casts into string, executes the Python native function,
   * and casts back to the type of input column.
   *
   * Virtually equivalent to:
   *
   * {{{
   *   from pyspark.sql.functions import udf
   *
   *   df = spark.range(3).toDF("col")
   *   python_udf = udf(lambda x: str(x), "string")
   *   casted_col = python_udf(df.col.cast("string"))
   *   casted_col.cast(df.schema["col"].dataType)
   * }}}
   */
  case class TestPythonUDF(name: String) extends TestUDF {
    private[IntegratedUDFTestUtils] lazy val udf = new UserDefinedPythonFunction(
      name = name,
      func = SimplePythonFunction(
        command = pythonFunc,
        envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
        pythonIncludes = List.empty[String].asJava,
        pythonExec = pythonExec,
        pythonVer = pythonVer,
        broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
        accumulator = null),
      dataType = StringType,
      pythonEvalType = PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true) {

      override def builder(e: Seq[Expression]): Expression = {
        assert(e.length == 1, "Defined UDF only has one column")
        val expr = e.head
        assert(expr.resolved, "column should be resolved to use the same type " +
          "as input. Try df(name) or df.col(name)")
        val pythonUDF = new PythonUDFWithoutId(
          super.builder(Cast(expr, StringType) :: Nil).asInstanceOf[PythonUDF])
        Cast(pythonUDF, expr.dataType)
      }
    }

    def apply(exprs: Column*): Column = udf(exprs: _*)

    val prettyName: String = "Regular Python UDF"
  }

  /**
   * A Scalar Pandas UDF that takes one column, casts into string, executes the
   * Python native function, and casts back to the type of input column.
   *
   * Virtually equivalent to:
   *
   * {{{
   *   from pyspark.sql.functions import pandas_udf
   *
   *   df = spark.range(3).toDF("col")
   *   scalar_udf = pandas_udf(lambda x: x.apply(lambda v: str(v)), "string")
   *   casted_col = scalar_udf(df.col.cast("string"))
   *   casted_col.cast(df.schema["col"].dataType)
   * }}}
   */
  case class TestScalarPandasUDF(name: String) extends TestUDF {
    private[IntegratedUDFTestUtils] lazy val udf = new UserDefinedPythonFunction(
      name = name,
      func = SimplePythonFunction(
        command = pandasFunc,
        envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
        pythonIncludes = List.empty[String].asJava,
        pythonExec = pythonExec,
        pythonVer = pythonVer,
        broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
        accumulator = null),
      dataType = StringType,
      pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      udfDeterministic = true) {

      override def builder(e: Seq[Expression]): Expression = {
        assert(e.length == 1, "Defined UDF only has one column")
        val expr = e.head
        assert(expr.resolved, "column should be resolved to use the same type " +
          "as input. Try df(name) or df.col(name)")
        val pythonUDF = new PythonUDFWithoutId(
          super.builder(Cast(expr, StringType) :: Nil).asInstanceOf[PythonUDF])
        Cast(pythonUDF, expr.dataType)
      }
    }

    def apply(exprs: Column*): Column = udf(exprs: _*)

    val prettyName: String = "Scalar Pandas UDF"
  }

  /**
   * A Grouped Aggregate Pandas UDF that takes one column, executes the
   * Python native function calculating the count of the column using pandas.
   *
   * Virtually equivalent to:
   *
   * {{{
   *   import pandas as pd
   *   from pyspark.sql.functions import pandas_udf
   *
   *   df = spark.createDataFrame(
   *       [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
   *
   *   @pandas_udf("double")
   *   def pandas_count(v: pd.Series) -> int:
   *       return v.count()
   *
   *   count_col = pandas_count(df['v'])
   * }}}
   */
  case class TestGroupedAggPandasUDF(name: String) extends TestUDF {
    private[IntegratedUDFTestUtils] lazy val udf = new UserDefinedPythonFunction(
      name = name,
      func = SimplePythonFunction(
        command = pandasGroupedAggFunc,
        envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
        pythonIncludes = List.empty[String].asJava,
        pythonExec = pythonExec,
        pythonVer = pythonVer,
        broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
        accumulator = null),
      dataType = IntegerType,
      pythonEvalType = PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
      udfDeterministic = true)

    def apply(exprs: Column*): Column = udf(exprs: _*)

    val prettyName: String = "Grouped Aggregate Pandas UDF"
  }

  /**
   * A Scala UDF that takes one column, casts into string, executes the
   * Scala native function, and casts back to the type of input column.
   *
   * Virtually equivalent to:
   *
   * {{{
   *   import org.apache.spark.sql.functions.udf
   *
   *   val df = spark.range(3).toDF("col")
   *   val scala_udf = udf((input: Any) => input.toString)
   *   val casted_col = scala_udf(df.col("col").cast("string"))
   *   casted_col.cast(df.schema("col").dataType)
   * }}}
   */
  class TestInternalScalaUDF(name: String) extends SparkUserDefinedFunction(
    (input: Any) => if (input == null) {
      null
    } else {
      input.toString
    },
    StringType,
    inputEncoders = Seq.fill(1)(None),
    name = Some(name)) {

    override def apply(exprs: Column*): Column = {
      assert(exprs.length == 1, "Defined UDF only has one column")
      val expr = exprs.head.expr
      assert(expr.resolved, "column should be resolved to use the same type " +
        "as input. Try df(name) or df.col(name)")
      Column(Cast(createScalaUDF(Cast(expr, StringType) :: Nil), expr.dataType))
    }

    override def withName(name: String): TestInternalScalaUDF = {
      // "withName" should overridden to return TestInternalScalaUDF. Otherwise, the current object
      // is sliced and the overridden "apply" is not invoked.
      new TestInternalScalaUDF(name)
    }
  }

  case class TestScalaUDF(name: String) extends TestUDF {
    private[IntegratedUDFTestUtils] lazy val udf = new TestInternalScalaUDF(name)

    def apply(exprs: Column*): Column = udf(exprs: _*)

    val prettyName: String = "Scala UDF"
  }

  /**
   * Register UDFs used in this test case.
   */
  def registerTestUDF(testUDF: TestUDF, session: SparkSession): Unit = testUDF match {
    case udf: TestPythonUDF => session.udf.registerPython(udf.name, udf.udf)
    case udf: TestScalarPandasUDF => session.udf.registerPython(udf.name, udf.udf)
    case udf: TestGroupedAggPandasUDF => session.udf.registerPython(udf.name, udf.udf)
    case udf: TestScalaUDF => session.udf.register(udf.name, udf.udf)
    case other => throw new RuntimeException(s"Unknown UDF class [${other.getClass}]")
  }
}
