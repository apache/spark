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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.types._

/**
 * End-to-end tests for the Arrow columnar Python UDF input path.
 *
 * Verifies that when reading from an Arrow-backed DataSource V2 connector,
 * ArrowEvalPythonExec uses the columnar path (no ColumnarToRowExec) and
 * produces correct results.
 */
class ArrowColumnarPythonUDFSuite extends SharedSparkSession {

  import IntegratedUDFTestUtils._

  private class CharStorageUDT extends UserDefinedType[String] {
    override def sqlType: DataType = CharType(3)
    override def serialize(value: String): Any = value
    override def deserialize(value: Any): String = value.toString
    override def userClass: Class[String] = classOf[String]
  }

  private val arrowSource =
    "org.apache.spark.sql.execution.python.ArrowBackedDataSourceV2"

  private def readArrowSource(numRows: Int = 100, numPartitions: Int = 1) = {
    spark.read
      .format(arrowSource)
      .option("numRows", numRows.toString)
      .option("numPartitions", numPartitions.toString)
      .load()
  }

  private def collectNodes[T <: SparkPlan](
      plan: SparkPlan)(implicit tag: reflect.ClassTag[T]): Seq[T] = {
    plan.collect {
      case p if tag.runtimeClass.isInstance(p) => p.asInstanceOf[T]
    }
  }

  test("CHAR/VARCHAR output normalization also unwraps UDT siblings") {
    val logicalSchema =
      StructType(Seq(
        StructField("c", CharType(3)),
        StructField("point", new ExamplePointUDT()),
        StructField("nested", new CharStorageUDT())))
    val expectedSchema = StructType(
      Seq(
        StructField("c", StringType),
        StructField("point", ArrayType(DoubleType, containsNull = false)),
        StructField("nested", StringType)))

    assert(
      ColumnarArrowEvalPythonEvaluatorFactory.toPhysicalType(logicalSchema) === expectedSchema)
    assert(CharVarcharUtils.physicalTypeHasCharVarchar(logicalSchema))
  }

  test("Arrow-backed source: no ColumnarToRowExec before ArrowEvalPythonExec") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_test_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource()
      // Select all columns to avoid column pruning inserting a
      // ProjectExec (which does not support columnar) between the
      // scan and ArrowEvalPythonExec.
      val result = df.selectExpr(
        "id", "name", "value", "data",
        "arrow_test_udf(id) as udf_id")
      val plan = result.queryExecution.executedPlan

      // ArrowEvalPythonExec should be present.
      val arrowExecs = collectNodes[ArrowEvalPythonExec](plan)
      assert(arrowExecs.nonEmpty,
        s"Expected ArrowEvalPythonExec in plan:\n$plan")

      // The direct child of ArrowEvalPythonExec should support
      // columnar (the scan), with no ColumnarToRowExec in between.
      val arrowExec = arrowExecs.head
      assert(arrowExec.child.supportsColumnar,
        "ArrowEvalPythonExec child should support columnar " +
          s"when reading from Arrow-backed source:\n$plan")
    }
  }

  test("Arrow-backed source: correctness of scalar pandas UDF") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_str_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource(numRows = 10)
      // The test pandas UDF converts input to string.
      val result = df.selectExpr("id", "arrow_str_udf(id) as udf_id")
      val rows = result.collect()

      assert(rows.length == 10)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i, s"id mismatch at row $i")
        assert(row.getString(1) == i.toString,
          s"UDF result mismatch at row $i")
      }
    }
  }

  test("Arrow-backed source: CHAR/VARCHAR output checks") {
    assume(shouldTestPandasUDFs)
    withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> "true",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val charUDF = TestTypedScalarPandasUDF(
        name = "arrow_char_udf", returnType = CharType(4))
      val varcharUDF = TestTypedScalarPandasUDF(
        name = "arrow_varchar_udf", returnType = VarcharType(3))
      registerTestUDF(charUDF, spark)
      registerTestUDF(varcharUDF, spark)

      val df = readArrowSource(numRows = 10)
      val padded = df.selectExpr(
        "id", "name", "value", "data",
        "arrow_char_udf(id) as udf_id")
      val arrowExec = collectNodes[ArrowEvalPythonExec](
        padded.queryExecution.executedPlan).head
      assert(arrowExec.child.supportsColumnar,
        "ArrowEvalPythonExec should retain its Arrow-backed columnar child")
      assert(padded.collect().map(_.getString(4)).toSeq ===
        (0 until 10).map(_.toString.padTo(4, ' ').mkString))

      val exception = intercept[SparkRuntimeException] {
        df.selectExpr(
          "id", "name", "value", "data",
          "arrow_varchar_udf(name) as udf_name").collect()
      }
      assert(exception.getMessage.contains("EXCEED_LIMIT_LENGTH"))
    }
  }

  test("Arrow-backed source: default policy exposes checked CHAR/VARCHAR output as STRING") {
    assume(shouldTestPandasUDFs)
    withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> "true",
        SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "false",
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "false",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
      val charUDF = TestTypedScalarPandasUDF(
        name = "default_arrow_char_udf", returnType = CharType(4))
      registerTestUDF(charUDF, spark)

      val result = readArrowSource(numRows = 1)
        .selectExpr("default_arrow_char_udf(id) as udf_id")

      assert(result.schema.head.dataType === StringType)
      assert(result.head().getString(0) === "0   ")
    }
  }

  test("Arrow-backed source: higher-order CHAR/VARCHAR output checks") {
    assume(shouldTestPandasUDFs)
    withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> "true",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val charUDF = TestTypedScalarPandasUDF(
        name = "arrow_hof_char_udf", returnType = CharType(4))
      val varcharUDF = TestTypedScalarPandasUDF(
        name = "arrow_hof_varchar_udf", returnType = VarcharType(3))
      registerTestUDF(charUDF, spark)
      registerTestUDF(varcharUDF, spark)

      val df = readArrowSource(numRows = 10)
      val padded = df.selectExpr(
        "id", "name", "value", "data",
        "transform(array(id), x -> arrow_hof_char_udf(x)) as udf_values")
      val arrowExec = collectNodes[ArrowEvalPythonExec](
        padded.queryExecution.executedPlan).head
      assert(arrowExec.child.supportsColumnar,
        "ArrowEvalPythonExec should retain its Arrow-backed columnar child")
      assert(padded.collect().map(_.getSeq[String](4)).toSeq ===
        (0 until 10).map(index => Seq(index.toString.padTo(4, ' ').mkString)))

      val exception = intercept[SparkRuntimeException] {
        df.selectExpr(
          "id", "name", "value", "data",
          "transform(array(name), x -> arrow_hof_varchar_udf(x)) as udf_values").collect()
      }
      assert(exception.getMessage.contains("EXCEED_LIMIT_LENGTH"))
    }
  }

  test("Arrow-backed source: legacy CHAR/VARCHAR output remains unchecked") {
    assume(shouldTestPandasUDFs)
    withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> "true",
        SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true",
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "false",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
      val charUDF = TestTypedScalarPandasUDF(
        name = "legacy_arrow_char_udf", returnType = CharType(4))
      val varcharUDF = TestTypedScalarPandasUDF(
        name = "legacy_arrow_varchar_udf", returnType = VarcharType(3))
      registerTestUDF(charUDF, spark)
      registerTestUDF(varcharUDF, spark)

      val result = readArrowSource(numRows = 10).selectExpr(
        "id", "name", "value", "data",
        "legacy_arrow_char_udf(id) as udf_id",
        "legacy_arrow_varchar_udf(name) as udf_name")
      val arrowExec = collectNodes[ArrowEvalPythonExec](
        result.queryExecution.executedPlan).head
      assert(arrowExec.child.supportsColumnar,
        "ArrowEvalPythonExec should retain its Arrow-backed columnar child")

      val rows = result.collect()
      rows.zipWithIndex.foreach { case (row, index) =>
        assert(row.getString(4) === index.toString)
        assert(row.getString(5) === s"row_$index")
      }
    }
  }

  test("Arrow-backed source: multiple UDF columns") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val udf1 = TestScalarPandasUDF(
        name = "arrow_udf1", returnType = Some(StringType))
      val udf2 = TestScalarPandasUDF(
        name = "arrow_udf2", returnType = Some(StringType))
      registerTestUDF(udf1, spark)
      registerTestUDF(udf2, spark)

      val df = readArrowSource(numRows = 10)
      val result = df.selectExpr(
        "id", "arrow_udf1(id) as u1", "arrow_udf2(name) as u2")
      val rows = result.collect()

      assert(rows.length == 10)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i)
        assert(row.getString(1) == i.toString)
        assert(row.getString(2) == s"row_$i")
      }
    }
  }

  test("Arrow-backed source: scan supportsColumnar is true") {
    val df = readArrowSource()
    val plan = df.queryExecution.executedPlan
    // The top-level plan may be ColumnarToRowExec (for Spark's row output),
    // but the scan underneath should support columnar.
    val scans = plan.collect {
      case p if p.supportsColumnar => p
    }
    assert(scans.nonEmpty,
      s"Arrow-backed source should have a columnar-capable scan:\n$plan")
  }

  test("Row-based source: child does not support columnar (baseline)") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "row_test_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      // spark.range does not produce Arrow-backed columnar output.
      val df = spark.range(100).toDF("id")
      val result = df.selectExpr("row_test_udf(id)")
      val plan = result.queryExecution.executedPlan

      val arrowExecs = collectNodes[ArrowEvalPythonExec](plan)
      arrowExecs.foreach { exec =>
        assert(!exec.child.supportsColumnar,
          "Row-based source child should not support columnar:" +
            s"\n$plan")
      }
    }
  }

  test("Arrow-backed source: multiple partitions") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_mp_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource(numRows = 100, numPartitions = 4)
      val result = df.selectExpr("id", "arrow_mp_udf(id) as udf_id")
      val rows = result.collect().sortBy(_.getInt(0))

      assert(rows.length == 100)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i)
        assert(row.getString(1) == i.toString)
      }
    }
  }
}
