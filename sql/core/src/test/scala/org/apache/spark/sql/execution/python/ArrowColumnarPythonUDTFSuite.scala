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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.{DataFrame, IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/**
 * End-to-end tests for the Arrow columnar input path of Arrow-optimized Python UDTFs.
 *
 * Verifies that when reading from an Arrow-backed DataSource V2 connector,
 * ArrowEvalPythonUDTFExec reads its arguments from the columnar child (no ColumnarToRowExec)
 * and produces the same results as the row-based path.
 */
class ArrowColumnarPythonUDTFSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import IntegratedUDFTestUtils._

  private val arrowSource =
    "org.apache.spark.sql.execution.python.ArrowBackedDataSourceV2"

  // Yields a variable number of rows per input row (none for multiples of 5), then one row from
  // `terminate()` with the number of processed rows.
  private val pythonScript: String =
    """
      |class CountingUDTF:
      |    def __init__(self):
      |        self._count = 0
      |
      |    def eval(self, id: int, name: str):
      |        self._count += 1
      |        for i in range(id % 5):
      |            yield id * 10 + i, None if name is None else name + "_" + str(i)
      |
      |    def terminate(self):
      |        yield -self._count, "terminate"
      |""".stripMargin

  private val returnType: StructType = StructType.fromDDL("x int, s string")

  private val arrowUDTF: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      "CountingUDTF",
      pythonScript,
      Some(returnType),
      evalType = PythonEvalType.SQL_ARROW_TABLE_UDF)

  private def withArrowSource(numRows: Int, numPartitions: Int)(f: => Unit): Unit = {
    withTempView("arrow_t") {
      spark.read
        .format(arrowSource)
        .option("numRows", numRows.toString)
        .option("numPartitions", numPartitions.toString)
        .load()
        .createOrReplaceTempView("arrow_t")
      spark.udtf.registerPython("counting_udtf", arrowUDTF)
      f
    }
  }

  private def udtfExec(df: DataFrame): ArrowEvalPythonUDTFExec = {
    val execs = collect(df.queryExecution.executedPlan) {
      case e: ArrowEvalPythonUDTFExec => e
    }
    assert(execs.size == 1, s"Expected one ArrowEvalPythonUDTFExec in plan:\n" +
      df.queryExecution.executedPlan)
    execs.head
  }

  // Whether the input of the UDTF goes through a ColumnarToRowExec (possibly inside whole-stage
  // codegen).
  private def hasColumnarToRowChild(plan: SparkPlan): Boolean =
    plan.children.exists(_.exists(_.isInstanceOf[ColumnarToRowExec]))

  // All the columns of the source are referenced, so that no ProjectExec (which does not support
  // columnar) is inserted between the scan and the UDTF.
  private val lateralQuery =
    """
      |SELECT t.id, t.name, t.value, t.data, f.x, f.s
      |FROM arrow_t t, LATERAL counting_udtf(t.id, t.name) f
      |""".stripMargin

  test("Arrow-backed source: no ColumnarToRowExec before ArrowEvalPythonUDTFExec") {
    assume(shouldTestPythonUDFs)
    withArrowSource(numRows = 100, numPartitions = 1) {
      val exec = udtfExec(sql(lateralQuery))
      assert(exec.child.supportsColumnar,
        s"ArrowEvalPythonUDTFExec should read its columnar child directly:\n$exec")
      assert(!hasColumnarToRowChild(exec))
    }
  }

  test("Arrow-backed source: same results as the row-based path") {
    assume(shouldTestPythonUDFs)
    withArrowSource(numRows = 3000, numPartitions = 3) {
      val expected = withSQLConf(
          SQLConf.ARROW_PYSPARK_UDTF_COLUMNAR_INPUT_ENABLED.key -> "false") {
        val df = sql(lateralQuery)
        assert(hasColumnarToRowChild(udtfExec(df)))
        df.collect()
      }
      // Multiple output rows per input row, input rows without output, and terminate() rows.
      assert(expected.exists(_.getString(5) == "terminate"))
      assert(expected.length > 3000)

      val df = sql(lateralQuery)
      assert(udtfExec(df).child.supportsColumnar)
      checkAnswer(df, expected.toSeq)
    }
  }

  test("Arrow-backed source: pruned child output") {
    assume(shouldTestPythonUDFs)
    withArrowSource(numRows = 500, numPartitions = 2) {
      // `id` and `name` are only UDTF arguments: the buffered child rows only have `value` and
      // `data`.
      val query =
        """
          |SELECT t.value, t.data, f.x, f.s
          |FROM arrow_t t, LATERAL counting_udtf(t.id, t.name) f
          |""".stripMargin
      val expected = withSQLConf(
          SQLConf.ARROW_PYSPARK_UDTF_COLUMNAR_INPUT_ENABLED.key -> "false") {
        sql(query).collect()
      }
      val df = sql(query)
      val exec = udtfExec(df)
      assert(exec.child.supportsColumnar)
      assert(exec.requiredChildOutput.map(_.name) == Seq("value", "data"))
      checkAnswer(df, expected.toSeq)
    }
  }

  test("UDTF arguments that are not child columns use the row-based path") {
    assume(shouldTestPythonUDFs)
    withArrowSource(numRows = 10, numPartitions = 1) {
      val df = sql(
        """
          |SELECT t.id, t.name, t.value, t.data, f.x, f.s
          |FROM arrow_t t, LATERAL counting_udtf(t.id + 1, 'a') f
          |""".stripMargin)
      val exec = udtfExec(df)
      assert(!exec.supportsColumnar)
      assert(df.collect().exists(_.getString(5) == "terminate"))
    }
  }
}
