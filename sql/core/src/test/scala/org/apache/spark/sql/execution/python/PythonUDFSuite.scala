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

import org.apache.spark.sql.{AnalysisException, Dataset, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.functions.{array, avg, col, count, transform}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class PythonUDFSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  import IntegratedUDFTestUtils._

  val scalaTestUDF = TestScalaUDF(name = "scalaUDF", Some(LongType))
  val pythonTestUDF = TestPythonUDF(name = "pyUDF", Some(LongType))

  lazy val base = Seq(
    (Some(1), Some(1)), (Some(1), Some(2)), (Some(2), Some(1)),
    (Some(2), Some(2)), (Some(3), Some(1)), (Some(3), Some(2)),
    (None, Some(1)), (Some(3), None), (None, None)).toDF("a", "b")

  test("SPARK-28445: PythonUDF as grouping key and aggregate expressions") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(base("a") + 1), scalaTestUDF(count(base("b"))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(base("a") + 1), pythonTestUDF(count(base("b"))))
    checkAnswer(df1, df2)
  }

  test("SPARK-28445: PythonUDF as grouping key and used in aggregate expressions") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(base("a") + 1) + 1, scalaTestUDF(count(base("b"))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(base("a") + 1) + 1, pythonTestUDF(count(base("b"))))
    checkAnswer(df1, df2)
  }

  test("SPARK-28445: PythonUDF in aggregate expression has grouping key in its arguments") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(scalaTestUDF(base("a") + 1)), scalaTestUDF(count(base("b"))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(pythonTestUDF(base("a") + 1)), pythonTestUDF(count(base("b"))))
    checkAnswer(df1, df2)
  }

  test("SPARK-28445: PythonUDF over grouping key is argument to aggregate function") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(scalaTestUDF(base("a") + 1)),
        scalaTestUDF(count(scalaTestUDF(base("a") + 1))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(pythonTestUDF(base("a") + 1)),
        pythonTestUDF(count(pythonTestUDF(base("a") + 1))))
    checkAnswer(df1, df2)
  }

  test("SPARK-39962: Global aggregation of Pandas UDF should respect the column order") {
    assume(shouldTestPandasUDFs)
    val df = Seq[(java.lang.Integer, java.lang.Integer)]((1, null)).toDF("a", "b")

    val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
    val reorderedDf = df.select("b", "a")
    val actual = reorderedDf.agg(
      pandasTestUDF(reorderedDf("a")), pandasTestUDF(reorderedDf("b")))
    val expected = df.agg(pandasTestUDF(df("a")), pandasTestUDF(df("b")))

    checkAnswer(actual, expected)
  }

  test("SPARK-34265: Instrument Python UDF execution using SQL Metrics") {
    assume(shouldTestPythonUDFs)
    val pythonSQLMetrics = List(
      "data sent to Python workers",
      "data returned from Python workers",
      "number of output rows",
      "time to initialize Python workers",
      "time to start Python workers",
      "time to run Python workers")

    val df = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(pythonTestUDF(base("a") + 1)))
    df.count()

    val statusStore = spark.sharedState.statusStore
    val lastExecId = statusStore.executionsList().last.executionId
    val executionMetrics = statusStore.execution(lastExecId).get.metrics.mkString
    for (metric <- pythonSQLMetrics) {
      assert(executionMetrics.contains(metric))
    }
  }

  test("PythonUDAF pretty name") {
    assume(shouldTestPandasUDFs)
    val udfName = "pandas_udf"
    val df = spark.range(1)
    val pandasTestUDF = TestGroupedAggPandasUDF(name = udfName)
    assert(df.agg(pandasTestUDF(df("id"))).schema.fieldNames.exists(_.startsWith(udfName)))
  }

  test("SPARK-48706: Negative test case for Python UDF in higher order functions") {
    assume(shouldTestPythonUDFs)
    checkError(
      exception = intercept[AnalysisException] {
        spark.range(1).select(transform(array("id"), x => pythonTestUDF(x))).collect()
      },
      condition = "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF",
      parameters = Map("funcName" -> "\"pyUDF(namedlambdavariable())\""),
      context = ExpectedContext(
        "transform", s".*${this.getClass.getSimpleName}.*"))
  }

  test("SPARK-48666: Python UDF execution against partitioned column") {
    assume(shouldTestPythonUDFs)
    withTable("t") {
      spark.range(1).selectExpr("id AS t", "(id + 1) AS p").write.partitionBy("p").saveAsTable("t")
      val table = spark.table("t")
      val newTable = table.withColumn("new_column", pythonTestUDF(table("p")))
      val df = newTable.as("t1").join(
        newTable.as("t2"), col("t1.new_column") === col("t2.new_column"))
      checkAnswer(df, Row(0, 1, 1, 0, 1, 1))
    }
  }

  test("SPARK-53311: Nondeterministic Python UDF pull out in aggregate with grouping") {
    assume(shouldTestPythonUDFs)

    // nondeterministic UDF
    val pythonUDF = TestPythonUDF(name = "foo", Some(LongType), deterministic = false)

    // This query should work without throwing an analysis exception
    // The UDF foo(value) appears in both grouping expressions and aggregate expressions
    // The fix ensures that both instances are properly mapped to the same attribute
    val df = spark.range(1)
      .selectExpr("id", "id % 3 as value")
      .groupBy(pythonUDF(col("value")))
      .agg(avg("id"), pythonUDF(col("value")))

    checkAnswer(df, Row(0, 0.0, 0))
  }

  test("profile test") {
    assume(shouldTestPythonUDFs)
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(base("a") + 1), pythonTestUDF(count(base("b"))))
    df2.collect()
    // scalastyle:off println
    val df3 = spark.readStream.format(
      "org.apache.spark.sql.execution.streaming.sources.PythonProfileSourceProvider").load()
    val q = df3.writeStream.foreachBatch { (df: Dataset[Row], batchId: Long) =>
        // Clear the console before printing the next batch
        print("\u001b[2J")  // Clear entire screen
        print("\u001b[H")   // Move cursor to top-left corner

        df.collect().foreach { row =>
          val listOfMaps = row.get(0).asInstanceOf[collection.mutable.ArraySeq[Map[String, String]]]

          // Group by thread id and name
          val groupedByThread = listOfMaps.groupBy(m =>
            (m.getOrElse("10", "unknown"), m.getOrElse("11", "unknown"))
          )

          groupedByThread.toSeq.sortBy(_._1._1).foreach { case ((threadId, threadName), funcsAll) =>
            println(s"\nFunction stats for (Thread) ($threadId - $threadName)\n")

            // Only top 5 rows for readability
            val funcs = funcsAll.take(5)

            // Dynamic column widths
            val nameWidth =
              (funcs.map(_.getOrElse("15", "").length).maxOption.getOrElse(4) max "name".length) + 2
            val ncallWidth =
              (funcs.map(_.getOrElse("3", "").length).maxOption.getOrElse(5) max "ncall".length) + 2
            val timeWidth = 12

            val fmt =
              s"%-${nameWidth}s %${ncallWidth}s %${timeWidth}s %${timeWidth}s %${timeWidth}s"

            println(fmt.format("name", "ncall", "tsub", "ttot", "tavg"))
            println("-" * (nameWidth + ncallWidth + timeWidth * 3 + 4))

            funcs.foreach { m =>
              val name = {
                val full = m.getOrElse("15", "")
                if (full.length > nameWidth) "..." + full.takeRight(nameWidth - 3) else full
              }

              val ncall = m.getOrElse("3", "")
              val tsub = formatDouble(m.getOrElse("7", ""))
              val ttot = formatDouble(m.getOrElse("6", ""))
              val tavg = formatDouble(m.getOrElse("14", ""))

              println(fmt.format(name, ncall, tsub, ttot, tavg))
            }
          }
        }

        // Helper to format numbers as fixed-point
        def formatDouble(value: String): String = {
          try f"${value.toDouble}%.6f" catch {
            case _: Throwable => value
          }
        }
      }.start()
    q.awaitTermination()
    // scalastyle:on println
  }
}
