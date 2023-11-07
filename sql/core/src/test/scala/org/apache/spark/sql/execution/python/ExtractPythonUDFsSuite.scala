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

import org.apache.spark.sql.catalyst.plans.logical.{ArrowEvalPython, BatchEvalPython, Limit, LocalLimit}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, SparkPlanTest}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ExtractPythonUDFsSuite extends SparkPlanTest with SharedSparkSession {
  import testImplicits._

  val batchedPythonUDF = new MyDummyPythonUDF
  val batchedNondeterministicPythonUDF = new MyDummyNondeterministicPythonUDF
  val scalarPandasUDF = new MyDummyScalarPandasUDF

  private def collectBatchExec(plan: SparkPlan): Seq[BatchEvalPythonExec] = plan.collect {
    case b: BatchEvalPythonExec => b
  }

  private def collectArrowExec(plan: SparkPlan): Seq[ArrowEvalPythonExec] = plan.collect {
    case b: ArrowEvalPythonExec => b
  }

  test("Chained Batched Python UDFs should be combined to a single physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", batchedPythonUDF(col("a")))
      .withColumn("d", batchedPythonUDF(col("c")))
    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
  }

  test("Chained Scalar Pandas UDFs should be combined to a single physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", scalarPandasUDF(col("a")))
      .withColumn("d", scalarPandasUDF(col("c")))
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(arrowEvalNodes.size == 1)
  }

  test("Mixed Batched Python UDFs and Pandas UDF should be separate physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", batchedPythonUDF(col("a")))
      .withColumn("d", scalarPandasUDF(col("b")))

    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
    assert(arrowEvalNodes.size == 1)
  }

  test("Independent Batched Python UDFs and Scalar Pandas UDFs should be combined separately") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c1", batchedPythonUDF(col("a")))
      .withColumn("c2", batchedPythonUDF(col("c1")))
      .withColumn("d1", scalarPandasUDF(col("a")))
      .withColumn("d2", scalarPandasUDF(col("d1")))

    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
    assert(arrowEvalNodes.size == 1)
  }

  test("Dependent Batched Python UDFs and Scalar Pandas UDFs should not be combined") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c1", batchedPythonUDF(col("a")))
      .withColumn("d1", scalarPandasUDF(col("c1")))
      .withColumn("c2", batchedPythonUDF(col("d1")))
      .withColumn("d2", scalarPandasUDF(col("c2")))

    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 2)
    assert(arrowEvalNodes.size == 2)
  }

  test("Python UDF should not break column pruning/filter pushdown -- Parquet V1") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { f =>
        spark.range(10).select($"id".as("a"), $"id".as("b"))
          .write.parquet(f.getCanonicalPath)
        val df = spark.read.parquet(f.getCanonicalPath)

        withClue("column pruning") {
          val query = df.filter(batchedPythonUDF($"a")).select($"a")

          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scanNodes.length == 1)
          assert(scanNodes.head.output.map(_.name) == Seq("a"))
        }

        withClue("filter pushdown") {
          val query = df.filter($"a" > 1 && batchedPythonUDF($"a"))
          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scanNodes.length == 1)
          // $"a" is not null and $"a" > 1
          assert(scanNodes.head.dataFilters.length == 2)
          assert(scanNodes.head.dataFilters.flatMap(_.references.map(_.name)).distinct == Seq("a"))
        }
      }
    }
  }

  test("Python UDF should not break column pruning/filter pushdown -- Parquet V2") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { f =>
        spark.range(10).select($"id".as("a"), $"id".as("b"))
          .write.parquet(f.getCanonicalPath)
        val df = spark.read.parquet(f.getCanonicalPath)

        withClue("column pruning") {
          val query = df.filter(batchedPythonUDF($"a")).select($"a")

          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: BatchScanExec => scan
          }
          assert(scanNodes.length == 1)
          assert(scanNodes.head.output.map(_.name) == Seq("a"))
        }

        withClue("filter pushdown") {
          val query = df.filter($"a" > 1 && batchedPythonUDF($"a"))
          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: BatchScanExec => scan
          }
          assert(scanNodes.length == 1)
          // $"a" is not null and $"a" > 1
          val filters = scanNodes.head.scan.asInstanceOf[ParquetScan].pushedFilters
          assert(filters.length == 2)
          assert(filters.flatMap(_.references).distinct === Array("a"))
        }
      }
    }
  }

  test("SPARK-33303: Deterministic UDF calls are deduplicated") {
    val df = Seq("Hello").toDF("a")

    val df2 = df.withColumn("c", batchedPythonUDF(col("a"))).withColumn("d", col("c"))
    val pythonEvalNodes2 = collectBatchExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes2.size == 1)
    assert(pythonEvalNodes2.head.udfs.size == 1)

    val df3 = df.withColumns(Seq("c", "d"),
      Seq(batchedPythonUDF(col("a")), batchedPythonUDF(col("a"))))
    val pythonEvalNodes3 = collectBatchExec(df3.queryExecution.executedPlan)
    assert(pythonEvalNodes3.size == 1)
    assert(pythonEvalNodes3.head.udfs.size == 1)

    val df4 = df.withColumn("c", batchedNondeterministicPythonUDF(col("a")))
      .withColumn("d", col("c"))
    val pythonEvalNodes4 = collectBatchExec(df4.queryExecution.executedPlan)
    assert(pythonEvalNodes4.size == 1)
    assert(pythonEvalNodes4.head.udfs.size == 1)

    val df5 = df.withColumns(Seq("c", "d"),
      Seq(batchedNondeterministicPythonUDF(col("a")), batchedNondeterministicPythonUDF(col("a"))))
    val pythonEvalNodes5 = collectBatchExec(df5.queryExecution.executedPlan)
    assert(pythonEvalNodes5.size == 1)
    assert(pythonEvalNodes5.head.udfs.size == 2)
  }

  test("Infers LocalLimit for Python evaluator") {
    val df = Seq(("Hello", 4), ("World", 8)).toDF("a", "b")

    // Check that PushProjectionThroughLimit brings GlobalLimit - LocalLimit to the top (for
    // CollectLimit) and that LimitPushDown keeps LocalLimit under UDF.
    val df2 = df.limit(1).select(batchedPythonUDF(col("b")))
    assert(df2.queryExecution.optimizedPlan match {
      case Limit(_, _) => true
    })
    assert(df2.queryExecution.optimizedPlan.find {
      case b: BatchEvalPython => b.child.isInstanceOf[LocalLimit]
      case _ => false
    }.isDefined)

    val df3 = df.limit(1).select(scalarPandasUDF(col("b")))
    assert(df3.queryExecution.optimizedPlan match {
      case Limit(_, _) => true
    })
    assert(df3.queryExecution.optimizedPlan.find {
      case a: ArrowEvalPython => a.child.isInstanceOf[LocalLimit]
      case _ => false
    }.isDefined)

    val df4 = df.limit(1).select(batchedPythonUDF(col("b")), scalarPandasUDF(col("b")))
    assert(df4.queryExecution.optimizedPlan match {
      case Limit(_, _) => true
    })
    val evalsWithLimit = df4.queryExecution.optimizedPlan.collect {
      case b: BatchEvalPython if b.child.isInstanceOf[LocalLimit] => b
      case a: ArrowEvalPython if a.child.isInstanceOf[LocalLimit] => a
    }
    assert(evalsWithLimit.length == 2)

    // Check that LimitPushDown properly pushes LocalLimit past EvalPython operators.
    val df5 = df.select(batchedPythonUDF(col("b")), scalarPandasUDF(col("b"))).limit(1)
    df5.queryExecution.optimizedPlan.foreach {
      case b: BatchEvalPython => assert(b.child.isInstanceOf[LocalLimit])
      case a: ArrowEvalPython => assert(a.child.isInstanceOf[LocalLimit])
      case _ =>
    }
  }
}

