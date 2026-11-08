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

import org.apache.spark.SparkConf
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.execution.columnar.{ArrowCachedBatchSerializer, InMemoryRelation}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.{ExamplePoint, SharedSparkSession}
import org.apache.spark.sql.types.StringType

/**
 * Tests Arrow Python UDFs over the Arrow cache, whose scan returns rows or Arrow-backed batches,
 * and closes the root of the previous batch when it returns the next one.
 */
class ArrowCachedPythonUDFSuite extends SharedSparkSession {

  import IntegratedUDFTestUtils._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(StaticSQLConf.SPARK_CACHE_SERIALIZER.key, classOf[ArrowCachedBatchSerializer].getName)

  // The serializer of the cache is kept for the JVM once it is used.
  override def beforeAll(): Unit = {
    InMemoryRelation.clearSerializer()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      InMemoryRelation.clearSerializer()
    }
  }

  test("Arrow Python UDFs over the Arrow cache, with and without columnar input") {
    assume(shouldTestPandasUDFs)
    // The scan of the Arrow cache also returns rows, so no ColumnarToRowExec is added below
    // ArrowEvalPythonExec, whose child still supports columnar: it must still read rows when
    // columnar input is disabled, as it is by default. With columnar input, the cache closes the
    // root of a batch when it returns the next one, so the pass-through columns of its 200
    // batches must be copied before then.
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1000") {
      val pandasUDF = TestScalarPandasUDF(
        name = "cached_str_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val cached = spark.range(0, 200000, 1, 1).selectExpr("id", "CAST(id AS STRING) AS s")
        .cache()
      try {
        Seq(false, true).foreach { columnarInput =>
          withSQLConf(
              SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> columnarInput.toString) {
            // The test UDF casts its input to string, which is removed on a string column.
            val result = cached.selectExpr("id", "s", "cached_str_udf(s) AS u")
            val arrowExec = result.queryExecution.executedPlan.collect {
              case exec: ArrowEvalPythonExec => exec
            }
            assert(arrowExec.nonEmpty && arrowExec.head.child.supportsColumnar &&
              arrowExec.head.supportsColumnar == columnarInput)

            val rows = result.collect()
            assert(rows.map(_.getLong(0)).toSeq == (0L until 200000L))
            rows.foreach { row =>
              val id = row.getLong(0)
              assert(row.getString(1) == id.toString, s"pass-through mismatch for id $id")
              assert(row.getString(2) == id.toString, s"UDF result mismatch for id $id")
            }
          }
        }
      } finally {
        cached.unpersist()
      }
    }
  }

  test("SPARK-60325: Columnar input passes through null type and user-defined type columns") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "cached_types_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val session = spark
      import session.implicits._
      val numRows = 1000
      val points = (0 until numRows).map(i => (i.toString, new ExamplePoint(i, i + 1.0)))
      val cached = spark.sparkContext.parallelize(points, 4).toDF("s", "p")
        .selectExpr("s", "p", "NULL AS n")
        .cache()
      try {
        val result = cached.selectExpr("s", "p", "n", "cached_types_udf(s) AS u")
        val arrowExec = result.queryExecution.executedPlan.collect {
          case exec: ArrowEvalPythonExec => exec
        }
        assert(arrowExec.nonEmpty && arrowExec.head.supportsColumnar)

        val rows = result.collect()
        assert(rows.map(_.getString(0).toInt).toSet == (0 until numRows).toSet)
        rows.foreach { row =>
          val id = row.getString(0).toInt
          assert(row.getAs[ExamplePoint](1) == new ExamplePoint(id, id + 1.0))
          assert(row.isNullAt(2))
          assert(row.getString(3) == id.toString)
        }
      } finally {
        cached.unpersist()
      }
    }
  }
}
