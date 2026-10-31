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
package org.apache.spark.sql.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow}
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable}
import org.apache.spark.sql.connector.catalog.functions.{BucketFunction, ScalarFunction, UnboundBucketFunction}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class InMemoryBucketFixtureSuite extends DistributionAndOrderingSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_BUCKETING_ENABLED, true)
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED, false)

  before {
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), UnboundBucketFunction)
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  private val fallbackTypes = Seq(
    ("DATE", "DATE '2020-01-02'"),
    ("DECIMAL(10, 2)", "CAST(12.34 AS DECIMAL(10, 2))"),
    ("DOUBLE", "CAST(12.34 AS DOUBLE)"),
    ("FLOAT", "CAST(12.34 AS FLOAT)"),
    ("BOOLEAN", "TRUE"),
    ("ARRAY<INT>", "array(1, 2)"),
    ("STRUCT<a: INT>", "named_struct('a', 1)"))

  fallbackTypes.foreach { case (dataType, value) =>
    test(s"bucket fixtures retain nullable $dataType writes without unsafe partition metadata") {
      val table = "testcat.ns.bucket_fallback"
      sql(s"CREATE TABLE $table (k $dataType, id INT) PARTITIONED BY (bucket(4, k))")
      sql(s"INSERT INTO $table VALUES ($value, 1), (NULL, 2)")
      val result = sql(s"SELECT k, id FROM $table")
      checkAnswer(result, sql(s"SELECT $value AS k, 1 AS id").collect().toSeq :+ Row(null, 2))
      val scans = result.queryExecution.executedPlan.collect { case scan: BatchScanExec => scan }
      assert(scans.nonEmpty)
      assert(scans.forall(_.outputPartitioning.isInstanceOf[UnknownPartitioning]))
    }
  }

  private val supportedTypes = Seq(
    ("TINYINT", "CAST(-7 AS TINYINT)"),
    ("SMALLINT", "CAST(-7 AS SMALLINT)"),
    ("INT", "-7"),
    ("BIGINT", "CAST(-7 AS BIGINT)"),
    ("TIMESTAMP", "TIMESTAMP '2020-01-02 03:04:05.123456'"),
    ("TIMESTAMP_NTZ", "CAST('2020-01-02 03:04:05.123456' AS TIMESTAMP_NTZ)"),
    ("STRING", "'abc'"),
    ("BINARY", "X'010203'"))

  supportedTypes.foreach { case (dataType, value) =>
    test(s"bucket fixtures and bound functions agree for nullable $dataType values") {
      val name = "bucket_typed"
      val table = s"testcat.ns.$name"
      sql(s"CREATE TABLE $table (k $dataType, id INT) PARTITIONED BY (bucket(7, k))")
      sql(s"INSERT INTO $table VALUES ($value, 1), (NULL, 2)")
      checkAnswer(sql(s"SELECT k, id FROM $table"),
        sql(s"SELECT $value AS k, 1 AS id").collect().toSeq :+ Row(null, 2))
      val fixture = catalog.loadTable(Identifier.of(Array("ns"), name)).asInstanceOf[InMemoryTable]
      val valueType = fixture.schema()("k").dataType
      val inputType = new StructType().add("buckets", IntegerType).add("k", valueType)
      val function = UnboundBucketFunction.bind(inputType).asInstanceOf[ScalarFunction[Int]]
      fixture.data.foreach { split =>
        split.rows.foreach { row =>
          val key = row.get(0, valueType)
          val input = new GenericInternalRow(Array[Any](7, key))
          assert(function.produceResult(input) == split.key.head)
        }
      }
    }
  }

  Seq(
    ("INT", "-5", "3"),
    ("STRING", "'abc'", "'xyz'"),
    ("BINARY", "X'010203'", "X'040506'"))
    .foreach { case (dataType, first, second) =>
      test(s"bucket $dataType functions route one-sided shuffle rows to their stored buckets") {
        withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false") {
          sql(s"CREATE TABLE testcat.ns.bucket_left (k $dataType, id INT) " +
            "PARTITIONED BY (bucket(7, k))")
          sql(s"CREATE TABLE testcat.ns.bucket_right (k $dataType, id INT)")
          sql(s"INSERT INTO testcat.ns.bucket_left VALUES ($first, 1), ($second, 2), (NULL, 3)")
          sql(s"INSERT INTO testcat.ns.bucket_right VALUES ($first, 10), ($second, 20), (NULL, 30)")
          val result = sql("SELECT /*+ MERGE(l, r) */ l.id, r.id " +
            "FROM testcat.ns.bucket_left l JOIN testcat.ns.bucket_right r ON l.k = r.k")
          val shuffles = result.queryExecution.executedPlan.collect {
            case exchange: ShuffleExchangeExec => exchange
          }
          assert(shuffles.size == 1)
          checkAnswer(result, Seq(Row(1, 10), Row(2, 20)))
        }
      }
    }

  test("bucket function does not reuse a value after a mutable row becomes null") {
    val input = new SpecificInternalRow(Seq(IntegerType, LongType))
    input.setInt(0, 7)
    input.setLong(1, 5L)
    assert(BucketFunction.produceResult(input) == 5)
    // SpecificInternalRow retains the preceding primitive value when its null bit is set.
    input.setNullAt(1)
    assert(BucketFunction.produceResult(input) == 0)
  }

  test("bucket binding rejects unsupported signatures and preserves long reduction") {
    Seq(
      new StructType().add("k", LongType),
      new StructType().add("buckets", LongType).add("k", LongType),
      new StructType().add("buckets", IntegerType).add("k", LongType).add("extra", LongType))
      .foreach { inputType =>
        intercept[UnsupportedOperationException] {
          UnboundBucketFunction.bind(inputType)
        }
      }
    val reducer = BucketFunction.reducer(6, BucketFunction, 4)
    assert(reducer != null)
    Seq(-13L, -1L, 0L, 7L, 16L).foreach { value =>
      val original = BucketFunction.produceResult(InternalRow(6, value))
      assert(reducer.reduce(original) == Math.floorMod(value, 2))
    }
  }
}
