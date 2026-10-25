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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}
import org.apache.spark.sql.connector.catalog.{Column, Identifier}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, BucketFunction, ScalarFunction, UnboundBucketFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, identity}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}

class PerPartitionRuntimeFilterSuite extends KeyGroupedPartitioningSuiteBase {

  // The in-memory scans are small, so the size threshold of the application side is lifted.
  override def sparkConf: SparkConf = super.sparkConf
    .set(V2_BUCKETING_ENABLED, true)
    .set(AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD, 0L)
    .set(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED, true)

  before {
    catalog.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
      UnboundBucketFunction)
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  private def createTables(
      idType: DataType = LongType,
      factPartitioning: Array[Transform] = Array(bucket(4, "id")),
      dimPartitioning: Array[Transform] = Array(bucket(4, "id"))): Unit = {
    createTable("fact", Array(Column.create("id", idType), Column.create("v", IntegerType)),
      factPartitioning)
    sql("INSERT INTO testcat.ns.fact VALUES " +
      (0 until 16).map(i => s"($i, ${i * 10})").mkString(", "))
    createTable("dim", Array(Column.create("id", idType), Column.create("flag", IntegerType)),
      dimPartitioning)
    sql("INSERT INTO testcat.ns.dim VALUES (1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
  }

  private def join(): DataFrame = sql(
    """
      |SELECT f.id, f.v
      |FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id
      |WHERE d.flag = 1
      |""".stripMargin)

  private val expectedRows = Seq(Row(1, 10), Row(3, 30), Row(5, 50), Row(9, 90))

  // The size of the creation side that a join on `id` gets from `table` with `flag = 1`.
  private def creationSideSize(table: String): BigInt =
    spark.table(s"testcat.ns.$table").where("flag = 1").select("id")
      .queryExecution.optimizedPlan.stats.sizeInBytes

  private def perPartitionSemiJoins(df: DataFrame): Seq[Join] =
    df.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, LeftSemi, Some(condition), _)
          if condition.exists(_.isInstanceOf[BloomFilterMightContain]) => j
    }

  // A bloom filter of the whole creation side, which a subquery computes.
  private def isGlobalCheck(e: Expression): Boolean = e match {
    case BloomFilterMightContain(_: PlanExpression[_], _) => true
    case _ => false
  }

  private def hasGlobalBloomFilter(df: DataFrame): Boolean =
    df.queryExecution.optimizedPlan.exists {
      case Filter(condition, _) => condition.exists(isGlobalCheck)
      case _ => false
    }

  // `InjectRuntimeFilter` added only the bloom filter of the whole creation side.
  private def checkOnlyGlobalBloomFilter(df: DataFrame): Unit = {
    val plan = df.queryExecution.optimizedPlan
    assert(perPartitionSemiJoins(df).isEmpty, plan)
    assert(hasGlobalBloomFilter(df), plan)
    checkAnswer(df, expectedRows)
  }

  test("a per-partition bloom filter joins the application side next to the global bloom filter") {
    createTables()
    val df = join()
    assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
    assert(hasGlobalBloomFilter(df), df.queryExecution.optimizedPlan)
    checkAnswer(df, expectedRows)
    assert(collect(df.queryExecution.executedPlan) { case s: ShuffleExchangeExec => s }.isEmpty,
      "the semi join pairs the partitions as the join does")
  }

  test("a later join gets its bloom filter on a key whose semi join has no global one") {
    // The creation side of `dim` is too big for one bloom filter of all its keys, so its join gets
    // only the semi join. The join with `dim2` on the same key still gets its bloom filter, but no
    // second semi join.
    createTables()
    createTable("dim2", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.dim2 VALUES (1, 1), (3, 1)")
    val size = creationSideSize("dim")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id " +
        "JOIN testcat.ns.dim2 d2 ON f.id = d2.id WHERE d.flag = 1 AND d2.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).size == 1, plan)
      assert(hasGlobalBloomFilter(df), plan)
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
    }
  }

  test("the right side still gets its bloom filter when the left side gets only a semi join") {
    // The creation side of `l` is too big for one bloom filter of all its keys, so `l` gets only
    // the semi join. `rt` still gets the bloom filter of `l`.
    createTable("l", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.l VALUES " +
      (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
    createTable("rt", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.rt VALUES " +
      (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
    val size = creationSideSize("rt")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT l.id, l.v FROM testcat.ns.l l JOIN testcat.ns.rt rt ON l.id = rt.id " +
        "WHERE l.v < 30 AND rt.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).nonEmpty, plan)
      assert(hasGlobalBloomFilter(df), plan)
      checkAnswer(df, Seq(Row(1, 10)))
    }
  }

  test("no per-partition bloom filter when it is turned off") {
    createTables()
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
      checkOnlyGlobalBloomFilter(join())
    }
  }

  test("no per-partition bloom filter for an identity partitioning") {
    // Each partition would hold a single key, so its bloom filter would check nothing. Dynamic
    // partition pruning on `id` would keep the rule from adding any bloom filter.
    createTables(factPartitioning = Array(identity("id")), dimPartitioning = Array(identity("id")))
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
      checkOnlyGlobalBloomFilter(join())
    }
  }

  test("no per-partition bloom filter for different transforms") {
    createTables(dimPartitioning = Array(bucket(8, "id")))
    checkOnlyGlobalBloomFilter(join())
  }

  test("no per-partition bloom filter for a non-deterministic transform") {
    // Both sides of the semi join evaluate the transform, so they could disagree on a row's key.
    catalog.clearFunctions()
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), new UnboundFunction {
      override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Int] {
        override def inputTypes(): Array[DataType] = BucketFunction.inputTypes()
        override def resultType(): DataType = BucketFunction.resultType()
        override def name(): String = BucketFunction.name()
        override def canonicalName(): String = BucketFunction.canonicalName()
        override def isDeterministic(): Boolean = false
        override def produceResult(input: InternalRow): Int = BucketFunction.produceResult(input)
      }
      override def description(): String = name()
      override def name(): String = "bucket"
    })
    createTables()
    checkOnlyGlobalBloomFilter(join())
  }

  test("no per-partition bloom filter when the transform's input types differ from the column") {
    // The test catalog's `bucket` takes a BIGINT value, so over an INT column its call fails.
    createTables(idType = IntegerType)
    checkOnlyGlobalBloomFilter(join())
  }
}
