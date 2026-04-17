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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, DynamicPruningExpression}
import org.apache.spark.sql.connector.catalog.{BufferedRows, InMemoryEnhancedRuntimePartitionFilterTable, InMemoryTableEnhancedRuntimePartitionFilterCatalog}
import org.apache.spark.sql.connector.expressions.PartitionFieldReference
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that [[PartitionPredicate]] instances are pushed via iterative runtime filtering
 * (second [[SupportsRuntimeV2Filtering#filter]] call) for both DPP and scalar subquery
 * runtime filters.
 */
class DataSourceV2EnhancedRuntimePartitionFilterSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter {

  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  protected val catalogName = "testruntimepartfilter"

  before {
    spark.conf.set(s"spark.sql.catalog.$catalogName",
      classOf[InMemoryTableEnhancedRuntimePartitionFilterCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
  }

  test("DPP: PartitionPredicate pushed via iterative runtime filtering") {
    val fact = s"$catalogName.fact"
    val dim = s"$catalogName.dim"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $fact VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (dim_id INT, dim_val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2, 'two')")

      withSQLConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10") {
        val df = sql(
          s"""SELECT f.id, f.part FROM $fact f JOIN $dim d
             |ON f.part = d.dim_id WHERE d.dim_val = 'two'""".stripMargin)
        checkAnswer(df, Row(2, 2))

        assertDPPRuntimeFilters(df)

        assertPushedPartitionPredicates(df, 1)
        assertScanReturnsPartitionKeys(df, Set("2"))
        assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
      }
    }
  }

  test("DPP: PartitionPredicate on non-first partition column") {
    val fact = s"$catalogName.fact2"
    val dim = s"$catalogName.dim2"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact (id INT, p1 INT, p2 INT) " +
        s"USING $v2Source PARTITIONED BY (p1, p2)")
      for (i <- 0 until 5; j <- 0 until 2) {
        sql(s"INSERT INTO $fact VALUES (${i * 2 + j}, $i, $j)")
      }
      sql(s"CREATE TABLE $dim (dim_id INT, dim_val STRING) " +
        s"USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (1, 'one')")

      withSQLConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key ->
          "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key ->
          "10") {
        val df = sql(
          s"""SELECT f.id, f.p1, f.p2 FROM $fact f JOIN $dim d
             |ON f.p2 = d.dim_id
             |WHERE d.dim_val = 'one'""".stripMargin)
        checkAnswer(df, Seq(
          Row(1, 0, 1), Row(3, 1, 1), Row(5, 2, 1),
          Row(7, 3, 1), Row(9, 4, 1)))

        assertDPPRuntimeFilters(df)

        assertPushedPartitionPredicates(df, 1)
        assertReferencedPartitionFieldOrdinals(
          df, Array(1), Array("p1", "p2"))
      }
    }
  }

  test("scalar subquery: PartitionPredicate pushed via iterative runtime filtering") {
    val tbl = s"$catalogName.tbl"
    val dim = s"$catalogName.dim"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      assertScalarSubqueryRuntimeFilters(df)

      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("3"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("scalar subquery: complex expression with arithmetic on subquery result") {
    val tbl = s"$catalogName.tbl_complex"
    val dim = s"$catalogName.dim_complex"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2)")

      val df = sql(s"SELECT * FROM $tbl WHERE part > (SELECT max(val) FROM $dim) + 1")
      checkAnswer(df, Row(4, 4))

      assertScalarSubqueryRuntimeFilters(df)

      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("4"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("scalar subquery: RLIKE (untranslatable) with subquery pattern") {
    val tbl = s"$catalogName.tbl_rlike"
    val dim = s"$catalogName.dim_rlike"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part STRING) USING $v2Source PARTITIONED BY (part)")
      sql(s"INSERT INTO $tbl VALUES (1, 'abc')")
      sql(s"INSERT INTO $tbl VALUES (2, 'def')")
      sql(s"INSERT INTO $tbl VALUES (3, 'abx')")
      sql(s"INSERT INTO $tbl VALUES (4, 'xyz')")

      sql(s"CREATE TABLE $dim (pattern STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('^ab')")

      val df = sql(s"SELECT * FROM $tbl WHERE part RLIKE (SELECT max(pattern) FROM $dim)")
      checkAnswer(df, Seq(Row(1, "abc"), Row(3, "abx")))

      assertScalarSubqueryRuntimeFilters(df)

      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("abc", "abx"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("scalar subquery: UDF on partition column with subquery value") {
    val tbl = s"$catalogName.tbl_udf"
    val dim = s"$catalogName.dim_udf"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part STRING) USING $v2Source PARTITIONED BY (part)")
      sql(s"INSERT INTO $tbl VALUES (1, 'a')")
      sql(s"INSERT INTO $tbl VALUES (2, 'A')")
      sql(s"INSERT INTO $tbl VALUES (3, 'b')")
      sql(s"INSERT INTO $tbl VALUES (4, 'B')")

      sql(s"CREATE TABLE $dim (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('A')")

      spark.udf.register("my_upper_runtime",
        (s: String) => if (s == null) null else s.toUpperCase(java.util.Locale.ROOT))

      val df = sql(
        s"SELECT * FROM $tbl WHERE my_upper_runtime(part) = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "A")))

      assertScalarSubqueryRuntimeFilters(df)

      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "A"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("scalar subquery: two PartitionPredicates for two subqueries on different partition cols") {
    val fact = s"$catalogName.fact_2sub"
    val dim1 = s"$catalogName.dim_2sub1"
    val dim2 = s"$catalogName.dim_2sub2"
    withTable(fact, dim1, dim2) {
      sql(s"CREATE TABLE $fact (id INT, p1 INT, p2 STRING) " +
        s"USING $v2Source PARTITIONED BY (p1, p2)")
      sql(s"INSERT INTO $fact VALUES (1, 1, 'a')")
      sql(s"INSERT INTO $fact VALUES (2, 1, 'b')")
      sql(s"INSERT INTO $fact VALUES (3, 2, 'a')")
      sql(s"INSERT INTO $fact VALUES (4, 2, 'b')")

      sql(s"CREATE TABLE $dim1 (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim1 VALUES (1)")

      sql(s"CREATE TABLE $dim2 (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim2 VALUES ('a')")

      val df = sql(
        s"""SELECT * FROM $fact
           |WHERE p1 = (SELECT max(val) FROM $dim1)
           |  AND p2 = (SELECT max(val) FROM $dim2)""".stripMargin)
      checkAnswer(df, Row(1, 1, "a"))

      assertScalarSubqueryRuntimeFilters(df, expectedCount = 2)

      assertPushedPartitionPredicates(df, 2)
      assertScanReturnsPartitionKeys(df, Set("1/a"))

      val predicates = getPushedPartitionPredicates(df)
      val partFieldNames = Array("p1", "p2")
      val p1Pred = predicates.find(_.references().exists(
        _.asInstanceOf[PartitionFieldReference].ordinal() == 0))
      val p2Pred = predicates.find(_.references().exists(
        _.asInstanceOf[PartitionFieldReference].ordinal() == 1))
      assert(p1Pred.isDefined, "Expected a PartitionPredicate referencing p1 (ordinal 0)")
      assert(p2Pred.isDefined, "Expected a PartitionPredicate referencing p2 (ordinal 1)")
      assertPartitionPredicateOrdinals(p1Pred.get, Array(0), partFieldNames)
      assertPartitionPredicateOrdinals(p2Pred.get, Array(1), partFieldNames)
    }
  }

  test("scalar subquery: PartitionPredicate on non-first column of three-partition-column table") {
    val tbl = s"$catalogName.tbl_3part"
    val dim = s"$catalogName.dim_3part"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, p0 INT, p1 STRING, p2 INT) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $tbl VALUES (1, 1, 'a', 10)")
      sql(s"INSERT INTO $tbl VALUES (2, 1, 'b', 10)")
      sql(s"INSERT INTO $tbl VALUES (3, 2, 'a', 20)")
      sql(s"INSERT INTO $tbl VALUES (4, 2, 'b', 20)")

      sql(s"CREATE TABLE $dim (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('a')")

      val df = sql(
        s"SELECT * FROM $tbl WHERE p1 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Seq(Row(1, 1, "a", 10), Row(3, 2, "a", 20)))

      assertScalarSubqueryRuntimeFilters(df)

      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("1/a/10", "2/a/20"))
      assertReferencedPartitionFieldOrdinals(df, Array(1), Array("p0", "p1", "p2"))
    }
  }

  test("no PartitionPredicate for scalar subquery on data column") {
    val tbl = s"$catalogName.tbl_data"
    val dim = s"$catalogName.dim_data"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, data INT, part INT) " +
        s"USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, ${i * 10}, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (30)")

      val df = sql(
        s"SELECT * FROM $tbl WHERE data = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 30, 3))

      assertPushedPartitionPredicates(df, 0)
    }
  }

  test("no PartitionPredicate when supportsIterativeFiltering is false") {
    val baseCatalog = "testv2filterNoIterative"
    spark.conf.set(s"spark.sql.catalog.$baseCatalog",
      classOf[catalog.InMemoryTableWithV2FilterCatalog].getName)

    val tbl = s"$baseCatalog.tbl"
    val dim = s"$baseCatalog.dim"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      val batchScan = collectBatchScan(df)
      assert(batchScan.runtimeFilters.nonEmpty)

      val scan = batchScan.scan
      assert(
        !scan.asInstanceOf[
          catalog.InMemoryTableWithV2Filter#InMemoryV2FilterBatchScan
        ].supportsIterativeFiltering(),
        "Base V2 filter table should not support iterative filtering")
    }
  }

  private def assertDPPRuntimeFilters(
      df: DataFrame, expectedCount: Int = 1): Unit = {
    val batchScan = collectBatchScan(df)
    val dppFilters = batchScan.runtimeFilters.collect {
      case d: DynamicPruningExpression => d
    }
    assert(dppFilters.size === expectedCount,
      s"Expected $expectedCount DynamicPruningExpression(s) " +
        s"in runtimeFilters, got ${dppFilters.size}")
  }

  private def assertScalarSubqueryRuntimeFilters(
      df: DataFrame, expectedCount: Int = 1): Unit = {
    val batchScan = collectBatchScan(df)
    val scalarFilters = batchScan.runtimeFilters.collect {
      case f if !f.isInstanceOf[DynamicPruning] => f
    }
    val dppFilters = batchScan.runtimeFilters.collect {
      case d: DynamicPruning => d
    }
    assert(scalarFilters.size === expectedCount,
      s"Expected $expectedCount scalar subquery runtime filter(s), " +
        s"got ${scalarFilters.size}")
    assert(dppFilters.isEmpty,
      "Expected non-DPP runtime filters (scalar subquery)")
  }

  private def collectBatchScan(df: DataFrame): BatchScanExec = {
    stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
  }

  private def getPushedPartitionPredicates(
      df: DataFrame): Seq[PartitionPredicate] = {
    val batchScan = collectBatchScan(df)
    batchScan.scan match {
      case s: InMemoryEnhancedRuntimePartitionFilterTable#
        InMemoryEnhancedRuntimePartitionFilterBatchScan =>
        s.pushedPartitionPredicates
      case _ => Seq.empty
    }
  }

  private def assertPushedPartitionPredicates(
      df: DataFrame,
      expectedCount: Int): Unit = {
    val predicates = getPushedPartitionPredicates(df)
    assert(predicates.size === expectedCount,
      s"Expected $expectedCount pushed partition predicate(s), " +
        s"got ${predicates.size}: $predicates")
  }

  private def assertPartitionPredicateOrdinals(
      predicate: PartitionPredicate,
      expectedOrdinals: Array[Int],
      expectedPartitionFieldNames: Array[String]): Unit = {
    val refs = predicate.references()
    val ordinals = refs.map(_.asInstanceOf[PartitionFieldReference].ordinal()).sorted
    assert(ordinals.sameElements(expectedOrdinals.sorted),
      s"Expected references().map(_.ordinal()) " +
        s"${expectedOrdinals.sorted.mkString("[", ", ", "]")}, " +
        s"got ${ordinals.mkString("[", ", ", "]")}")

    val names = expectedPartitionFieldNames
    refs.foreach { ref =>
      assert(ref.isInstanceOf[PartitionFieldReference],
        s"Expected PartitionFieldReference, got ${ref.getClass.getName}")
      val partRef = ref.asInstanceOf[PartitionFieldReference]
      assert(partRef.fieldNames().nonEmpty,
        s"PartitionFieldReference.ordinal=${partRef.ordinal()} has empty fieldNames")
      assert(partRef.ordinal() < names.length,
        s"PartitionFieldReference.ordinal=${partRef.ordinal()} " +
          s"out of range for names length ${names.length}")
      val expectedName = names(partRef.ordinal())
      val actualName = partRef.fieldNames().mkString(".")
      assert(actualName === expectedName,
        s"PartitionFieldReference.ordinal=${partRef.ordinal()}: " +
          s"expected fieldNames '${expectedName}', got '${actualName}'")
    }
  }

  private def assertReferencedPartitionFieldOrdinals(
      df: DataFrame,
      expectedOrdinals: Array[Int],
      expectedPartitionFieldNames: Array[String]): Unit = {
    getPushedPartitionPredicates(df).foreach { p =>
      assertPartitionPredicateOrdinals(p, expectedOrdinals, expectedPartitionFieldNames)
    }
  }

  private def assertScanReturnsPartitionKeys(
      df: DataFrame,
      expectedPartitionKeys: Set[String]): Unit = {
    val batchScan = collectBatchScan(df)
    val partitions = batchScan.batch.planInputPartitions()
    assert(partitions.length === expectedPartitionKeys.size,
      s"Expected ${expectedPartitionKeys.size} partition(s), got ${partitions.length}")
    val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
    assert(partKeys === expectedPartitionKeys,
      s"Partition keys should be $expectedPartitionKeys, got $partKeys")
  }
}
